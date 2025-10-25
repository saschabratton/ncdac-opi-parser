use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Check if a path exists
fn path_exists(path: &Path) -> bool {
    path.exists()
}

/// Resolve the ZIP file path for a given file ID
///
/// This function looks for a ZIP file in the data directory matching the file_id:
/// 1. First checks for an exact case-sensitive match
/// 2. Then performs a case-insensitive search through all ZIP files
///
/// # Arguments
/// * `file_id` - The base name of the file (without .zip extension)
/// * `data_dir` - The data directory to search in
///
/// # Returns
/// The full path to the ZIP file
///
/// # Errors
/// Returns an error if no matching ZIP file is found
fn resolve_zip_path(file_id: &str, data_dir: &Path) -> Result<PathBuf> {
    let direct_candidate = data_dir.join(format!("{file_id}.zip"));
    if path_exists(&direct_candidate) {
        return Ok(direct_candidate);
    }

    let lower_file_id = file_id.to_lowercase();
    let entries = fs::read_dir(data_dir)
        .with_context(|| format!("Failed to read data directory: {}", data_dir.display()))?;

    for entry in entries {
        let entry = entry.context("Failed to read directory entry")?;
        let path = entry.path();

        if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
            if !file_name.to_lowercase().ends_with(".zip") {
                continue;
            }

            let entry_base = &file_name[..file_name.len() - 4];
            if entry_base.to_lowercase() == lower_file_id {
                return Ok(path);
            }
        }
    }

    anyhow::bail!(
        "Unable to locate ZIP archive for {} in {}",
        file_id,
        data_dir.display()
    );
}

/// Ensure the destination directory is ready for extraction
///
/// If the directory already exists, it will be removed and recreated.
///
/// # Arguments
/// * `destination_path` - The path where files will be extracted
///
/// # Errors
/// Returns errors if directory operations fail
fn ensure_destination(destination_path: &Path) -> Result<()> {
    if path_exists(destination_path) {
        fs::remove_dir_all(destination_path).with_context(|| {
            format!(
                "Failed to remove existing directory: {}",
                destination_path.display()
            )
        })?;
    }

    fs::create_dir_all(destination_path).with_context(|| {
        format!(
            "Failed to create destination directory: {}",
            destination_path.display()
        )
    })?;

    Ok(())
}

/// Extract a single entry from the ZIP archive to disk
///
/// # Arguments
/// * `file` - The ZIP file entry
/// * `destination_dir` - The base directory for extraction
/// * `pb` - Progress bar to update during extraction
///
/// # Returns
/// The number of bytes written (0 for directories)
///
/// # Errors
/// Returns errors if file operations fail
fn extract_entry(file: &mut zip::read::ZipFile, destination_dir: &Path, pb: &ProgressBar) -> Result<u64> {
    let entry_name = file.name().to_string();

    if entry_name.is_empty() {
        return Ok(0);
    }

    let file_path = destination_dir.join(&entry_name);

    if file.is_dir() {
        fs::create_dir_all(&file_path).with_context(|| {
            format!("Failed to create directory: {}", file_path.display())
        })?;
        return Ok(0);
    }

    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("Failed to create parent directory: {}", parent.display())
        })?;
    }

    let mut output_file = File::create(&file_path)
        .with_context(|| format!("Failed to create file: {}", file_path.display()))?;

    let mut total_written = 0u64;
    let mut buffer = vec![0; 8192];

    loop {
        let bytes_read = file.read(&mut buffer)
            .with_context(|| format!("Failed to read from ZIP entry: {}", entry_name))?;

        if bytes_read == 0 {
            break;
        }

        output_file.write_all(&buffer[..bytes_read])
            .with_context(|| format!("Failed to write file: {}", file_path.display()))?;

        total_written += bytes_read as u64;
        pb.inc(bytes_read as u64);
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Some(mode) = file.unix_mode() {
            let permissions = std::fs::Permissions::from_mode(mode);
            fs::set_permissions(&file_path, permissions)
                .with_context(|| format!("Failed to set permissions: {}", file_path.display()))?;
        }
    }

    Ok(total_written)
}

/// Extract a ZIP data file to the data directory
///
/// This function extracts a ZIP file identified by `file_id` to a subdirectory
/// in the data directory. The ZIP file should be located at `./data/{file_id}.zip`
/// and will be extracted to `./data/{file_id}/`.
///
/// If the destination directory already exists, it will be removed and recreated.
/// Progress is displayed using a progress bar showing extraction progress.
///
/// # Arguments
/// * `file_id` - The identifier for the file (without .zip extension)
/// * `file_name` - Human-readable name for progress display
///
/// # Returns
/// The path to the extraction directory on success
///
/// # Errors
/// * Returns errors if the ZIP file cannot be found or opened
/// * Returns errors if extraction fails
///
/// # Example
/// ```no_run
/// use ncdac_opi_parser::unzip::unzip_data_file;
///
/// let result = unzip_data_file("INMT4AA", "Inmate Profile");
/// ```
pub fn unzip_data_file(file_id: &str, file_name: &str) -> Result<PathBuf> {
    let data_dir = crate::utilities::data_directory();

    let zip_path = resolve_zip_path(file_id, &data_dir)
        .with_context(|| format!("Failed to locate ZIP file for {}", file_id))?;

    let destination_dir = data_dir.join(file_id);

    ensure_destination(&destination_dir)?;

    let file = File::open(&zip_path)
        .with_context(|| format!("Failed to open ZIP file: {}", zip_path.display()))?;

    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("Failed to read ZIP archive: {}", zip_path.display()))?;

    let entry_count = archive.len();
    let mut total_size = 0u64;
    for i in 0..entry_count {
        if let Ok(file) = archive.by_index(i) {
            total_size += file.size();
        }
    }

    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );
    pb.set_message(format!("Decompressing {} ({})", file_name, file_id));

    for i in 0..entry_count {
        let mut file = archive.by_index(i)
            .with_context(|| format!("Failed to read ZIP entry at index {}", i))?;

        extract_entry(&mut file, &destination_dir, &pb)
            .with_context(|| format!("Failed to extract entry: {}", file.name()))?;
    }

    pb.finish_with_message(format!("✓ Decompressed {} ({})", file_name, file_id));

    Ok(destination_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_path_exists() {
        let temp_dir = std::env::temp_dir();
        assert!(path_exists(&temp_dir));

        let non_existent = temp_dir.join("this_should_not_exist_12345");
        assert!(!path_exists(&non_existent));
    }

    #[test]
    fn test_ensure_destination_creates_directory() {
        let temp_dir = std::env::temp_dir().join("test_ensure_dest");

        let _ = fs::remove_dir_all(&temp_dir);

        ensure_destination(&temp_dir).unwrap();
        assert!(temp_dir.exists());
        assert!(temp_dir.is_dir());

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_ensure_destination_overwrites_existing() {
        let temp_dir = std::env::temp_dir().join("test_ensure_dest_overwrite");

        fs::create_dir_all(&temp_dir).unwrap();
        let test_file = temp_dir.join("test.txt");
        let mut file = File::create(&test_file).unwrap();
        file.write_all(b"test").unwrap();

        ensure_destination(&temp_dir).unwrap();
        assert!(temp_dir.exists());
        assert!(temp_dir.is_dir());

        assert!(!test_file.exists());

        fs::remove_dir_all(&temp_dir).unwrap();
    }
}
