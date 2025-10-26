//! ZIP file extraction and decompression utilities.
//!
//! This module provides functions for extracting ZIP files with support for
//! both sequential and parallel decompression operations. The parallel
//! decompression feature allows multiple ZIP files to be extracted concurrently
//! using a shared progress bar for aggregated progress tracking.
//!
//! # Examples
//!
//! Sequential extraction with individual progress bar:
//! ```no_run
//! use ncdac_opi_parser::unzip::unzip_data_file;
//!
//! let result = unzip_data_file("INMT4AA", "Inmate Profile");
//! ```
//!
//! Parallel extraction with shared progress bar:
//! ```no_run
//! use ncdac_opi_parser::unzip::{decompress_with_shared_progress, calculate_total_uncompressed_bytes};
//! use ncdac_opi_parser::files::FILES;
//! use indicatif::ProgressBar;
//! use rayon::prelude::*;
//! use std::sync::Arc;
//! use std::path::Path;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let data_dir = Path::new("./data");
//! let files_to_decompress = &FILES[0..3];
//!
//! // Calculate total size for progress bar
//! let total_bytes = calculate_total_uncompressed_bytes(files_to_decompress, data_dir)?;
//!
//! // Create shared progress bar
//! let shared_pb = Arc::new(ProgressBar::new(total_bytes));
//!
//! // Decompress files in parallel
//! files_to_decompress.par_iter().try_for_each(|file| {
//!     decompress_with_shared_progress(file.id, file.name, &shared_pb)
//!         .map(|_| ())
//! })?;
//! # Ok(())
//! # }
//! ```

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
/// * `pb` - Progress bar to update during extraction (can be Arc-wrapped)
///
/// # Returns
/// The number of bytes written (0 for directories)
///
/// # Errors
/// Returns errors if file operations fail
fn extract_entry(
    file: &mut zip::read::ZipFile,
    destination_dir: &Path,
    pb: &Arc<ProgressBar>,
) -> Result<u64> {
    let entry_name = file.name().to_string();

    if entry_name.is_empty() {
        return Ok(0);
    }

    let file_path = destination_dir.join(&entry_name);

    if file.is_dir() {
        fs::create_dir_all(&file_path)
            .with_context(|| format!("Failed to create directory: {}", file_path.display()))?;
        return Ok(0);
    }

    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create parent directory: {}", parent.display()))?;
    }

    let mut output_file = File::create(&file_path)
        .with_context(|| format!("Failed to create file: {}", file_path.display()))?;

    let mut total_written = 0u64;
    let mut buffer = vec![0; 8192];

    loop {
        let bytes_read = file
            .read(&mut buffer)
            .with_context(|| format!("Failed to read from ZIP entry: {}", entry_name))?;

        if bytes_read == 0 {
            break;
        }

        output_file
            .write_all(&buffer[..bytes_read])
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

/// Decompress a ZIP file with a shared progress bar for parallel decompression
///
/// This function extracts a ZIP file to a subdirectory in the data directory,
/// using a shared Arc-wrapped ProgressBar that can be updated concurrently
/// from multiple threads during parallel decompression operations.
///
/// # Arguments
/// * `file_id` - The identifier for the file (without .zip extension)
/// * `file_name` - Human-readable name for error messages
/// * `shared_pb` - Arc-wrapped ProgressBar shared across parallel workers
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
/// use ncdac_opi_parser::unzip::decompress_with_shared_progress;
/// use indicatif::ProgressBar;
/// use std::sync::Arc;
///
/// let pb = Arc::new(ProgressBar::new(1000000));
/// let result = decompress_with_shared_progress("INMT4AA", "Inmate Profile", &pb);
/// ```
pub fn decompress_with_shared_progress(
    file_id: &str,
    file_name: &str,
    shared_pb: &Arc<ProgressBar>,
) -> Result<PathBuf> {
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

    for i in 0..entry_count {
        let mut file = archive
            .by_index(i)
            .with_context(|| format!("Failed to read ZIP entry at index {}", i))?;

        extract_entry(&mut file, &destination_dir, shared_pb).with_context(|| {
            format!(
                "Failed to extract entry '{}' from {} ({})",
                file.name(),
                file_name,
                file_id
            )
        })?;
    }

    Ok(destination_dir)
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

    let pb = Arc::new(ProgressBar::new(total_size));
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );
    pb.set_message(format!("Decompressing {} ({})", file_name, file_id));

    for i in 0..entry_count {
        let mut file = archive
            .by_index(i)
            .with_context(|| format!("Failed to read ZIP entry at index {}", i))?;

        extract_entry(&mut file, &destination_dir, &pb)
            .with_context(|| format!("Failed to extract entry: {}", file.name()))?;
    }

    pb.finish_with_message(format!("✓ Decompressed {} ({})", file_name, file_id));

    Ok(destination_dir)
}

/// Calculate the total uncompressed bytes across all ZIP files
///
/// This function opens each ZIP file in the provided list, sums the uncompressed
/// size of all entries across all archives, then closes each ZIP file. This total
/// is used to initialize the aggregated progress bar during parallel decompression.
///
/// # Arguments
/// * `files` - Slice of FileMetadata for files to decompress
/// * `data_dir` - The data directory containing the ZIP files
///
/// # Returns
/// The total uncompressed bytes across all ZIP files
///
/// # Errors
/// * Returns error if any ZIP file cannot be found, opened, or read
/// * Error context identifies which file failed
///
/// # Example
/// ```no_run
/// use ncdac_opi_parser::unzip::calculate_total_uncompressed_bytes;
/// use ncdac_opi_parser::files::FILES;
/// use std::path::Path;
///
/// let data_dir = Path::new("./data");
/// let total_bytes = calculate_total_uncompressed_bytes(&FILES, data_dir)?;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub fn calculate_total_uncompressed_bytes(
    files: &[crate::files::FileMetadata],
    data_dir: &Path,
) -> Result<u64> {
    let mut total_bytes = 0u64;

    for file_metadata in files {
        let file_id = file_metadata.id;

        let zip_path = resolve_zip_path(file_id, data_dir).with_context(|| {
            format!(
                "Failed to locate ZIP file for {} ({}) during size calculation",
                file_metadata.name, file_id
            )
        })?;

        let file = File::open(&zip_path).with_context(|| {
            format!(
                "Failed to open ZIP file for {} ({}): {}",
                file_metadata.name,
                file_id,
                zip_path.display()
            )
        })?;

        let mut archive = zip::ZipArchive::new(file).with_context(|| {
            format!(
                "Failed to read ZIP archive for {} ({}): {}",
                file_metadata.name,
                file_id,
                zip_path.display()
            )
        })?;

        for i in 0..archive.len() {
            if let Ok(entry) = archive.by_index(i) {
                total_bytes += entry.size();
            }
        }

        // ZIP archive is automatically closed when it goes out of scope
    }

    Ok(total_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;
    use zip::write::SimpleFileOptions;
    use zip::write::ZipWriter;

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

    fn create_test_zip(zip_path: &Path, files: &[(&str, &[u8])]) -> Result<()> {
        let file = File::create(zip_path)
            .with_context(|| format!("Failed to create test ZIP: {}", zip_path.display()))?;
        let mut zip = ZipWriter::new(file);
        let options = SimpleFileOptions::default();

        for (name, content) in files {
            zip.start_file(*name, options)
                .with_context(|| format!("Failed to start ZIP entry: {}", name))?;
            zip.write_all(content)
                .with_context(|| format!("Failed to write ZIP entry: {}", name))?;
        }

        zip.finish()
            .context("Failed to finalize ZIP archive")?;
        Ok(())
    }

    #[test]
    fn test_decompress_with_shared_progress_successful_extraction() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();
        let file_id = "TEST001";

        let zip_path = data_dir.join(format!("{}.zip", file_id));
        let test_content = b"Hello, this is test content!";
        create_test_zip(&zip_path, &[("test.txt", test_content)]).unwrap();

        let total_size = test_content.len() as u64;
        let pb = Arc::new(ProgressBar::new(total_size));

        // Test decompression by mocking data_directory temporarily
        // Since we can't easily override utilities::data_directory(), we'll test the core logic
        // by using resolve_zip_path and extract_entry directly

        let resolved_path = resolve_zip_path(file_id, data_dir).unwrap();
        assert_eq!(resolved_path, zip_path);

        let destination_dir = data_dir.join(file_id);
        ensure_destination(&destination_dir).unwrap();

        let file = File::open(&zip_path).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        let mut zip_file = archive.by_index(0).unwrap();

        let bytes_written = extract_entry(&mut zip_file, &destination_dir, &pb).unwrap();

        assert_eq!(bytes_written, test_content.len() as u64);
        let extracted_file = destination_dir.join("test.txt");
        assert!(extracted_file.exists());

        let extracted_content = fs::read(&extracted_file).unwrap();
        assert_eq!(extracted_content, test_content);

        assert_eq!(pb.position(), test_content.len() as u64);
    }

    #[test]
    fn test_decompress_preserves_file_sizes() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();
        let file_id = "TEST002";

        let zip_path = data_dir.join(format!("{}.zip", file_id));
        let small_file = b"small";
        let medium_file = b"This is a medium sized file with more content.";
        let large_file = vec![b'X'; 1000]; // 1000 bytes

        create_test_zip(
            &zip_path,
            &[
                ("small.txt", small_file),
                ("medium.txt", medium_file),
                ("large.dat", &large_file),
            ],
        )
        .unwrap();

        let total_size = (small_file.len() + medium_file.len() + large_file.len()) as u64;
        let pb = Arc::new(ProgressBar::new(total_size));

        let destination_dir = data_dir.join(file_id);
        ensure_destination(&destination_dir).unwrap();

        let file = File::open(&zip_path).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();

        for i in 0..archive.len() {
            let mut zip_file = archive.by_index(i).unwrap();
            extract_entry(&mut zip_file, &destination_dir, &pb).unwrap();
        }

        let small_extracted = fs::read(destination_dir.join("small.txt")).unwrap();
        assert_eq!(small_extracted.len(), small_file.len());
        assert_eq!(small_extracted, small_file);

        let medium_extracted = fs::read(destination_dir.join("medium.txt")).unwrap();
        assert_eq!(medium_extracted.len(), medium_file.len());
        assert_eq!(medium_extracted, medium_file);

        let large_extracted = fs::read(destination_dir.join("large.dat")).unwrap();
        assert_eq!(large_extracted.len(), large_file.len());
        assert_eq!(large_extracted, &large_file[..]);

        assert_eq!(pb.position(), total_size);
    }

    #[cfg(unix)]
    #[test]
    fn test_decompress_preserves_unix_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();
        let file_id = "TEST003";

        let test_file_path = data_dir.join("executable.sh");
        let mut test_file = File::create(&test_file_path).unwrap();
        test_file.write_all(b"#!/bin/bash\necho 'test'").unwrap();

        // Set executable permissions (0o755)
        let perms = std::fs::Permissions::from_mode(0o755);
        fs::set_permissions(&test_file_path, perms).unwrap();

        let zip_path = data_dir.join(format!("{}.zip", file_id));
        let file = File::create(&zip_path).unwrap();
        let mut zip = ZipWriter::new(file);

        let content = fs::read(&test_file_path).unwrap();
        let metadata = fs::metadata(&test_file_path).unwrap();
        let mode = metadata.permissions().mode();

        let options = SimpleFileOptions::default().unix_permissions(mode);
        zip.start_file("executable.sh", options).unwrap();
        zip.write_all(&content).unwrap();
        zip.finish().unwrap();

        let pb = Arc::new(ProgressBar::new(content.len() as u64));
        let destination_dir = data_dir.join(file_id);
        ensure_destination(&destination_dir).unwrap();

        let file = File::open(&zip_path).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        let mut zip_file = archive.by_index(0).unwrap();

        extract_entry(&mut zip_file, &destination_dir, &pb).unwrap();

        let extracted_file = destination_dir.join("executable.sh");
        let extracted_metadata = fs::metadata(&extracted_file).unwrap();
        let extracted_mode = extracted_metadata.permissions().mode();

        // Check that executable bits are preserved (mask with 0o777 to compare permission bits)
        assert_eq!(extracted_mode & 0o777, mode & 0o777);
    }

    #[test]
    fn test_decompress_with_shared_progress_handles_directories() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();
        let file_id = "TEST004";

        let zip_path = data_dir.join(format!("{}.zip", file_id));
        let file = File::create(&zip_path).unwrap();
        let mut zip = ZipWriter::new(file);
        let options = SimpleFileOptions::default();

        zip.add_directory("subdir/", options).unwrap();

        zip.start_file("subdir/nested.txt", options).unwrap();
        zip.write_all(b"nested content").unwrap();

        zip.finish().unwrap();

        let pb = Arc::new(ProgressBar::new(100));
        let destination_dir = data_dir.join(file_id);
        ensure_destination(&destination_dir).unwrap();

        let file = File::open(&zip_path).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();

        for i in 0..archive.len() {
            let mut zip_file = archive.by_index(i).unwrap();
            extract_entry(&mut zip_file, &destination_dir, &pb).unwrap();
        }

        let subdir = destination_dir.join("subdir");
        assert!(subdir.exists());
        assert!(subdir.is_dir());

        let nested_file = subdir.join("nested.txt");
        assert!(nested_file.exists());

        let content = fs::read_to_string(&nested_file).unwrap();
        assert_eq!(content, "nested content");
    }

    #[test]
    fn test_calculate_total_bytes_single_zip() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        let file_id = "TEST_SINGLE";
        let zip_path = data_dir.join(format!("{}.zip", file_id));
        let content1 = b"Hello World!";
        let content2 = vec![b'A'; 500];

        create_test_zip(
            &zip_path,
            &[("file1.txt", content1), ("file2.dat", &content2)],
        )
        .unwrap();

        let file_metadata = crate::files::FileMetadata::new(
            file_id,
            "Test File",
            "https://example.com/test.zip",
        );

        let total_bytes = calculate_total_uncompressed_bytes(&[file_metadata], data_dir).unwrap();

        let expected_size = (content1.len() + content2.len()) as u64;
        assert_eq!(total_bytes, expected_size);
    }

    #[test]
    fn test_calculate_total_bytes_multiple_zips() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        let file1_id = "TEST_MULTI_1";
        let zip1_path = data_dir.join(format!("{}.zip", file1_id));
        let content1a = b"First file, first content";
        let content1b = vec![b'B'; 200];
        create_test_zip(
            &zip1_path,
            &[("a.txt", content1a), ("b.dat", &content1b)],
        )
        .unwrap();

        let file2_id = "TEST_MULTI_2";
        let zip2_path = data_dir.join(format!("{}.zip", file2_id));
        let content2a = b"Second file content";
        let content2b = vec![b'C'; 300];
        create_test_zip(
            &zip2_path,
            &[("c.txt", content2a), ("d.dat", &content2b)],
        )
        .unwrap();

        let file3_id = "TEST_MULTI_3";
        let zip3_path = data_dir.join(format!("{}.zip", file3_id));
        let content3 = vec![b'X'; 1000];
        create_test_zip(&zip3_path, &[("large.bin", &content3)]).unwrap();

        let files = vec![
            crate::files::FileMetadata::new(file1_id, "Test 1", "https://example.com/1.zip"),
            crate::files::FileMetadata::new(file2_id, "Test 2", "https://example.com/2.zip"),
            crate::files::FileMetadata::new(file3_id, "Test 3", "https://example.com/3.zip"),
        ];

        let total_bytes = calculate_total_uncompressed_bytes(&files, data_dir).unwrap();

        let expected_size = (content1a.len()
            + content1b.len()
            + content2a.len()
            + content2b.len()
            + content3.len()) as u64;
        assert_eq!(total_bytes, expected_size);
    }

    #[test]
    fn test_calculate_total_bytes_handles_missing_zip() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        let file_metadata = crate::files::FileMetadata::new(
            "NONEXISTENT",
            "Missing File",
            "https://example.com/missing.zip",
        );

        let result = calculate_total_uncompressed_bytes(&[file_metadata], data_dir);

        assert!(result.is_err());
        let error_message = format!("{:?}", result.unwrap_err());
        assert!(error_message.contains("NONEXISTENT"));
        assert!(error_message.contains("Missing File"));
    }

    #[test]
    fn test_calculate_total_bytes_empty_list() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        let total_bytes = calculate_total_uncompressed_bytes(&[], data_dir).unwrap();

        assert_eq!(total_bytes, 0);
    }

    #[test]
    fn test_parallel_decompression_multiple_valid_zips() {
        use rayon::prelude::*;

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        let file_ids = vec!["PARALLEL_1", "PARALLEL_2", "PARALLEL_3"];
        let test_contents = vec![
            b"Content for file 1".as_slice(),
            b"Content for file 2".as_slice(),
            b"Content for file 3".as_slice(),
        ];

        let mut total_size = 0u64;

        for (file_id, content) in file_ids.iter().zip(&test_contents) {
            let zip_path = data_dir.join(format!("{}.zip", file_id));
            create_test_zip(&zip_path, &[(format!("{}.txt", file_id).as_str(), content)]).unwrap();
            total_size += content.len() as u64;
        }

        let shared_pb = Arc::new(ProgressBar::new(total_size));

        let result: Result<Vec<()>> = file_ids
            .par_iter()
            .map(|file_id| {
                let destination_dir = data_dir.join(file_id);
                ensure_destination(&destination_dir)?;

                let zip_path = resolve_zip_path(file_id, data_dir)?;
                let zip_file = File::open(&zip_path)?;
                let mut archive = zip::ZipArchive::new(zip_file)?;

                for i in 0..archive.len() {
                    let mut entry = archive.by_index(i)?;
                    extract_entry(&mut entry, &destination_dir, &shared_pb)?;
                }

                Ok(())
            })
            .collect();

        assert!(result.is_ok());

        for (file_id, expected_content) in file_ids.iter().zip(&test_contents) {
            let extracted_file = data_dir.join(file_id).join(format!("{}.txt", file_id));
            assert!(extracted_file.exists());

            let content = fs::read(&extracted_file).unwrap();
            assert_eq!(content, *expected_content);
        }

        assert_eq!(shared_pb.position(), total_size);
    }

    #[test]
    fn test_parallel_decompression_fail_fast() {
        use rayon::prelude::*;

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        let valid_id = "VALID_ZIP";
        let missing_id = "MISSING_ZIP";

        let zip_path = data_dir.join(format!("{}.zip", valid_id));
        create_test_zip(&zip_path, &[("test.txt", b"valid content")]).unwrap();

        let file_ids = vec![valid_id, missing_id];
        let shared_pb = Arc::new(ProgressBar::new(1000));

        let result: Result<()> = file_ids.par_iter().try_for_each(|file_id| {
            let zip_path = resolve_zip_path(file_id, data_dir)?;
            let destination_dir = data_dir.join(file_id);
            ensure_destination(&destination_dir)?;

            let zip_file = File::open(&zip_path)?;
            let mut archive = zip::ZipArchive::new(zip_file)?;

            for i in 0..archive.len() {
                let mut entry = archive.by_index(i)?;
                extract_entry(&mut entry, &destination_dir, &shared_pb)?;
            }

            Ok(())
        });

        assert!(result.is_err());
        let error_message = format!("{:?}", result.unwrap_err());
        assert!(error_message.contains("MISSING_ZIP"));
    }

    #[test]
    fn test_parallel_decompression_progress_updates() {
        use rayon::prelude::*;

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        let file_ids = vec!["PROG_1", "PROG_2", "PROG_3"];
        let contents = vec![
            vec![b'A'; 500],
            vec![b'B'; 300],
            vec![b'C'; 200],
        ];

        for (file_id, content) in file_ids.iter().zip(&contents) {
            let zip_path = data_dir.join(format!("{}.zip", file_id));
            create_test_zip(&zip_path, &[(format!("{}.dat", file_id).as_str(), content)]).unwrap();
        }

        let total_size: u64 = contents.iter().map(|c| c.len() as u64).sum();
        let shared_pb = Arc::new(ProgressBar::new(total_size));

        file_ids.par_iter().for_each(|file_id| {
            let destination_dir = data_dir.join(file_id);
            ensure_destination(&destination_dir).unwrap();

            let zip_path = resolve_zip_path(file_id, data_dir).unwrap();
            let zip_file = File::open(&zip_path).unwrap();
            let mut archive = zip::ZipArchive::new(zip_file).unwrap();

            for i in 0..archive.len() {
                let mut entry = archive.by_index(i).unwrap();
                extract_entry(&mut entry, &destination_dir, &shared_pb).unwrap();
            }
        });

        assert_eq!(shared_pb.position(), total_size);
    }

    #[test]
    fn test_data_integrity_byte_for_byte_comparison() {
        use rayon::prelude::*;

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        let file_ids = vec!["INTEGRITY_1", "INTEGRITY_2"];
        let test_data_1: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
        let test_data_2: Vec<u8> = (0u8..=255).cycle().take(1024).collect();

        for (file_id, test_data) in file_ids.iter().zip(&[&test_data_1[..], &test_data_2[..]]) {
            let zip_path = data_dir.join(format!("{}.zip", file_id));
            create_test_zip(&zip_path, &[(format!("{}.bin", file_id).as_str(), test_data)])
                .unwrap();
        }

        let total_size = (test_data_1.len() + test_data_2.len()) as u64;
        let shared_pb = Arc::new(ProgressBar::new(total_size));

        file_ids.par_iter().for_each(|file_id| {
            let destination_dir = data_dir.join(file_id);
            ensure_destination(&destination_dir).unwrap();

            let zip_path = resolve_zip_path(file_id, data_dir).unwrap();
            let zip_file = File::open(&zip_path).unwrap();
            let mut archive = zip::ZipArchive::new(zip_file).unwrap();

            for i in 0..archive.len() {
                let mut entry = archive.by_index(i).unwrap();
                extract_entry(&mut entry, &destination_dir, &shared_pb).unwrap();
            }
        });

        let extracted_1 = fs::read(data_dir.join("INTEGRITY_1").join("INTEGRITY_1.bin")).unwrap();
        assert_eq!(extracted_1, test_data_1, "File 1 byte-for-byte mismatch");

        let extracted_2 = fs::read(data_dir.join("INTEGRITY_2").join("INTEGRITY_2.bin")).unwrap();
        assert_eq!(extracted_2, test_data_2, "File 2 byte-for-byte mismatch");
    }

    #[test]
    fn test_concurrent_extraction_accuracy_multiple_files_per_archive() {
        use rayon::prelude::*;

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        let file_ids = vec!["MULTI_FILE_1", "MULTI_FILE_2"];

        let zip1_path = data_dir.join(format!("{}.zip", file_ids[0]));
        create_test_zip(
            &zip1_path,
            &[
                ("file_a.txt", b"Content A"),
                ("file_b.txt", b"Content B"),
                ("file_c.dat", &vec![b'X'; 100]),
            ],
        )
        .unwrap();

        let zip2_path = data_dir.join(format!("{}.zip", file_ids[1]));
        create_test_zip(
            &zip2_path,
            &[
                ("file_d.txt", b"Content D"),
                ("file_e.txt", b"Content E"),
                ("file_f.dat", &vec![b'Y'; 200]),
            ],
        )
        .unwrap();

        let total_size = (9 + 9 + 100 + 9 + 9 + 200) as u64;
        let shared_pb = Arc::new(ProgressBar::new(total_size));

        file_ids.par_iter().for_each(|file_id| {
            let destination_dir = data_dir.join(file_id);
            ensure_destination(&destination_dir).unwrap();

            let zip_path = resolve_zip_path(file_id, data_dir).unwrap();
            let zip_file = File::open(&zip_path).unwrap();
            let mut archive = zip::ZipArchive::new(zip_file).unwrap();

            for i in 0..archive.len() {
                let mut entry = archive.by_index(i).unwrap();
                extract_entry(&mut entry, &destination_dir, &shared_pb).unwrap();
            }
        });

        assert_eq!(
            fs::read_to_string(data_dir.join("MULTI_FILE_1").join("file_a.txt")).unwrap(),
            "Content A"
        );
        assert_eq!(
            fs::read_to_string(data_dir.join("MULTI_FILE_1").join("file_b.txt")).unwrap(),
            "Content B"
        );
        assert_eq!(
            fs::read(data_dir.join("MULTI_FILE_1").join("file_c.dat"))
                .unwrap()
                .len(),
            100
        );

        assert_eq!(
            fs::read_to_string(data_dir.join("MULTI_FILE_2").join("file_d.txt")).unwrap(),
            "Content D"
        );
        assert_eq!(
            fs::read_to_string(data_dir.join("MULTI_FILE_2").join("file_e.txt")).unwrap(),
            "Content E"
        );
        assert_eq!(
            fs::read(data_dir.join("MULTI_FILE_2").join("file_f.dat"))
                .unwrap()
                .len(),
            200
        );
    }

    #[test]
    fn test_single_file_scenario_no_regression() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        let file_id = "SINGLE_FILE";
        let zip_path = data_dir.join(format!("{}.zip", file_id));
        let test_content = b"Single file decompression test";

        create_test_zip(&zip_path, &[("single.txt", test_content)]).unwrap();

        let total_size = test_content.len() as u64;
        let shared_pb = Arc::new(ProgressBar::new(total_size));

        let destination_dir = data_dir.join(file_id);
        ensure_destination(&destination_dir).unwrap();

        let zip_file = File::open(&zip_path).unwrap();
        let mut archive = zip::ZipArchive::new(zip_file).unwrap();

        for i in 0..archive.len() {
            let mut entry = archive.by_index(i).unwrap();
            extract_entry(&mut entry, &destination_dir, &shared_pb).unwrap();
        }

        let extracted = fs::read(destination_dir.join("single.txt")).unwrap();
        assert_eq!(extracted, test_content);
        assert_eq!(shared_pb.position(), total_size);
    }

    #[test]
    fn test_large_zip_file_handling() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        let file_id = "LARGE_FILE";
        let zip_path = data_dir.join(format!("{}.zip", file_id));
        let large_content = vec![b'Z'; 1_048_576]; // 1MB

        create_test_zip(&zip_path, &[("large.bin", &large_content)]).unwrap();

        let total_size = large_content.len() as u64;
        let shared_pb = Arc::new(ProgressBar::new(total_size));

        let destination_dir = data_dir.join(file_id);
        ensure_destination(&destination_dir).unwrap();

        let zip_file = File::open(&zip_path).unwrap();
        let mut archive = zip::ZipArchive::new(zip_file).unwrap();

        for i in 0..archive.len() {
            let mut entry = archive.by_index(i).unwrap();
            extract_entry(&mut entry, &destination_dir, &shared_pb).unwrap();
        }

        let extracted = fs::read(destination_dir.join("large.bin")).unwrap();
        assert_eq!(extracted.len(), large_content.len());
        assert_eq!(extracted, large_content);
        assert_eq!(shared_pb.position(), total_size);
    }

    #[test]
    fn test_mixed_file_sizes_parallel_decompression() {
        use rayon::prelude::*;

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path();

        let file_ids = vec!["TINY", "MEDIUM", "LARGE"];
        let tiny_content = vec![b'T'; 100];
        let medium_content = vec![b'M'; 10_000];
        let large_content = vec![b'L'; 100_000];

        let zip1_path = data_dir.join(format!("{}.zip", file_ids[0]));
        create_test_zip(&zip1_path, &[("tiny.dat", &tiny_content)]).unwrap();

        let zip2_path = data_dir.join(format!("{}.zip", file_ids[1]));
        create_test_zip(&zip2_path, &[("medium.dat", &medium_content)]).unwrap();

        let zip3_path = data_dir.join(format!("{}.zip", file_ids[2]));
        create_test_zip(&zip3_path, &[("large.dat", &large_content)]).unwrap();

        let total_size = (tiny_content.len() + medium_content.len() + large_content.len()) as u64;
        let shared_pb = Arc::new(ProgressBar::new(total_size));

        file_ids.par_iter().for_each(|file_id| {
            let destination_dir = data_dir.join(file_id);
            ensure_destination(&destination_dir).unwrap();

            let zip_path = resolve_zip_path(file_id, data_dir).unwrap();
            let zip_file = File::open(&zip_path).unwrap();
            let mut archive = zip::ZipArchive::new(zip_file).unwrap();

            for i in 0..archive.len() {
                let mut entry = archive.by_index(i).unwrap();
                extract_entry(&mut entry, &destination_dir, &shared_pb).unwrap();
            }
        });

        assert_eq!(
            fs::read(data_dir.join("TINY").join("tiny.dat"))
                .unwrap()
                .len(),
            100
        );
        assert_eq!(
            fs::read(data_dir.join("MEDIUM").join("medium.dat"))
                .unwrap()
                .len(),
            10_000
        );
        assert_eq!(
            fs::read(data_dir.join("LARGE").join("large.dat"))
                .unwrap()
                .len(),
            100_000
        );

        assert_eq!(shared_pb.position(), total_size);
    }
}
