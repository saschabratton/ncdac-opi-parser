//! Download NC DAC data files from the official website.
//!
//! This module provides functionality to download ZIP files and the database structure PDF
//! from the North Carolina Department of Adult Correction website.

use crate::files::FileMetadata;
use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::blocking::Client;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// URL for the database structure PDF
pub const DB_STRUCTURE_PDF_URL: &str = "https://www.doc.state.nc.us/offenders/PublicTables.pdf";

/// Download a file from a URL to a destination path with progress reporting.
///
/// # Arguments
///
/// * `url` - The URL to download from
/// * `dest` - The destination file path
/// * `file_name` - Human-readable file name for progress display
pub fn download_file(
    url: &str,
    dest: &Path,
    file_name: &str,
) -> Result<()> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .context("Failed to create HTTP client")?;

    let mut response = client
        .get(url)
        .send()
        .context(format!("Failed to download from {}", url))?;

    if !response.status().is_success() {
        anyhow::bail!("HTTP error: {}", response.status());
    }

    let total_size = response.content_length().unwrap_or(100_000_000);

    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
            .progress_chars("#>-"),
    );
    pb.set_message(format!("Downloading {}", file_name));

    let mut dest_file = File::create(dest)
        .context(format!("Failed to create file: {}", dest.display()))?;

    let mut downloaded = 0u64;
    let mut buffer = vec![0; 8192];

    loop {
        let bytes_read = response
            .read(&mut buffer)
            .context("Failed to read response")?;

        if bytes_read == 0 {
            break;
        }

        dest_file
            .write_all(&buffer[..bytes_read])
            .context("Failed to write to file")?;

        downloaded += bytes_read as u64;
        pb.set_position(downloaded);
    }

    pb.finish_with_message(format!("✓ Downloaded {}", file_name));

    Ok(())
}

/// Download a data file by its metadata.
///
/// Downloads the file to `./data/{FILE_ID}.zip` relative to the current directory.
///
/// # Arguments
///
/// * `file` - The file metadata
/// * `data_dir` - The data directory path
pub fn download_data_file(file: &FileMetadata, data_dir: &Path) -> Result<()> {
    fs::create_dir_all(data_dir)
        .context(format!("Failed to create directory: {}", data_dir.display()))?;

    let dest = data_dir.join(format!("{}.zip", file.id));

    download_file(
        file.download_url,
        &dest,
        &format!("{} ({})", file.name, file.id),
    )?;

    Ok(())
}

/// Download the database structure PDF.
///
/// # Arguments
///
/// * `data_dir` - The data directory path
pub fn download_db_structure_pdf(data_dir: &Path) -> Result<()> {
    fs::create_dir_all(data_dir)
        .context(format!("Failed to create directory: {}", data_dir.display()))?;

    let dest = data_dir.join("PublicTables.pdf");

    download_file(
        DB_STRUCTURE_PDF_URL,
        &dest,
        "Database Structure (PDF)",
    )?;

    Ok(())
}

/// Get the expected file size from the remote server using HTTP HEAD request.
///
/// # Arguments
///
/// * `url` - The URL to check
///
/// # Returns
///
/// The expected file size in bytes, or None if it cannot be determined
fn get_remote_file_size(url: &str) -> Option<u64> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .ok()?;

    let response = client.head(url).send().ok()?;

    // Manually parse Content-Length header instead of using response.content_length()
    // because reqwest sometimes returns 0 even when the header is present
    if let Some(content_length_header) = response.headers().get("content-length") {
        if let Ok(content_length_str) = content_length_header.to_str() {
            if let Ok(size) = content_length_str.parse::<u64>() {
                return Some(size);
            }
        }
    }

    None
}

/// File download status
#[derive(Debug, PartialEq)]
pub enum FileStatus {
    /// File exists and has correct size
    Complete,
    /// File exists but has incorrect size
    Incomplete,
    /// File does not exist
    Missing,
}

/// Check the download status of a data file.
///
/// This performs a quick HTTP HEAD request to verify the local file size
/// matches the expected size from the server without re-downloading.
///
/// # Arguments
///
/// * `file` - The file metadata
/// * `data_dir` - The data directory path
///
/// # Returns
///
/// The file's download status
pub fn get_file_status(file: &FileMetadata, data_dir: &Path) -> FileStatus {
    let path = data_dir.join(format!("{}.zip", file.id));

    if !path.exists() {
        return FileStatus::Missing;
    }

    let local_size = match fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(_) => return FileStatus::Missing,
    };

    match get_remote_file_size(file.download_url) {
        Some(expected_size) => {
            if local_size == expected_size {
                FileStatus::Complete
            } else {
                FileStatus::Incomplete
            }
        }
        None => {
            // If we can't get the remote size, assume the file is valid
            // This provides a fallback in case of network issues
            FileStatus::Complete
        }
    }
}

/// Check if a data file exists and has the correct size.
///
/// This performs a quick HTTP HEAD request to verify the local file size
/// matches the expected size from the server without re-downloading.
///
/// # Arguments
///
/// * `file` - The file metadata
/// * `data_dir` - The data directory path
///
/// # Returns
///
/// `true` if the file exists and has the correct size, `false` otherwise
pub fn is_file_downloaded(file: &FileMetadata, data_dir: &Path) -> bool {
    get_file_status(file, data_dir) == FileStatus::Complete
}


/// Get the data directory path.
///
/// Returns `./data/` relative to the current working directory.
pub fn get_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

/// Get expected file sizes from a ZIP archive.
///
/// Opens the ZIP file and retrieves the uncompressed sizes of all entries.
///
/// # Arguments
///
/// * `zip_path` - Path to the ZIP file
///
/// # Returns
///
/// HashMap mapping file names to their expected uncompressed sizes, or None if the ZIP can't be read
fn get_expected_sizes_from_zip(zip_path: &Path) -> Option<HashMap<String, u64>> {
    let file = File::open(zip_path).ok()?;
    let mut archive = zip::ZipArchive::new(file).ok()?;

    let mut sizes = HashMap::new();
    for i in 0..archive.len() {
        if let Ok(entry) = archive.by_index(i) {
            let name = entry.name().to_string();
            sizes.insert(name, entry.size());
        }
    }

    Some(sizes)
}

/// Check if decompressed files (.des and .dat) exist.
///
/// This is a fast check that only verifies file existence, not integrity.
/// Use `are_decompressed_files_valid()` if you need to validate hashes.
///
/// # Arguments
///
/// * `file` - The file metadata
/// * `data_dir` - The data directory path
///
/// # Returns
///
/// `true` if both .des and .dat files exist, `false` otherwise
pub fn decompressed_files_exist(file: &FileMetadata, data_dir: &Path) -> bool {
    let file_dir = data_dir.join(file.id);
    let des_path = file_dir.join(format!("{}.des", file.id));
    let dat_path = file_dir.join(format!("{}.dat", file.id));

    des_path.exists() && dat_path.exists()
}

/// Check if decompressed files (.des and .dat) are valid.
///
/// Validates that both .des and .dat files exist and have the correct sizes
/// by comparing against the expected sizes from the ZIP archive.
///
/// # Arguments
///
/// * `file` - The file metadata
/// * `data_dir` - The data directory path
///
/// # Returns
///
/// `true` if both .des and .dat files exist and have correct sizes, `false` otherwise
pub fn  (file: &FileMetadata, data_dir: &Path) -> bool {
    if !decompressed_files_exist(file, data_dir) {
        return false;
    }

    let file_dir = data_dir.join(file.id);
    let des_path = file_dir.join(format!("{}.des", file.id));
    let dat_path = file_dir.join(format!("{}.dat", file.id));

    let zip_path = data_dir.join(format!("{}.zip", file.id));
    let expected_sizes = match get_expected_sizes_from_zip(&zip_path) {
        Some(sizes) => sizes,
        None => {
            // If we can't read the ZIP, assume decompressed files are valid
            // This handles cases where ZIP was deleted after extraction
            return true;
        }
    };

    let des_filename = format!("{}.des", file.id);
    if let Some(&expected_des_size) = expected_sizes.get(&des_filename) {
        if let Ok(metadata) = fs::metadata(&des_path) {
            if metadata.len() != expected_des_size {
                return false;
            }
        } else {
            return false;
        }
    }

    let dat_filename = format!("{}.dat", file.id);
    if let Some(&expected_dat_size) = expected_sizes.get(&dat_filename) {
        if let Ok(metadata) = fs::metadata(&dat_path) {
            if metadata.len() != expected_dat_size {
                return false;
            }
        } else {
            return false;
        }
    }

    true
}

/// Categorization of files by their download status
#[derive(Debug, Default)]
pub struct FilesStatus {
    /// Files that don't exist at all
    pub missing: Vec<String>,
    /// Files that exist but have incorrect size
    pub incomplete: Vec<String>,
    /// Decompressed files exist but ZIP is missing (can't verify)
    pub unverifiable: Vec<String>,
}

/// Categorize files by their download status.
///
/// Checks in this order:
/// 1. If decompressed files (.des and .dat) exist and are valid against ZIP, file is considered available
/// 2. If decompressed files exist but ZIP is missing, file is marked as unverifiable
/// 3. If decompressed files are invalid or don't exist, check ZIP file:
///    - If ZIP exists and has correct size (via HTTP HEAD), file will be re-decompressed
///    - If ZIP exists but has wrong size, file is marked as incomplete (needs re-download)
///    - If ZIP doesn't exist, file is marked as missing (needs download)
///
/// # Arguments
///
/// * `files` - Array of file metadata to check
/// * `data_dir` - The data directory path
///
/// # Returns
///
/// `FilesStatus` containing vectors of missing, incomplete, and unverifiable file IDs
pub fn categorize_files(files: &[FileMetadata], data_dir: &Path) -> FilesStatus {
    let mut status = FilesStatus::default();

    for file in files {
        let des_dat_exist = decompressed_files_exist(file, data_dir);
        let zip_status = get_file_status(file, data_dir);

        if des_dat_exist && zip_status == FileStatus::Missing {
            status.unverifiable.push(file.id.to_string());
            continue;
        }

        if are_decompressed_files_valid(file, data_dir) {
            continue;
        }

        match zip_status {
            FileStatus::Complete => {
            }
            FileStatus::Incomplete => {
                status.incomplete.push(file.id.to_string());
            }
            FileStatus::Missing => {
                status.missing.push(file.id.to_string());
            }
        }
    }

    status
}

/// Check which files are missing from the data directory.
///
/// Checks in this order:
/// 1. If decompressed files (.des and .dat) exist, file is considered available
/// 2. If ZIP file exists and has valid size, file is considered available
/// 3. Otherwise, file is considered missing
///
/// # Arguments
///
/// * `files` - Array of file metadata to check
/// * `data_dir` - The data directory path
///
/// # Returns
///
/// Vector of file IDs that are missing (neither decompressed files nor valid ZIP exists)
pub fn get_missing_files(files: &[FileMetadata], data_dir: &Path) -> Vec<String> {
    let status = categorize_files(files, data_dir);
    let mut all_missing = status.missing;
    all_missing.extend(status.incomplete);
    all_missing
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_get_data_dir() {
        let data_dir = get_data_dir();
        assert_eq!(data_dir, PathBuf::from("./data"));
    }

    #[test]
    fn test_db_structure_url() {
        assert!(DB_STRUCTURE_PDF_URL.starts_with("https://"));
        assert!(DB_STRUCTURE_PDF_URL.contains("PublicTables.pdf"));
    }
}
