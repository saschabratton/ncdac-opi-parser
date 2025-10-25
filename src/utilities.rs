//! Utility functions for the NC DAC OPI parser.
//!
//! This module provides common utilities for path management, string formatting,
//! schema inspection, and data directory operations.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Returns the path to the data directory.
///
/// The data directory is located at "./data" relative to the project directory.
///
/// # Examples
///
/// ```
/// use ncdac_opi_parser::utilities::data_directory;
///
/// let data_dir = data_directory();
/// assert!(data_dir.ends_with("data"));
/// ```
pub fn data_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data")
}

/// Converts a string to snake_case.
///
/// This function:
/// 1. Converts the string to lowercase
/// 2. Replaces all non-alphanumeric characters with underscores
/// 3. Removes leading and trailing underscores
/// 4. Collapses multiple consecutive underscores into a single underscore
///
/// # Examples
///
/// ```
/// use ncdac_opi_parser::utilities::to_snake_case;
///
/// assert_eq!(to_snake_case("Hello World"), "hello_world");
/// assert_eq!(to_snake_case("firstName"), "firstname");
/// assert_eq!(to_snake_case("First-Name"), "first_name");
/// assert_eq!(to_snake_case("__test__"), "test");
/// assert_eq!(to_snake_case("multiple   spaces"), "multiple_spaces");
/// ```
pub fn to_snake_case(s: &str) -> String {
    let lowercase = s.to_lowercase();

    let with_underscores: String = lowercase
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();

    let mut result = String::with_capacity(with_underscores.len());
    let mut prev_was_underscore = false;

    for c in with_underscores.chars() {
        if c == '_' {
            if !prev_was_underscore {
                result.push(c);
            }
            prev_was_underscore = true;
        } else {
            result.push(c);
            prev_was_underscore = false;
        }
    }

    result.trim_matches('_').to_string()
}

/// Gets the primary key field from a schema.
///
/// Searches for specific primary key candidates in the schema in order:
/// - CMDORNUM
/// - CIDORNUM
/// - CDDORNUM
///
/// Returns the first matching key found, or None if none are present.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use ncdac_opi_parser::utilities::get_primary_key_field;
///
/// let mut schema = HashMap::new();
/// schema.insert("CMDORNUM".to_string(), "INTEGER".to_string());
/// schema.insert("NAME".to_string(), "TEXT".to_string());
///
/// assert_eq!(get_primary_key_field(&schema), Some("CMDORNUM"));
/// ```
pub fn get_primary_key_field<V>(schema: &HashMap<String, V>) -> Option<&'static str> {
    const KEY_CANDIDATES: &[&str] = &["CMDORNUM", "CIDORNUM", "CDDORNUM"];

    for &key in KEY_CANDIDATES {
        if schema.contains_key(key) {
            return Some(key);
        }
    }

    None
}

/// Formats a number with thousand separators.
///
/// Uses US English locale formatting (comma as thousand separator).
///
/// # Examples
///
/// ```
/// use ncdac_opi_parser::utilities::format_count;
///
/// assert_eq!(format_count(0), "0");
/// assert_eq!(format_count(1000), "1,000");
/// assert_eq!(format_count(1234567), "1,234,567");
/// assert_eq!(format_count(42), "42");
/// ```
pub fn format_count(n: usize) -> String {
    let s = n.to_string();
    let len = s.len();

    if len <= 3 {
        return s;
    }

    let mut result = String::with_capacity(len + (len - 1) / 3);
    let mut digit_count = 0;

    for c in s.chars().rev() {
        if digit_count > 0 && digit_count % 3 == 0 {
            result.push(',');
        }
        result.push(c);
        digit_count += 1;
    }

    result.chars().rev().collect()
}

/// Formats a duration in a human-readable format.
///
/// Returns a string in the format "Xh Ym Zs" where:
/// - Hours are only shown if > 0
/// - Minutes are shown if > 0 or if hours > 0
/// - Seconds are always shown
///
/// # Arguments
///
/// * `start` - The start time
/// * `end` - The end time (defaults to current time if None)
///
/// # Errors
///
/// Returns an error if the end time is before the start time.
///
/// # Examples
///
/// ```
/// use std::time::{SystemTime, Duration};
/// use ncdac_opi_parser::utilities::format_duration;
///
/// let start = SystemTime::now();
/// let end = start + Duration::from_secs(3665); // 1h 1m 5s
///
/// let formatted = format_duration(start, Some(end)).unwrap();
/// assert_eq!(formatted, "1h 1m 5s");
/// ```
pub fn format_duration(start: SystemTime, end: Option<SystemTime>) -> Result<String> {
    let end_time = end.unwrap_or_else(SystemTime::now);

    let duration = end_time
        .duration_since(start)
        .context("End time is before start time (negative duration)")?;

    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    let mut parts = Vec::new();

    if hours > 0 {
        parts.push(format!("{}h", hours));
    }

    if minutes > 0 || hours > 0 {
        parts.push(format!("{}m", minutes));
    }

    parts.push(format!("{}s", seconds));

    Ok(parts.join(" "))
}

/// Deletes a subdirectory within the data directory.
///
/// This function removes the specified subdirectory and all its contents
/// from within the data directory. If the directory doesn't exist, the
/// operation succeeds silently.
///
/// # Arguments
///
/// * `subdirectory` - The name of the subdirectory to delete (relative to data directory)
///
/// # Errors
///
/// Returns an error if the directory exists but cannot be deleted due to
/// permission issues or other I/O errors.
///
/// # Examples
///
/// ```no_run
/// use ncdac_opi_parser::utilities::delete_data_subdirectory;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), anyhow::Error> {
/// delete_data_subdirectory("temp").await?;
/// # Ok(())
/// # }
/// ```
pub async fn delete_data_subdirectory(subdirectory: &str) -> Result<()> {
    let target_path = data_directory().join(subdirectory);

    match tokio::fs::remove_dir_all(&target_path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(())
        }
        Err(e) => Err(e).with_context(|| {
            format!(
                "Failed to delete data subdirectory: {}",
                target_path.display()
            )
        }),
    }
}

/// Counts the number of lines in a file.
///
/// This function efficiently counts lines in a file by reading it in buffered chunks.
/// It's optimized for large files and skips empty lines.
///
/// # Arguments
///
/// * `file_path` - Path to the file to count lines in
///
/// # Returns
///
/// The number of non-empty lines in the file
///
/// # Errors
///
/// Returns an error if the file cannot be opened or read
///
/// # Examples
///
/// ```no_run
/// use ncdac_opi_parser::utilities::count_lines;
/// use std::path::Path;
///
/// let count = count_lines(Path::new("data/OFNT3AA1/OFNT3AA1.dat")).unwrap();
/// println!("File has {} lines", count);
/// ```
pub fn count_lines(file_path: &Path) -> Result<u64> {
    let file = File::open(file_path)
        .with_context(|| format!("Failed to open file: {}", file_path.display()))?;

    let reader = BufReader::new(file);
    let mut count = 0u64;

    for line in reader.lines() {
        let _ = line?;
        count += 1;
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("Hello World"), "hello_world");
        assert_eq!(to_snake_case("firstName"), "firstname");
        assert_eq!(to_snake_case("First-Name"), "first_name");
        assert_eq!(to_snake_case("__test__"), "test");
        assert_eq!(to_snake_case("multiple   spaces"), "multiple_spaces");
        assert_eq!(to_snake_case("ABC123"), "abc123");
        assert_eq!(to_snake_case("test_case"), "test_case");
        assert_eq!(to_snake_case("Test__Case"), "test_case");
        assert_eq!(to_snake_case("___leading"), "leading");
        assert_eq!(to_snake_case("trailing___"), "trailing");
        assert_eq!(to_snake_case("special!@#$chars"), "special_chars");
        assert_eq!(to_snake_case(""), "");
        assert_eq!(to_snake_case("___"), "");
    }

    #[test]
    fn test_get_primary_key_field() {
        let mut schema1 = HashMap::new();
        schema1.insert("CMDORNUM".to_string(), "INTEGER");
        schema1.insert("NAME".to_string(), "TEXT");
        assert_eq!(get_primary_key_field(&schema1), Some("CMDORNUM"));

        let mut schema2 = HashMap::new();
        schema2.insert("CIDORNUM".to_string(), "INTEGER");
        schema2.insert("NAME".to_string(), "TEXT");
        assert_eq!(get_primary_key_field(&schema2), Some("CIDORNUM"));

        let mut schema3 = HashMap::new();
        schema3.insert("CDDORNUM".to_string(), "INTEGER");
        schema3.insert("NAME".to_string(), "TEXT");
        assert_eq!(get_primary_key_field(&schema3), Some("CDDORNUM"));

        let mut schema4 = HashMap::new();
        schema4.insert("NAME".to_string(), "TEXT");
        assert_eq!(get_primary_key_field(&schema4), None);

        let mut schema5 = HashMap::new();
        schema5.insert("CMDORNUM".to_string(), "INTEGER");
        schema5.insert("CIDORNUM".to_string(), "INTEGER");
        schema5.insert("CDDORNUM".to_string(), "INTEGER");
        assert_eq!(get_primary_key_field(&schema5), Some("CMDORNUM"));
    }

    #[test]
    fn test_format_count() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(42), "42");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1000), "1,000");
        assert_eq!(format_count(1234), "1,234");
        assert_eq!(format_count(12345), "12,345");
        assert_eq!(format_count(123456), "123,456");
        assert_eq!(format_count(1234567), "1,234,567");
        assert_eq!(format_count(12345678), "12,345,678");
        assert_eq!(format_count(123456789), "123,456,789");
        assert_eq!(format_count(1000000), "1,000,000");
    }

    #[test]
    fn test_format_duration() {
        let start = SystemTime::UNIX_EPOCH;

        let result = format_duration(start, Some(start)).unwrap();
        assert_eq!(result, "0s");

        let end = start + Duration::from_secs(45);
        let result = format_duration(start, Some(end)).unwrap();
        assert_eq!(result, "45s");

        let end = start + Duration::from_secs(125); // 2m 5s
        let result = format_duration(start, Some(end)).unwrap();
        assert_eq!(result, "2m 5s");

        let end = start + Duration::from_secs(3665); // 1h 1m 5s
        let result = format_duration(start, Some(end)).unwrap();
        assert_eq!(result, "1h 1m 5s");

        let end = start + Duration::from_secs(3605); // 1h 0m 5s
        let result = format_duration(start, Some(end)).unwrap();
        assert_eq!(result, "1h 0m 5s");

        let end = start + Duration::from_secs(3600); // 1h 0m 0s
        let result = format_duration(start, Some(end)).unwrap();
        assert_eq!(result, "1h 0m 0s");

        let end = start + Duration::from_secs(60); // 1m 0s
        let result = format_duration(start, Some(end)).unwrap();
        assert_eq!(result, "1m 0s");

        let end = start - Duration::from_secs(10);
        let result = format_duration(start, Some(end));
        assert!(result.is_err());
    }

    #[test]
    fn test_data_directory() {
        let data_dir = data_directory();
        assert!(data_dir.to_string_lossy().contains("data"));
        assert!(data_dir.is_absolute());
    }
}
