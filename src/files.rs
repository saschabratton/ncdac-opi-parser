//! NC DAC file metadata and lookup functionality.
//!
//! This module provides metadata for the 12 NC DAC file types and a lookup function
//! to retrieve file information by ID.

/// Metadata for a NC DAC file type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileMetadata {
    /// Unique identifier for the file type
    pub id: &'static str,
    /// Human-readable name of the file type
    pub name: &'static str,
    /// Download URL for the ZIP file
    pub download_url: &'static str,
}

impl FileMetadata {
    /// Creates a new FileMetadata instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use ncdac_opi_parser::files::FileMetadata;
    ///
    /// let file = FileMetadata::new(
    ///     "OFNT3AA1",
    ///     "Offender Profile",
    ///     "https://www.doc.state.nc.us/offenders/OFNT3AA1.zip"
    /// );
    /// assert_eq!(file.id, "OFNT3AA1");
    /// ```
    #[must_use]
    pub const fn new(
        id: &'static str,
        name: &'static str,
        download_url: &'static str,
    ) -> Self {
        Self {
            id,
            name,
            download_url,
        }
    }
}

/// Static array containing all NC DAC file metadata.
///
/// This array contains metadata for all 12 NC DAC file types in the system.
/// Download URLs are from https://webapps.doc.state.nc.us/opi/downloads.do?method=view
pub const FILES: [FileMetadata; 12] = [
    FileMetadata::new(
        "OFNT3AA1",
        "Offender Profile",
        "https://www.doc.state.nc.us/offenders/OFNT3AA1.zip",
    ),
    FileMetadata::new(
        "APPT7AA1",
        "Probation and Parole Client Profile",
        "https://www.doc.state.nc.us/offenders/APPT7AA1.zip",
    ),
    FileMetadata::new(
        "APPT9BJ1",
        "Impact Scheduling Request",
        "https://www.doc.state.nc.us/offenders/APPT9BJ1.zip",
    ),
    FileMetadata::new(
        "INMT4AA1",
        "Inmate Profile",
        "https://www.doc.state.nc.us/offenders/INMT4AA1.zip",
    ),
    FileMetadata::new(
        "INMT4BB1",
        "Sentence Computations",
        "https://www.doc.state.nc.us/offenders/INMT4BB1.zip",
    ),
    FileMetadata::new(
        "INMT4CA1",
        "Parole Analyst Review",
        "https://www.doc.state.nc.us/offenders/INMT4CA1.zip",
    ),
    FileMetadata::new(
        "INMT9CF1",
        "Disciplinary Infractions",
        "https://www.doc.state.nc.us/offenders/INMT9CF1.zip",
    ),
    FileMetadata::new(
        "OFNT1BA1",
        "Financial Obligation",
        "https://www.doc.state.nc.us/offenders/OFNT1BA1.zip",
    ),
    FileMetadata::new(
        "OFNT3BB1",
        "Court Commmitment",
        "https://www.doc.state.nc.us/offenders/OFNT3BB1.zip",
    ),
    FileMetadata::new(
        "OFNT3CE1",
        "Sentence Component",
        "https://www.doc.state.nc.us/offenders/OFNT3CE1.zip",
    ),
    FileMetadata::new(
        "OFNT3DE1",
        "Special Conditions and Sanctions",
        "https://www.doc.state.nc.us/offenders/OFNT3DE1.zip",
    ),
    FileMetadata::new(
        "OFNT9BE1",
        "Warrant Issued",
        "https://www.doc.state.nc.us/offenders/OFNT9BE1.zip",
    ),
];

/// Retrieves file metadata by ID.
///
/// Performs a linear search through the `FILES` array to find a file with the matching ID.
/// Returns `None` if no file with the given ID exists.
///
/// # Arguments
///
/// * `id` - The file ID to search for
///
/// # Examples
///
/// ```
/// use ncdac_opi_parser::files::get_file_by_id;
///
/// let file = get_file_by_id("OFNT3AA1");
/// assert!(file.is_some());
/// assert_eq!(file.unwrap().name, "Offender Profile");
///
/// let not_found = get_file_by_id("INVALID");
/// assert!(not_found.is_none());
/// ```
#[must_use]
pub fn get_file_by_id(id: &str) -> Option<&'static FileMetadata> {
    FILES.iter().find(|file| file.id == id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_files_array_length() {
        assert_eq!(FILES.len(), 12);
    }

    #[test]
    fn test_get_file_by_id_found() {
        let file = get_file_by_id("OFNT3AA1");
        assert!(file.is_some());
        assert_eq!(file.unwrap().id, "OFNT3AA1");
        assert_eq!(file.unwrap().name, "Offender Profile");
    }

    #[test]
    fn test_get_file_by_id_all_files() {
        // Test that all files can be found
        assert!(get_file_by_id("OFNT3AA1").is_some());
        assert!(get_file_by_id("APPT7AA1").is_some());
        assert!(get_file_by_id("APPT9BJ1").is_some());
        assert!(get_file_by_id("INMT4AA1").is_some());
        assert!(get_file_by_id("INMT4BB1").is_some());
        assert!(get_file_by_id("INMT4CA1").is_some());
        assert!(get_file_by_id("INMT9CF1").is_some());
        assert!(get_file_by_id("OFNT1BA1").is_some());
        assert!(get_file_by_id("OFNT3BB1").is_some());
        assert!(get_file_by_id("OFNT3CE1").is_some());
        assert!(get_file_by_id("OFNT3DE1").is_some());
        assert!(get_file_by_id("OFNT9BE1").is_some());
    }

    #[test]
    fn test_get_file_by_id_not_found() {
        let file = get_file_by_id("INVALID");
        assert!(file.is_none());
    }

    #[test]
    fn test_get_file_by_id_case_sensitive() {
        let file = get_file_by_id("ofnt3aa1");
        assert!(file.is_none());
    }

    #[test]
    fn test_file_metadata_new() {
        let file = FileMetadata::new(
            "TEST1234",
            "Test File",
            "https://example.com/TEST1234.zip",
        );
        assert_eq!(file.id, "TEST1234");
        assert_eq!(file.name, "Test File");
        assert_eq!(file.download_url, "https://example.com/TEST1234.zip");
    }

    #[test]
    fn test_file_metadata_clone() {
        let file1 = FileMetadata::new(
            "TEST1234",
            "Test File",
            "https://example.com/TEST1234.zip",
        );
        let file2 = file1;
        assert_eq!(file1, file2);
    }

    #[test]
    fn test_specific_file_names() {
        let file = get_file_by_id("APPT7AA1").unwrap();
        assert_eq!(file.name, "Probation and Parole Client Profile");

        let file = get_file_by_id("INMT9CF1").unwrap();
        assert_eq!(file.name, "Disciplinary Infractions");

        // Note: The original has a typo "Commmitment" with 3 m's
        let file = get_file_by_id("OFNT3BB1").unwrap();
        assert_eq!(file.name, "Court Commmitment");
    }

    #[test]
    fn test_all_files_have_download_urls() {
        for file in &FILES {
            assert!(file.download_url.starts_with("https://"));
            assert!(file.download_url.contains(&file.id));
            assert!(file.download_url.ends_with(".zip"));
        }
    }
}
