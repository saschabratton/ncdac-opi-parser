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
    /// SHA-256 hash for ZIP file validation
    pub sha256: Option<&'static str>,
    /// SHA-256 hash for decompressed .des file validation
    pub des_sha256: Option<&'static str>,
    /// SHA-256 hash for decompressed .dat file validation
    pub dat_sha256: Option<&'static str>,
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
    ///     "https://www.doc.state.nc.us/offenders/OFNT3AA1.zip",
    ///     None,
    ///     None,
    ///     None
    /// );
    /// assert_eq!(file.id, "OFNT3AA1");
    /// ```
    #[must_use]
    pub const fn new(
        id: &'static str,
        name: &'static str,
        download_url: &'static str,
        sha256: Option<&'static str>,
        des_sha256: Option<&'static str>,
        dat_sha256: Option<&'static str>,
    ) -> Self {
        Self {
            id,
            name,
            download_url,
            sha256,
            des_sha256,
            dat_sha256,
        }
    }
}

/// Static array containing all NC DAC file metadata.
///
/// This array contains metadata for all 12 NC DAC file types in the system.
/// Download URLs are from https://webapps.doc.state.nc.us/opi/downloads.do?method=view
/// SHA-256 hashes were pre-computed from the official downloads for validation.
pub const FILES: [FileMetadata; 12] = [
    FileMetadata::new(
        "OFNT3AA1",
        "Offender Profile",
        "https://www.doc.state.nc.us/offenders/OFNT3AA1.zip",
        Some("95648caeaa88969b992cdcb1b68806e5fdee768313481eb01b5940fbbe4ec74a"),
        Some("7fe77769b1590a6731d215960e2fae1161e0f6aaa4891b967ead3849745f3310"),
        Some("53d25ad346658d6c4060ddb8e61f1af47135d0ad2c927813eacccb891c82f4d5"),
    ),
    FileMetadata::new(
        "APPT7AA1",
        "Probation and Parole Client Profile",
        "https://www.doc.state.nc.us/offenders/APPT7AA1.zip",
        Some("acba721152e5a69780b8c31b45a2fb13c576592da51454d7781e808f4f56405e"),
        Some("b00252add83de8179f4a0644a3d528bb7058d23325647be6bbd9a072672dd7a0"),
        Some("95b1e3b2afa3445dbeacd0c3a3795ad40a70fe35a3de301ac643bff2e158bac4"),
    ),
    FileMetadata::new(
        "APPT9BJ1",
        "Impact Scheduling Request",
        "https://www.doc.state.nc.us/offenders/APPT9BJ1.zip",
        Some("b60900557c42801731a4d9fa8d8b967194e672088d69e3ec61fa647e0968f9f3"),
        Some("bbfe4df95ae45c050c2c67cfcfda92cb968ec4a35e969494b5726ee713f4afce"),
        Some("3ac0c5dfcb3cb0c67d754dfea20de2aa23814909a4dfc1659bc2f51af85b7830"),
    ),
    FileMetadata::new(
        "INMT4AA1",
        "Inmate Profile",
        "https://www.doc.state.nc.us/offenders/INMT4AA1.zip",
        Some("95cc430a8730255285bc01be9ad8c92ad48d31d71ba904e40c1f9cdb6c3a5bb1"),
        Some("8eec828036226856d1be9ec976913b6d159c9f8411a1d495343e4801d8a9c07c"),
        Some("fdb01a4bb931258691c26627ca9a0e07820f275d55ab076918ea09c5ae650ac1"),
    ),
    FileMetadata::new(
        "INMT4BB1",
        "Sentence Computations",
        "https://www.doc.state.nc.us/offenders/INMT4BB1.zip",
        Some("2bf9c1f549f932ba7209148138af752099fe8e79b54998c64ff0b5e6ceb03842"),
        Some("ba8875855bfc81a5e0fae06580bffe98ed6fa5f6e93018b4f4d35d3fb63ab847"),
        Some("ba7f9d21412fca709a13784b7a31e814bbc8281f3092b2d1ed4c84dea289f548"),
    ),
    FileMetadata::new(
        "INMT4CA1",
        "Parole Analyst Review",
        "https://www.doc.state.nc.us/offenders/INMT4CA1.zip",
        Some("79ee997f22378e5f0909cba01d5da4e2f040b0a415122c98df89c0128a0d51b5"),
        Some("786d26856e80dfc24ff3352680d3c444acba56fb42b40eac82b7b2ca1c8debcf"),
        Some("2e32ca56e8a7325fd39dbeb5d3207fd2949932490a93158ae8b740b28a22c2fe"),
    ),
    FileMetadata::new(
        "INMT9CF1",
        "Disciplinary Infractions",
        "https://www.doc.state.nc.us/offenders/INMT9CF1.zip",
        Some("8abba1dca907da4028f5714d5771b63cd6f846a5ffea64f6fc6f732c23c00d77"),
        Some("9b86292ef8d90af5662b83d3e84621594d885fd6ebd748a4095f1e3169c9c7b5"),
        Some("de4f629d260c9d9fdf6f150a72e79d7923013f2cf6eb991cae66ec1ca7bffb13"),
    ),
    FileMetadata::new(
        "OFNT1BA1",
        "Financial Obligation",
        "https://www.doc.state.nc.us/offenders/OFNT1BA1.zip",
        Some("b960f1e304566030c9a675b8882c3ccb6e0009cdea54be2ce20968bb4fb397b6"),
        Some("3146f4c95790d220614140a291bf4ae7d99914d2bb4030db3640bdc3ad4a47f9"),
        Some("0ccac128e63570c05fb70eb2459cfd3440187f7b4f33a907b2954b41780a6460"),
    ),
    FileMetadata::new(
        "OFNT3BB1",
        "Court Commmitment",
        "https://www.doc.state.nc.us/offenders/OFNT3BB1.zip",
        Some("09a4998925675643ed4130fca938bd04cb9c746965b8eec177890b495f817591"),
        Some("857ccc75e587e7c15436ad8dca7414764ab08fc606392556d7c3b2fe3b94e44e"),
        Some("290265eeceecf990d73bdfcc583025d9c07e8c201988a509d07da00e4f2a7b36"),
    ),
    FileMetadata::new(
        "OFNT3CE1",
        "Sentence Component",
        "https://www.doc.state.nc.us/offenders/OFNT3CE1.zip",
        Some("6e346c3d3cd435474d36061626b1d519811de7d69ce0d4a610a4f9ccfae44e19"),
        Some("f0f3c7bf3df7d2749da40021cacba35aece16fdd69bc7fc173557c23289c5453"),
        Some("23affe4d8b2e1c6c1b3fb1bf7d7305793e9a13e196333d0f026d6d47a9073af3"),
    ),
    FileMetadata::new(
        "OFNT3DE1",
        "Special Conditions and Sanctions",
        "https://www.doc.state.nc.us/offenders/OFNT3DE1.zip",
        Some("b4eecb632506fe291da77aca9cb6a9c1eec27a9c2b489260ed22b091e2247043"),
        Some("2f895e17639df2b1c4e178577036e09d9f6e8b5c5d3fdf7a4a3ee4ae1dbab08c"),
        Some("feaf0e3d993bf7e92816a9873a13ab778efe0d0b32b80a2279a15b933b686dd0"),
    ),
    FileMetadata::new(
        "OFNT9BE1",
        "Warrant Issued",
        "https://www.doc.state.nc.us/offenders/OFNT9BE1.zip",
        Some("00b482035dd4b0b08f0f5de40b7f3e46fe266ffdf9c04b81b5f0ccef9c1278a3"),
        Some("22f2036f7e18a3329af128f9813259fc1946277f3ac3de17d42e88a516a3038d"),
        Some("f3d01914f6e58f04c6fde243372bc7f2c6e52d2a6201501778185f15bf6cc7a6"),
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
            None,
            None,
            None,
        );
        assert_eq!(file.id, "TEST1234");
        assert_eq!(file.name, "Test File");
        assert_eq!(file.download_url, "https://example.com/TEST1234.zip");
        assert_eq!(file.sha256, None);
        assert_eq!(file.des_sha256, None);
        assert_eq!(file.dat_sha256, None);
    }

    #[test]
    fn test_file_metadata_clone() {
        let file1 = FileMetadata::new(
            "TEST1234",
            "Test File",
            "https://example.com/TEST1234.zip",
            None,
            None,
            None,
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
