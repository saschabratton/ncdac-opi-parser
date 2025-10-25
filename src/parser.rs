//! Fixed-width data file parser for NC DAC OPI records.
//!
//! This module provides functionality to parse fixed-width `.dat` files using
//! schema definitions from `.des` descriptor files. It handles line-by-line
//! parsing with automatic field extraction and value coercion.
//!
//! # Example
//!
//! ```no_run
//! use ncdac_opi_parser::parser::DataParser;
//! use std::collections::HashMap;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let parser = DataParser::new("OFNT1BA1")?;
//!
//! for record in parser.parse()? {
//!     let record = record?;
//!     if let Some(Some(value)) = record.get("CMDORNUM") {
//!         println!("Offender ID: {}", value);
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use crate::file_description::FileDescription;
use crate::utilities::data_directory;
use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Lines};
use std::path::PathBuf;

/// Regex pattern for detecting strings that are all question marks.
///
/// Used in value coercion to identify null marker values like "???".
static ALL_QUESTION_MARKS: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\?+$").expect("Invalid question mark regex pattern"));

/// The null date marker used in the data files.
///
/// Date fields with this value should be treated as null/missing.
const NULL_DATE_MARKER: &str = "0001-01-01";

/// Parser for fixed-width DAT files.
///
/// The `DataParser` reads DAT files line by line and extracts field values
/// according to a schema defined in a DES descriptor file. It uses the
/// `FileDescription` to determine field positions and lengths.
///
/// # Architecture
///
/// The parser follows Rust's iterator pattern for memory-efficient streaming:
/// - No need to load the entire file into memory
/// - Records are parsed on-demand as the iterator is consumed
/// - Automatic resource cleanup when the iterator is dropped
///
/// # Value Coercion
///
/// The parser applies these coercion rules to all extracted field values:
/// 1. Whitespace is trimmed from both ends
/// 2. Empty strings become `None`
/// 3. The date "0001-01-01" becomes `None` (null date marker)
/// 4. Strings containing only "?" characters become `None` (null markers)
/// 5. All other values are preserved as `Some(String)`
///
/// # Example
///
/// ```no_run
/// use ncdac_opi_parser::parser::DataParser;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a parser for the OFNT1BA1 file
/// let parser = DataParser::new("OFNT1BA1")?;
///
/// // Parse all records
/// for result in parser.parse()? {
///     let record = result?;
///
///     // Access fields by their code
///     if let Some(offender_id) = &record.get("CMDORNUM").and_then(|v| v.as_ref()) {
///         println!("Processing offender: {}", offender_id);
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct DataParser {
    /// The file ID (e.g., "OFNT1BA1")
    file_id: String,
    /// The parsed schema definition
    file_description: FileDescription,
}

impl DataParser {
    /// Creates a new `DataParser` for the specified file ID.
    ///
    /// This constructor loads the corresponding `.des` descriptor file to
    /// understand the schema of the `.dat` file.
    ///
    /// # Arguments
    ///
    /// * `file_id` - The file identifier (e.g., "OFNT1BA1")
    ///
    /// # Errors
    ///
    /// Returns an error if the descriptor file cannot be read or parsed.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ncdac_opi_parser::parser::DataParser;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let parser = DataParser::new("OFNT1BA1")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(file_id: &str) -> Result<Self> {
        let file_description = FileDescription::new(file_id)?;
        Ok(Self {
            file_id: file_id.to_string(),
            file_description,
        })
    }

    /// Returns a reference to the file description schema.
    ///
    /// Useful for inspecting the schema before or during parsing.
    pub fn schema(&self) -> &FileDescription {
        &self.file_description
    }

    /// Returns the file ID.
    pub fn file_id(&self) -> &str {
        &self.file_id
    }

    /// Parses the DAT file and returns an iterator over records.
    ///
    /// Each record is a `HashMap` where keys are field codes (from the schema)
    /// and values are `Option<String>` (None for null values after coercion).
    ///
    /// # Returns
    ///
    /// A `RecordIterator` that yields `Result<HashMap<String, Option<String>>>`.
    ///
    /// # Errors
    ///
    /// Returns an error if the DAT file cannot be opened.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ncdac_opi_parser::parser::DataParser;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let parser = DataParser::new("OFNT1BA1")?;
    /// let mut count = 0;
    ///
    /// for record_result in parser.parse()? {
    ///     let record = record_result?;
    ///     count += 1;
    /// }
    ///
    /// println!("Parsed {} records", count);
    /// # Ok(())
    /// # }
    /// ```
    pub fn parse(&self) -> Result<RecordIterator<BufReader<File>>> {
        let file_path = self.get_dat_file_path();

        let file = File::open(&file_path).with_context(|| {
            format!("Failed to open DAT file: {}", file_path.display())
        })?;

        let reader = BufReader::new(file);
        Ok(RecordIterator::new(reader, self.file_description.clone()))
    }

    /// Gets the path to the DAT file.
    ///
    /// Returns the path: `./data/{file_id}/{file_id}.dat`
    fn get_dat_file_path(&self) -> PathBuf {
        data_directory()
            .join(&self.file_id)
            .join(format!("{}.dat", self.file_id))
    }

    /// Parses a single line from the DAT file.
    ///
    /// Extracts all fields defined in the schema and coerces their values
    /// according to the coercion rules.
    ///
    /// # Arguments
    ///
    /// * `line` - A line from the DAT file
    ///
    /// # Returns
    ///
    /// A HashMap mapping field codes to their coerced values.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ncdac_opi_parser::parser::DataParser;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let parser = DataParser::new("OFNT1BA1")?;
    /// let line = "1234567AB123..."; // Fixed-width record
    /// let record = parser.parse_line(line);
    /// # Ok(())
    /// # }
    /// ```
    pub fn parse_line(&self, line: &str) -> HashMap<String, Option<String>> {
        let mut record = HashMap::new();

        for (field_code, field_def) in &self.file_description.schema {
            let slice_start = field_def.start.saturating_sub(1);
            let slice_end = slice_start + field_def.length;

            let raw_value = if line.len() >= slice_end {
                &line[slice_start..slice_end]
            } else if line.len() > slice_start {
                &line[slice_start..]
            } else {
                ""
            };

            let coerced_value = Self::coerce_value(raw_value);
            record.insert(field_code.clone(), coerced_value);
        }

        record
    }

    /// Coerces a raw field value according to the data rules.
    ///
    /// # Coercion Rules
    ///
    /// 1. Trim whitespace from both ends
    /// 2. Empty strings → None
    /// 3. "0001-01-01" → None (null date marker)
    /// 4. Strings of only "?" → None (null markers like "???")
    /// 5. All other values → Some(value)
    ///
    /// # Arguments
    ///
    /// * `raw_value` - The raw field value extracted from the fixed-width record
    ///
    /// # Returns
    ///
    /// `Some(String)` for valid values, `None` for null markers.
    ///
    /// # Example
    ///
    /// ```
    /// use ncdac_opi_parser::parser::DataParser;
    ///
    /// assert_eq!(DataParser::coerce_value("  123  "), Some("123".to_string()));
    /// assert_eq!(DataParser::coerce_value(""), None);
    /// assert_eq!(DataParser::coerce_value("   "), None);
    /// assert_eq!(DataParser::coerce_value("0001-01-01"), None);
    /// assert_eq!(DataParser::coerce_value("???"), None);
    /// assert_eq!(DataParser::coerce_value("valid"), Some("valid".to_string()));
    /// ```
    pub fn coerce_value(raw_value: &str) -> Option<String> {
        let value = raw_value.trim();

        if value.is_empty() {
            return None;
        }

        if value == NULL_DATE_MARKER {
            return None;
        }

        if ALL_QUESTION_MARKS.is_match(value) {
            return None;
        }

        Some(value.to_string())
    }
}

/// Iterator over records in a DAT file.
///
/// This iterator reads lines from a buffered reader and parses each line
/// into a record using the provided schema. It automatically skips empty lines.
///
/// The iterator yields `Result<HashMap<String, Option<String>>>` where:
/// - The `HashMap` keys are field codes from the schema
/// - The values are `Option<String>` (None for null/missing values)
/// - The `Result` captures any I/O errors during reading
///
/// # Type Parameters
///
/// * `R` - A type that implements `BufRead` (typically `BufReader<File>`)
pub struct RecordIterator<R: BufRead> {
    lines: Lines<R>,
    file_description: FileDescription,
}

impl<R: BufRead> RecordIterator<R> {
    /// Creates a new `RecordIterator`.
    ///
    /// # Arguments
    ///
    /// * `reader` - A buffered reader for the DAT file
    /// * `file_description` - The schema definition for parsing records
    pub fn new(reader: R, file_description: FileDescription) -> Self {
        Self {
            lines: reader.lines(),
            file_description,
        }
    }
}

impl<R: BufRead> Iterator for RecordIterator<R> {
    type Item = Result<HashMap<String, Option<String>>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.lines.next() {
                Some(Ok(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }

                    let record = self.parse_line(&line);
                    return Some(Ok(record));
                }
                Some(Err(e)) => {
                    return Some(Err(e.into()));
                }
                None => {
                    return None;
                }
            }
        }
    }
}

impl<R: BufRead> RecordIterator<R> {
    /// Parses a single line into a record.
    ///
    /// This is an internal helper that extracts all fields according to the schema.
    fn parse_line(&self, line: &str) -> HashMap<String, Option<String>> {
        let mut record = HashMap::new();

        for (field_code, field_def) in &self.file_description.schema {
            let slice_start = field_def.start.saturating_sub(1);
            let slice_end = slice_start + field_def.length;

            let raw_value = if line.len() >= slice_end {
                &line[slice_start..slice_end]
            } else if line.len() > slice_start {
                &line[slice_start..]
            } else {
                ""
            };

            let coerced_value = DataParser::coerce_value(raw_value);
            record.insert(field_code.clone(), coerced_value);
        }

        record
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn create_test_schema() -> FileDescription {
        let content = r#"CMDORNUM      OFFENDER NC DOC ID NUMBER          CHAR      1       7
CPPREFIX      COP COMMITMENT PREFIX              CHAR      8       2
CPPAYSEQ      COP ACCOUNT SEQUENCE NUMBER        CHAR      10      3
DTOFUPDT      DATE OF LAST UPDATE                DATE      13      10
NOTES         ADDITIONAL NOTES                   CHAR      23      10"#;

        let schema = FileDescription::parse_content(content).unwrap();
        FileDescription {
            filename: "TEST".to_string(),
            schema,
        }
    }

    #[test]
    fn test_coerce_value_basic() {
        assert_eq!(
            DataParser::coerce_value("  hello  "),
            Some("hello".to_string())
        );

        assert_eq!(
            DataParser::coerce_value("world"),
            Some("world".to_string())
        );
    }

    #[test]
    fn test_coerce_value_empty() {
        assert_eq!(DataParser::coerce_value(""), None);
        assert_eq!(DataParser::coerce_value("   "), None);
        assert_eq!(DataParser::coerce_value("\t"), None);
    }

    #[test]
    fn test_coerce_value_null_date() {
        assert_eq!(DataParser::coerce_value("0001-01-01"), None);
        assert_eq!(DataParser::coerce_value("  0001-01-01  "), None);
        assert_eq!(
            DataParser::coerce_value("2023-12-25"),
            Some("2023-12-25".to_string())
        );
    }

    #[test]
    fn test_coerce_value_question_marks() {
        assert_eq!(DataParser::coerce_value("?"), None);
        assert_eq!(DataParser::coerce_value("???"), None);
        assert_eq!(DataParser::coerce_value("?????"), None);
        assert_eq!(DataParser::coerce_value("  ???  "), None);
        assert_eq!(
            DataParser::coerce_value("what?"),
            Some("what?".to_string())
        );
        assert_eq!(
            DataParser::coerce_value("a?b"),
            Some("a?b".to_string())
        );
    }

    #[test]
    fn test_parse_line_basic() {
        let file_desc = create_test_schema();
        let parser = DataParser {
            file_id: "TEST".to_string(),
            file_description: file_desc,
        };

        let line = "1234567AB123more data here";
        let record = parser.parse_line(line);

        assert_eq!(record.get("CMDORNUM"), Some(&Some("1234567".to_string())));
        assert_eq!(record.get("CPPREFIX"), Some(&Some("AB".to_string())));
        assert_eq!(record.get("CPPAYSEQ"), Some(&Some("123".to_string())));
    }

    #[test]
    fn test_parse_line_with_whitespace() {
        let file_desc = create_test_schema();
        let parser = DataParser {
            file_id: "TEST".to_string(),
            file_description: file_desc,
        };

        let line = "123    AB 001       ";
        let record = parser.parse_line(line);

        assert_eq!(record.get("CMDORNUM"), Some(&Some("123".to_string())));
        assert_eq!(record.get("CPPREFIX"), Some(&Some("AB".to_string())));
        assert_eq!(record.get("CPPAYSEQ"), Some(&Some("00".to_string())));
    }

    #[test]
    fn test_parse_line_with_null_markers() {
        let file_desc = create_test_schema();
        let parser = DataParser {
            file_id: "TEST".to_string(),
            file_description: file_desc,
        };

        let line = "1234567AB1230001-01-01???       ";
        let record = parser.parse_line(line);

        assert_eq!(record.get("CMDORNUM"), Some(&Some("1234567".to_string())));
        assert_eq!(record.get("CPPREFIX"), Some(&Some("AB".to_string())));
        assert_eq!(record.get("CPPAYSEQ"), Some(&Some("123".to_string())));
        assert_eq!(record.get("DTOFUPDT"), Some(&None));
        assert_eq!(record.get("NOTES"), Some(&None));
    }

    #[test]
    fn test_parse_line_short_line() {
        let file_desc = create_test_schema();
        let parser = DataParser {
            file_id: "TEST".to_string(),
            file_description: file_desc,
        };

        let line = "123";
        let record = parser.parse_line(line);

        assert_eq!(record.get("CMDORNUM"), Some(&Some("123".to_string())));
        assert_eq!(record.get("CPPREFIX"), Some(&None));
        assert_eq!(record.get("CPPAYSEQ"), Some(&None));
    }

    #[test]
    fn test_record_iterator_basic() {
        let file_desc = create_test_schema();
        let data = "1234567AB123\n7654321CD456\n";
        let cursor = Cursor::new(data);
        let reader = BufReader::new(cursor);

        let mut iterator = RecordIterator::new(reader, file_desc);

        let record1 = iterator.next().unwrap().unwrap();
        assert_eq!(record1.get("CMDORNUM"), Some(&Some("1234567".to_string())));
        assert_eq!(record1.get("CPPREFIX"), Some(&Some("AB".to_string())));

        let record2 = iterator.next().unwrap().unwrap();
        assert_eq!(record2.get("CMDORNUM"), Some(&Some("7654321".to_string())));
        assert_eq!(record2.get("CPPREFIX"), Some(&Some("CD".to_string())));

        assert!(iterator.next().is_none());
    }

    #[test]
    fn test_record_iterator_skips_empty_lines() {
        let file_desc = create_test_schema();
        let data = "1234567AB123\n\n\n7654321CD456\n   \n";
        let cursor = Cursor::new(data);
        let reader = BufReader::new(cursor);

        let mut iterator = RecordIterator::new(reader, file_desc);

        let record1 = iterator.next().unwrap().unwrap();
        assert_eq!(record1.get("CMDORNUM"), Some(&Some("1234567".to_string())));

        let record2 = iterator.next().unwrap().unwrap();
        assert_eq!(record2.get("CMDORNUM"), Some(&Some("7654321".to_string())));

        assert!(iterator.next().is_none());
    }

    #[test]
    fn test_record_iterator_empty_file() {
        let file_desc = create_test_schema();
        let data = "";
        let cursor = Cursor::new(data);
        let reader = BufReader::new(cursor);

        let mut iterator = RecordIterator::new(reader, file_desc);

        assert!(iterator.next().is_none());
    }

    #[test]
    fn test_record_iterator_only_empty_lines() {
        let file_desc = create_test_schema();
        let data = "\n\n   \n\t\n";
        let cursor = Cursor::new(data);
        let reader = BufReader::new(cursor);

        let mut iterator = RecordIterator::new(reader, file_desc);

        assert!(iterator.next().is_none());
    }

    #[test]
    fn test_record_iterator_collect() {
        let file_desc = create_test_schema();
        let data = "1234567AB123\n7654321CD456\n9999999EF789\n";
        let cursor = Cursor::new(data);
        let reader = BufReader::new(cursor);

        let iterator = RecordIterator::new(reader, file_desc);
        let records: Result<Vec<_>> = iterator.collect();
        let records = records.unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(
            records[0].get("CMDORNUM"),
            Some(&Some("1234567".to_string()))
        );
        assert_eq!(
            records[1].get("CMDORNUM"),
            Some(&Some("7654321".to_string()))
        );
        assert_eq!(
            records[2].get("CMDORNUM"),
            Some(&Some("9999999".to_string()))
        );
    }

    #[test]
    fn test_data_parser_new() {
        let result = DataParser::new("NONEXISTENT_FILE_12345");
        assert!(result.is_err());
    }

    #[test]
    fn test_data_parser_accessors() {
        let file_desc = create_test_schema();
        let parser = DataParser {
            file_id: "TEST".to_string(),
            file_description: file_desc,
        };

        assert_eq!(parser.file_id(), "TEST");
        assert_eq!(parser.schema().filename, "TEST");
        assert_eq!(parser.schema().field_count(), 5);
    }
}
