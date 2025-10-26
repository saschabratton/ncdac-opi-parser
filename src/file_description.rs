use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// Represents a field definition from a DES descriptor file.
///
/// Each field has a type (CHAR, DECIMAL, DATE, TIME), a start position (1-indexed),
/// a length in characters, and a human-readable description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDefinition {
    /// The field type (e.g., "CHAR", "DECIMAL", "DATE", "TIME")
    pub field_type: String,
    /// The 1-indexed start position in the fixed-width record
    pub start: usize,
    /// The length of the field in characters
    pub length: usize,
    /// The human-readable description of the field
    pub description: String,
}

impl FieldDefinition {
    /// Creates a new FieldDefinition.
    ///
    /// # Arguments
    ///
    /// * `field_type` - The type of the field
    /// * `start` - The 1-indexed start position
    /// * `length` - The length of the field
    /// * `description` - The human-readable description of the field
    pub fn new(field_type: String, start: usize, length: usize, description: String) -> Self {
        Self {
            field_type,
            start,
            length,
            description,
        }
    }

    /// Returns the end position (inclusive) of this field.
    pub fn end(&self) -> usize {
        self.start + self.length - 1
    }

    /// Returns the 0-indexed start position for use in string slicing.
    pub fn zero_indexed_start(&self) -> usize {
        self.start.saturating_sub(1)
    }
}

/// Parses and holds the schema definition for a DES descriptor file.
///
/// The schema maps field codes to their definitions, parsed from a `.des` file
/// located in the data directory structure.
///
/// # Example DES File Format
///
/// ```text
/// CMDORNUM      OFFENDER NC DOC ID NUMBER          CHAR      1       7
/// CPPREFIX      COP COMMITMENT PREFIX              CHAR      8       2
/// CPPAYSEQ      COP ACCOUNT SEQUENCE NUMBER        CHAR      10      3
/// ```
#[derive(Debug, Clone)]
pub struct FileDescription {
    /// The filename (without extension) of the descriptor
    pub filename: String,
    /// Maps field codes to their definitions
    pub schema: HashMap<String, FieldDefinition>,
}

/// Regex pattern for parsing DES file lines.
///
/// Pattern breakdown:
/// - `^(\S+)` - Field code (non-whitespace characters at start)
/// - `\s{2,}` - At least 2 spaces separator
/// - `(.+?)` - Description (non-greedy, captured but not used)
/// - `\s{2,}` - At least 2 spaces separator
/// - `([A-Z]+)` - Type (uppercase letters: CHAR, DECIMAL, DATE, TIME, etc.)
/// - `\s+` - One or more spaces
/// - `(\d+)` - Start position (digits)
/// - `\s+` - One or more spaces
/// - `(\d+)` - Length (digits)
static DES_LINE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(\S+)\s{2,}(.+?)\s{2,}([A-Z]+)\s+(\d+)\s+(\d+)")
        .expect("Invalid DES line regex pattern")
});

impl FileDescription {
    /// Creates a new FileDescription by parsing the corresponding DES file.
    ///
    /// The DES file is expected to be located at:
    /// `./data/{filename}/{filename}.des` (relative to the current working directory)
    ///
    /// Note: The data files are extracted from ZIP archives into subdirectories.
    /// For example, OFNT1BA1.zip is extracted to data/OFNT1BA1/, and the
    /// descriptor file is at data/OFNT1BA1/OFNT1BA1.des.
    ///
    /// # Arguments
    ///
    /// * `filename` - The base filename (e.g., "OFNT1BA1")
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The DES file cannot be read
    /// - The file parsing fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ncdac_opi_parser::FileDescription;
    ///
    /// let desc = FileDescription::new("OFNT1BA1")?;
    /// if let Some(field) = desc.schema.get("CMDORNUM") {
    ///     println!("Field type: {}", field.field_type);
    /// }
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn new(filename: &str) -> Result<Self> {
        let schema = Self::parse(filename)?;
        Ok(Self {
            filename: filename.to_string(),
            schema,
        })
    }

    /// Gets the data directory path.
    ///
    /// Returns the path to the data directory, which is `./data` relative
    /// to the project directory.
    fn get_data_directory() -> PathBuf {
        // Use the utilities module's data_directory function for consistency
        crate::utilities::data_directory()
    }

    /// Parses a DES descriptor file and returns the schema.
    ///
    /// # Arguments
    ///
    /// * `filename` - The base filename (without extension or path)
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed.
    fn parse(filename: &str) -> Result<HashMap<String, FieldDefinition>> {
        let data_dir = Self::get_data_directory();
        let descriptor_path = data_dir.join(filename).join(format!("{filename}.des"));

        let descriptor = fs::read_to_string(&descriptor_path).with_context(|| {
            format!(
                "Failed to read DES file: {}",
                descriptor_path.display()
            )
        })?;

        Self::parse_content(&descriptor)
    }

    /// Parses the content of a DES descriptor file.
    ///
    /// This method is separate from `parse` to allow for easier testing.
    ///
    /// # Arguments
    ///
    /// * `content` - The content of the DES file
    ///
    /// # Returns
    ///
    /// A HashMap mapping field codes to their definitions.
    pub fn parse_content(content: &str) -> Result<HashMap<String, FieldDefinition>> {
        let mut schema = HashMap::new();

        for raw_line in content.lines() {
            // Trim trailing whitespace but preserve leading structure
            let line = raw_line.trim_end();

            // Skip empty lines
            if line.trim().is_empty() {
                continue;
            }

            // Try to match the DES line pattern
            if let Some(captures) = DES_LINE_REGEX.captures(line) {
                let field_code = captures.get(1)
                    .expect("Field code capture group")
                    .as_str()
                    .to_string();

                let description = captures.get(2)
                    .expect("Description capture group")
                    .as_str()
                    .trim()
                    .to_string();

                let field_type = captures.get(3)
                    .expect("Field type capture group")
                    .as_str()
                    .trim()
                    .to_string();

                let start: usize = captures.get(4)
                    .expect("Start position capture group")
                    .as_str()
                    .parse()
                    .with_context(|| {
                        format!("Failed to parse start position for field {field_code}")
                    })?;

                let length: usize = captures.get(5)
                    .expect("Length capture group")
                    .as_str()
                    .parse()
                    .with_context(|| {
                        format!("Failed to parse length for field {field_code}")
                    })?;

                schema.insert(
                    field_code,
                    FieldDefinition::new(field_type, start, length, description),
                );
            }
        }

        Ok(schema)
    }

    /// Gets a field definition by field code.
    ///
    /// # Arguments
    ///
    /// * `field_code` - The field code to look up
    ///
    /// # Returns
    ///
    /// An Option containing a reference to the FieldDefinition if found.
    pub fn get_field(&self, field_code: &str) -> Option<&FieldDefinition> {
        self.schema.get(field_code)
    }

    /// Returns the number of fields in the schema.
    pub fn field_count(&self) -> usize {
        self.schema.len()
    }

    /// Returns an iterator over all field codes in the schema.
    pub fn field_codes(&self) -> impl Iterator<Item = &String> {
        self.schema.keys()
    }

    /// Extracts a field value from a fixed-width record line.
    ///
    /// # Arguments
    ///
    /// * `field_code` - The field code to extract
    /// * `record` - The fixed-width record line
    ///
    /// # Returns
    ///
    /// An Option containing the trimmed field value, or None if the field doesn't exist
    /// or if the record is too short.
    pub fn extract_field<'a>(&self, field_code: &str, record: &'a str) -> Option<&'a str> {
        let field_def = self.schema.get(field_code)?;
        let start = field_def.zero_indexed_start();
        let end = start + field_def.length;

        if record.len() < end {
            return None;
        }

        Some(record[start..end].trim())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_definition_basic() {
        let field = FieldDefinition::new("CHAR".to_string(), 1, 7, "Test description".to_string());
        assert_eq!(field.field_type, "CHAR");
        assert_eq!(field.start, 1);
        assert_eq!(field.length, 7);
        assert_eq!(field.description, "Test description");
        assert_eq!(field.end(), 7);
        assert_eq!(field.zero_indexed_start(), 0);
    }

    #[test]
    fn test_field_definition_end_calculation() {
        let field = FieldDefinition::new("CHAR".to_string(), 8, 2, "Another description".to_string());
        assert_eq!(field.end(), 9);
        assert_eq!(field.zero_indexed_start(), 7);
    }

    #[test]
    fn test_parse_content_basic() {
        let content = r#"CMDORNUM      OFFENDER NC DOC ID NUMBER          CHAR      1       7
CPPREFIX      COP COMMITMENT PREFIX              CHAR      8       2
CPPAYSEQ      COP ACCOUNT SEQUENCE NUMBER        CHAR      10      3     "#;

        let schema = FileDescription::parse_content(content).unwrap();

        assert_eq!(schema.len(), 3);

        let cmdornum = schema.get("CMDORNUM").unwrap();
        assert_eq!(cmdornum.field_type, "CHAR");
        assert_eq!(cmdornum.start, 1);
        assert_eq!(cmdornum.length, 7);
        assert_eq!(cmdornum.description, "OFFENDER NC DOC ID NUMBER");

        let cpprefix = schema.get("CPPREFIX").unwrap();
        assert_eq!(cpprefix.field_type, "CHAR");
        assert_eq!(cpprefix.start, 8);
        assert_eq!(cpprefix.length, 2);
        assert_eq!(cpprefix.description, "COP COMMITMENT PREFIX");
    }

    #[test]
    fn test_parse_content_with_different_types() {
        let content = r#"CMDORNUM      OFFENDER NC DOC ID NUMBER          CHAR      1       7
CPCOPBAL      COP BALANCE                        DECIMAL   171     11
DTOFUPDT      DATE OF LAST UPDATE                DATE      222     10
TMOFUPDT      TIME OF LAST UPDATE                TIME      232     8     "#;

        let schema = FileDescription::parse_content(content).unwrap();

        assert_eq!(schema.len(), 4);
        assert_eq!(schema.get("CMDORNUM").unwrap().field_type, "CHAR");
        assert_eq!(schema.get("CPCOPBAL").unwrap().field_type, "DECIMAL");
        assert_eq!(schema.get("DTOFUPDT").unwrap().field_type, "DATE");
        assert_eq!(schema.get("TMOFUPDT").unwrap().field_type, "TIME");
    }

    #[test]
    fn test_parse_content_skips_empty_lines() {
        let content = r#"
CMDORNUM      OFFENDER NC DOC ID NUMBER          CHAR      1       7

CPPREFIX      COP COMMITMENT PREFIX              CHAR      8       2

"#;

        let schema = FileDescription::parse_content(content).unwrap();
        assert_eq!(schema.len(), 2);
    }

    #[test]
    fn test_parse_content_skips_invalid_lines() {
        let content = r#"CMDORNUM      OFFENDER NC DOC ID NUMBER          CHAR      1       7
This is not a valid line
CPPREFIX      COP COMMITMENT PREFIX              CHAR      8       2
Another invalid line without proper format
"#;

        let schema = FileDescription::parse_content(content).unwrap();
        assert_eq!(schema.len(), 2);
    }

    #[test]
    fn test_get_field() {
        let content = r#"CMDORNUM      OFFENDER NC DOC ID NUMBER          CHAR      1       7
CPPREFIX      COP COMMITMENT PREFIX              CHAR      8       2     "#;

        let desc = FileDescription {
            filename: "test".to_string(),
            schema: FileDescription::parse_content(content).unwrap(),
        };

        assert!(desc.get_field("CMDORNUM").is_some());
        assert!(desc.get_field("CPPREFIX").is_some());
        assert!(desc.get_field("NONEXISTENT").is_none());
    }

    #[test]
    fn test_field_count() {
        let content = r#"CMDORNUM      OFFENDER NC DOC ID NUMBER          CHAR      1       7
CPPREFIX      COP COMMITMENT PREFIX              CHAR      8       2
CPPAYSEQ      COP ACCOUNT SEQUENCE NUMBER        CHAR      10      3     "#;

        let desc = FileDescription {
            filename: "test".to_string(),
            schema: FileDescription::parse_content(content).unwrap(),
        };

        assert_eq!(desc.field_count(), 3);
    }

    #[test]
    fn test_extract_field() {
        let content = r#"CMDORNUM      OFFENDER NC DOC ID NUMBER          CHAR      1       7
CPPREFIX      COP COMMITMENT PREFIX              CHAR      8       2
CPPAYSEQ      COP ACCOUNT SEQUENCE NUMBER        CHAR      10      3     "#;

        let desc = FileDescription {
            filename: "test".to_string(),
            schema: FileDescription::parse_content(content).unwrap(),
        };

        let record = "1234567AB123more data here";

        assert_eq!(desc.extract_field("CMDORNUM", record), Some("1234567"));
        assert_eq!(desc.extract_field("CPPREFIX", record), Some("AB"));
        assert_eq!(desc.extract_field("CPPAYSEQ", record), Some("123"));
    }

    #[test]
    fn test_extract_field_with_spaces() {
        let content = r#"CMDORNUM      OFFENDER NC DOC ID NUMBER          CHAR      1       7     "#;

        let desc = FileDescription {
            filename: "test".to_string(),
            schema: FileDescription::parse_content(content).unwrap(),
        };

        // Field value with trailing spaces
        let record = "123    more data";
        assert_eq!(desc.extract_field("CMDORNUM", record), Some("123"));
    }

    #[test]
    fn test_extract_field_record_too_short() {
        let content = r#"CMDORNUM      OFFENDER NC DOC ID NUMBER          CHAR      1       7     "#;

        let desc = FileDescription {
            filename: "test".to_string(),
            schema: FileDescription::parse_content(content).unwrap(),
        };

        let record = "123"; // Too short for the field
        assert_eq!(desc.extract_field("CMDORNUM", record), None);
    }

    #[test]
    fn test_extract_field_nonexistent() {
        let content = r#"CMDORNUM      OFFENDER NC DOC ID NUMBER          CHAR      1       7     "#;

        let desc = FileDescription {
            filename: "test".to_string(),
            schema: FileDescription::parse_content(content).unwrap(),
        };

        let record = "1234567";
        assert_eq!(desc.extract_field("NONEXISTENT", record), None);
    }
}
