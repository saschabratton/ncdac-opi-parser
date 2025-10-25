//! SQLite database handler for NC DAC OPI data.
//!
//! The `DataHandler` follows a reference table pattern where:
//! 1. One file is designated as the reference (primary key source)
//! 2. The reference table has a PRIMARY KEY constraint
//! 3. All other tables have FOREIGN KEY constraints referencing the reference table
//! 4. Foreign key violations are collected but don't stop processing
//!
//! # Example
//!
//! ```no_run
//! use ncdac_opi_parser::data_handler::DataHandler;
//! use ncdac_opi_parser::files::get_file_by_id;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a database handler
//! let mut handler = DataHandler::new("database.db")?;
//!
//! // Initialize with the reference file (Offender Profile)
//! let reference_file = get_file_by_id("OFNT3AA1").unwrap();
//! let results = handler.init(reference_file, None)?;
//! println!("Processed {} records", results.processed);
//!
//! // Process additional files
//! let other_file = get_file_by_id("OFNT1BA1").unwrap();
//! if let Some(results) = handler.process_file(other_file, None)? {
//!     println!("Processed {} records with {} errors",
//!              results.processed, results.errors.len());
//! }
//! # Ok(())
//! # }
//! ```

use crate::file_description::FileDescription;
use crate::files::FileMetadata;
use crate::parser::DataParser;
use crate::utilities::{get_primary_key_field, to_snake_case};
use anyhow::{anyhow, Context, Result};
use indicatif::ProgressBar;
use rusqlite::Connection;
use std::collections::HashSet;

/// The batch size for transaction commits.
///
/// Records are inserted in batches of this size to balance performance
/// and memory usage. Through benchmarking, 250 was found to be optimal,
/// providing 14% faster performance than 1000 (see BATCH_SIZE_OPTIMIZATION.md).
const BATCH_SIZE: usize = 250;

/// SQLite extended result code for foreign key constraint violations.
///
/// Used to detect when inserted rows reference missing parent records so we
/// can surface a detailed error message to the caller.
const FOREIGN_KEY_ERROR_CODE: i32 = 787;

/// Details about a processing error.
///
/// This struct captures information about errors that occur during processing,
/// particularly foreign key constraint violations.
#[derive(Debug, Clone)]
pub struct ErrorDetails {
    /// The file ID where the error occurred
    pub file_id: String,
    /// The table name where the error occurred
    pub table_name: String,
    /// Human-readable error message
    pub message: String,
    /// The underlying error message from SQLite
    pub error_message: String,
}

impl ErrorDetails {
    /// Creates a new ErrorDetails instance.
    pub fn new(
        file_id: String,
        table_name: String,
        message: String,
        error_message: String,
    ) -> Self {
        Self {
            file_id,
            table_name,
            message,
            error_message,
        }
    }
}

/// Results from processing a file.
///
/// Contains the number of records processed and any errors encountered.
#[derive(Debug, Clone)]
pub struct ProcessingResults {
    /// Number of records successfully processed
    pub processed: usize,
    /// Errors encountered during processing (typically foreign key violations)
    pub errors: Vec<ErrorDetails>,
}

impl ProcessingResults {
    /// Creates a new ProcessingResults instance.
    pub fn new(processed: usize, errors: Vec<ErrorDetails>) -> Self {
        Self { processed, errors }
    }
}

/// Handler for SQLite database operations on NC DAC OPI data.
///
/// The `DataHandler` manages database schema creation, data insertion,
/// and constraint enforcement for NC DAC OPI files. It uses a reference
/// table pattern where one table serves as the primary key source.
///
/// # Initialization
///
/// The handler must be initialized with a reference file before processing
/// other files. The reference file determines the primary key field that
/// all other tables will reference.
///
/// # Transaction Management
///
/// Records are inserted in batches (default 500) within transactions for
/// optimal performance. Each batch is committed independently, so partial
/// failures don't lose all work.
///
/// # Error Handling
///
/// Foreign key constraint violations are collected in the `errors` vector
/// but don't stop processing. This allows the handler to process as much
/// valid data as possible while tracking problematic records.
#[derive(Debug)]
pub struct DataHandler {
    /// SQLite database connection
    database: Connection,
    /// The reference file metadata (set during init)
    reference_file: Option<FileMetadata>,
    /// The reference table name in snake_case (set during init)
    reference_table_name: Option<String>,
    /// The primary key field name (set during init)
    reference_field: Option<String>,
    /// Whether the handler has been initialized
    is_initialized: bool,
    /// Set of file IDs that have been processed
    processed_files: HashSet<String>,
    /// Collection of all errors encountered during processing
    pub errors: Vec<ErrorDetails>,
}

impl DataHandler {
    /// Creates a new `DataHandler` with the specified database path.
    ///
    /// Opens or creates a SQLite database at the given path and enables
    /// foreign key constraint enforcement.
    ///
    /// # Arguments
    ///
    /// * `database_path` - Path to the SQLite database file
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database cannot be opened or created
    /// - Foreign key enforcement cannot be enabled
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ncdac_opi_parser::data_handler::DataHandler;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let handler = DataHandler::new("my_database.db")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(database_path: &str) -> Result<Self> {
        let database = Connection::open(database_path)
            .with_context(|| format!("Failed to open database: {}", database_path))?;

        database
            .pragma_update(None, "foreign_keys", "ON")
            .context("Failed to enable foreign key constraints")?;

        Ok(Self {
            database,
            reference_file: None,
            reference_table_name: None,
            reference_field: None,
            is_initialized: false,
            processed_files: HashSet::new(),
            errors: Vec::new(),
        })
    }

    /// Initializes the handler with a reference file.
    ///
    /// The reference file serves as the primary key source for the database.
    /// All other tables will have foreign key constraints referencing this table.
    ///
    /// This method must be called before processing any other files.
    ///
    /// # Arguments
    ///
    /// * `reference_file` - The file to use as the reference table
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The reference file's schema doesn't contain a recognized primary key field
    /// - The reference file cannot be processed
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ncdac_opi_parser::data_handler::DataHandler;
    /// use ncdac_opi_parser::files::get_file_by_id;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut handler = DataHandler::new("database.db")?;
    /// let reference = get_file_by_id("OFNT3AA1").unwrap();
    /// let results = handler.init(reference, None)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn init(&mut self, reference_file: &FileMetadata, pb: Option<&ProgressBar>) -> Result<ProcessingResults> {
        let reference_table_name = to_snake_case(reference_file.name);
        let reference_description = FileDescription::new(reference_file.id)?;

        let reference_field = get_primary_key_field(&reference_description.schema)
            .ok_or_else(|| {
                anyhow!(
                    "Reference table {} does not contain an expected key field",
                    reference_table_name
                )
            })?;

        self.reference_file = Some(*reference_file);
        self.reference_table_name = Some(reference_table_name);
        self.reference_field = Some(reference_field.to_string());
        self.is_initialized = true;

        let results = self.process_file(reference_file, pb)?;

        results.ok_or_else(|| anyhow!("Failed to process reference file"))
    }

    /// Creates a table for the specified file.
    ///
    /// Generates a CREATE TABLE statement from the file's DES schema and executes it.
    /// Maps field types to SQLite types (DECIMAL → REAL, everything else → TEXT).
    ///
    /// If the table is the reference table, adds a PRIMARY KEY constraint.
    /// Otherwise, adds a FOREIGN KEY constraint referencing the reference table.
    ///
    /// # Arguments
    ///
    /// * `file` - The file metadata for which to create a table
    ///
    /// # Returns
    ///
    /// The table name (in snake_case) that was created.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file's schema doesn't contain a recognized primary key field
    /// - The table creation SQL fails to execute
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ncdac_opi_parser::data_handler::DataHandler;
    /// use ncdac_opi_parser::files::get_file_by_id;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let handler = DataHandler::new("database.db")?;
    /// let file = get_file_by_id("OFNT1BA1").unwrap();
    /// // Note: handler must be initialized first in real usage
    /// // let table_name = handler.create_table_for_file(file)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_table_for_file(&self, file: &FileMetadata) -> Result<String> {
        let table_name = to_snake_case(file.name);
        let description = FileDescription::new(file.id)?;

        let primary_key = get_primary_key_field(&description.schema).ok_or_else(|| {
            anyhow!(
                "Table {} does not contain an expected key field",
                table_name
            )
        })?;

        let columns: Vec<String> = description
            .schema
            .iter()
            .map(|(field, definition)| {
                let column_type = map_type_to_sqlite(&definition.field_type);
                format!("{} {}", field, column_type)
            })
            .collect();

        let mut constraints = Vec::new();

        if Some(&table_name) == self.reference_table_name.as_ref() {
            constraints.push(format!("PRIMARY KEY ({})", primary_key));
        } else {
            let reference_table = self.reference_table_name.as_ref().ok_or_else(|| {
                anyhow!("Cannot create table: handler not initialized with reference table")
            })?;
            let reference_field = self.reference_field.as_ref().ok_or_else(|| {
                anyhow!("Cannot create table: reference field not set")
            })?;

            constraints.push(format!(
                "FOREIGN KEY ({}) REFERENCES {}({})",
                primary_key, reference_table, reference_field
            ));
        }

        let mut sql_parts = columns;
        sql_parts.extend(constraints);
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            table_name,
            sql_parts.join(", ")
        );

        self.database
            .execute(&sql, [])
            .with_context(|| format!("Failed to create table {}", table_name))?;

        Ok(table_name)
    }

    /// Inserts records from a file into its table.
    ///
    /// Parses the file's DAT records and inserts them in batches within transactions.
    /// Foreign key constraint violations are collected but don't stop processing.
    ///
    /// # Arguments
    ///
    /// * `file` - The file metadata for which to insert records
    ///
    /// # Returns
    ///
    /// A `ProcessingResults` with the count of processed records and any errors.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table doesn't exist (call `create_table_for_file` first)
    /// - A non-foreign-key database error occurs
    /// - The data parser encounters an error
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ncdac_opi_parser::data_handler::DataHandler;
    /// use ncdac_opi_parser::files::get_file_by_id;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut handler = DataHandler::new("database.db")?;
    /// let file = get_file_by_id("OFNT1BA1").unwrap();
    /// // Note: table must exist first in real usage
    /// // let results = handler.insert_records_for_file(file)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_records_for_file(&mut self, file: &FileMetadata, pb: Option<&ProgressBar>) -> Result<ProcessingResults> {
        let table_name = to_snake_case(file.name);
        let description = FileDescription::new(file.id)?;
        let parser = DataParser::new(file.id)?;

        let columns: Vec<String> = description.schema.keys().cloned().collect();

        let placeholders = columns.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name,
            columns.join(", "),
            placeholders
        );

        let mut processed = 0;
        let mut local_errors = Vec::new();
        let mut batch: Vec<(Vec<Option<String>>, usize)> = Vec::new();
        let mut line_number = 0;

        for record_result in parser.parse()? {
            let record = record_result?;
            line_number += 1;

            let values: Vec<Option<String>> = columns
                .iter()
                .map(|column| record.get(column).cloned().unwrap_or(None))
                .collect();

            batch.push((values, line_number));

            if batch.len() >= BATCH_SIZE {
                let batch_errors = self.commit_batch(&insert_sql, &batch, file, &table_name)?;
                local_errors.extend(batch_errors);
                processed += batch.len();

                if let Some(progress) = pb {
                    progress.inc(batch.len() as u64);
                }

                batch.clear();
            }
        }

        if !batch.is_empty() {
            let batch_errors = self.commit_batch(&insert_sql, &batch, file, &table_name)?;
            local_errors.extend(batch_errors);
            processed += batch.len();

            if let Some(progress) = pb {
                progress.inc(batch.len() as u64);
            }
        }

        self.errors.extend(local_errors.clone());

        Ok(ProcessingResults::new(processed, local_errors))
    }

    /// Commits a batch of records within a transaction.
    ///
    /// This is an internal helper that executes a batch of INSERT statements
    /// within a single transaction. Foreign key violations are caught and
    /// collected without stopping the transaction.
    ///
    /// # Arguments
    ///
    /// * `insert_sql` - The prepared INSERT statement
    /// * `batch` - The batch of records to insert (values and line numbers)
    /// * `file` - The file metadata for error reporting
    /// * `table_name` - The table name for error reporting
    ///
    /// # Returns
    ///
    /// A vector of errors encountered during the batch commit.
    ///
    /// # Errors
    ///
    /// Returns an error if a non-foreign-key database error occurs.
    fn commit_batch(
        &mut self,
        insert_sql: &str,
        batch: &[(Vec<Option<String>>, usize)],
        file: &FileMetadata,
        table_name: &str,
    ) -> Result<Vec<ErrorDetails>> {
        let mut errors = Vec::new();

        let tx = self
            .database
            .transaction()
            .context("Failed to begin transaction")?;

        {
            let mut stmt = tx
                .prepare(insert_sql)
                .context("Failed to prepare INSERT statement")?;

            for (values, line_number) in batch {
                let params: Vec<rusqlite::types::Value> = values
                    .iter()
                    .map(|v| match v {
                        Some(s) => rusqlite::types::Value::Text(s.clone()),
                        None => rusqlite::types::Value::Null,
                    })
                    .collect();

                match stmt.execute(rusqlite::params_from_iter(params.iter())) {
                    Ok(_) => {}
                    Err(rusqlite::Error::SqliteFailure(err, _))
                        if err.code == rusqlite::ErrorCode::ConstraintViolation =>
                    {
                        if err.extended_code == FOREIGN_KEY_ERROR_CODE {
                            let message = format!(
                                "Foreign key violation inserting into {}\n  File: {} ({})\n  Line: {}\n  Values: {:?}",
                                table_name, file.id, file.name, line_number, values
                            );

                            let error_details = ErrorDetails::new(
                                file.id.to_string(),
                                table_name.to_string(),
                                message,
                                err.to_string(),
                            );

                            errors.push(error_details);
                            continue;
                        } else {
                            return Err(rusqlite::Error::SqliteFailure(err, None).into());
                        }
                    }
                    Err(e) => {
                        return Err(e)
                            .with_context(|| format!("Failed to insert record at line {}", line_number))?;
                    }
                }
            }
        }

        tx.commit().context("Failed to commit transaction")?;

        Ok(errors)
    }

    /// Processes a complete file (creates table and inserts records).
    ///
    /// This is the main entry point for processing a file. It:
    /// 1. Checks if the handler is initialized
    /// 2. Checks if the file has already been processed
    /// 3. Creates the table for the file
    /// 4. Inserts all records from the file
    /// 5. Marks the file as processed
    ///
    /// # Arguments
    ///
    /// * `file` - The file metadata to process
    ///
    /// # Returns
    ///
    /// - `Ok(Some(results))` if the file was processed
    /// - `Ok(None)` if the file was already processed (skipped)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The handler hasn't been initialized
    /// - Table creation fails
    /// - Record insertion fails (excluding foreign key violations)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ncdac_opi_parser::data_handler::DataHandler;
    /// use ncdac_opi_parser::files::get_file_by_id;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut handler = DataHandler::new("database.db")?;
    ///
    /// // Initialize with reference file
    /// let reference = get_file_by_id("OFNT3AA1").unwrap();
    /// handler.init(reference, None)?;
    ///
    /// // Process another file
    /// let file = get_file_by_id("OFNT1BA1").unwrap();
    /// if let Some(results) = handler.process_file(file, None)? {
    ///     println!("Processed {} records", results.processed);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn process_file(
        &mut self,
        file: &FileMetadata,
        pb: Option<&ProgressBar>,
    ) -> Result<Option<ProcessingResults>> {
        if !self.is_initialized {
            return Err(anyhow!("DataHandler is not initialized"));
        }

        if self.processed_files.contains(file.id) {
            return Ok(None);
        }

        self.create_table_for_file(file)?;
        let results = self.insert_records_for_file(file, pb)?;

        self.processed_files.insert(file.id.to_string());

        Ok(Some(results))
    }

    /// Returns whether the handler has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.is_initialized
    }

    /// Returns a reference to the reference file metadata.
    pub fn reference_file(&self) -> Option<&FileMetadata> {
        self.reference_file.as_ref()
    }

    /// Returns a reference to the reference table name.
    pub fn reference_table_name(&self) -> Option<&str> {
        self.reference_table_name.as_deref()
    }

    /// Returns a reference to the reference field name.
    pub fn reference_field(&self) -> Option<&str> {
        self.reference_field.as_deref()
    }

    /// Returns the set of processed file IDs.
    pub fn processed_files(&self) -> &HashSet<String> {
        &self.processed_files
    }
}

/// Maps a DES field type to a SQLite type.
///
/// - DECIMAL → REAL
/// - All others → TEXT
///
/// This matches the behavior of the Node.js implementation.
///
/// # Examples
///
/// ```
/// use ncdac_opi_parser::data_handler::map_type_to_sqlite;
///
/// assert_eq!(map_type_to_sqlite("DECIMAL"), "REAL");
/// assert_eq!(map_type_to_sqlite("CHAR"), "TEXT");
/// assert_eq!(map_type_to_sqlite("DATE"), "TEXT");
/// assert_eq!(map_type_to_sqlite("TIME"), "TEXT");
/// ```
pub fn map_type_to_sqlite(field_type: &str) -> &'static str {
    match field_type {
        "DECIMAL" => "REAL",
        _ => "TEXT",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_map_type_to_sqlite() {
        assert_eq!(map_type_to_sqlite("DECIMAL"), "REAL");
        assert_eq!(map_type_to_sqlite("CHAR"), "TEXT");
        assert_eq!(map_type_to_sqlite("DATE"), "TEXT");
        assert_eq!(map_type_to_sqlite("TIME"), "TEXT");
        assert_eq!(map_type_to_sqlite("UNKNOWN"), "TEXT");
    }

    #[test]
    fn test_data_handler_new() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path().to_str().unwrap();

        let handler = DataHandler::new(path)?;
        assert!(!handler.is_initialized());
        assert!(handler.reference_file().is_none());
        assert!(handler.reference_table_name().is_none());
        assert!(handler.reference_field().is_none());
        assert_eq!(handler.processed_files().len(), 0);
        assert_eq!(handler.errors.len(), 0);

        Ok(())
    }

    #[test]
    fn test_data_handler_foreign_keys_enabled() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path().to_str().unwrap();

        let handler = DataHandler::new(path)?;

        let fk_enabled: i32 = handler
            .database
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))?;

        assert_eq!(fk_enabled, 1);

        Ok(())
    }

    #[test]
    fn test_error_details() {
        let error = ErrorDetails::new(
            "OFNT1BA1".to_string(),
            "test_table".to_string(),
            "Test error message".to_string(),
            "SQLite error".to_string(),
        );

        assert_eq!(error.file_id, "OFNT1BA1");
        assert_eq!(error.table_name, "test_table");
        assert_eq!(error.message, "Test error message");
        assert_eq!(error.error_message, "SQLite error");
    }

    #[test]
    fn test_processing_results() {
        let errors = vec![ErrorDetails::new(
            "TEST".to_string(),
            "table".to_string(),
            "msg".to_string(),
            "err".to_string(),
        )];

        let results = ProcessingResults::new(100, errors.clone());

        assert_eq!(results.processed, 100);
        assert_eq!(results.errors.len(), 1);
        assert_eq!(results.errors[0].file_id, "TEST");
    }

    #[test]
    fn test_process_file_without_init() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path().to_str().unwrap();

        let mut handler = DataHandler::new(path)?;

        let file = FileMetadata::new(
            "OFNT3AA1",
            "Offender Profile",
            "https://example.com/OFNT3AA1.zip",
            None,
            None,
            None,
        );
        let result = handler.process_file(&file, None);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not initialized"));

        Ok(())
    }
}
