//! Concurrency infrastructure for parallel file processing.
//!
//! # Example
//!
//! ```no_run
//! use ncdac_opi_parser::concurrency::{ErrorAggregator, set_pragma_synchronous_normal};
//! use ncdac_opi_parser::data_handler::ErrorDetails;
//! use rusqlite::Connection;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a thread-safe error aggregator
//! let aggregator = ErrorAggregator::new();
//!
//! // Add errors from multiple threads
//! let error = ErrorDetails::new(
//!     "OFNT1BA1".to_string(),
//!     "test_table".to_string(),
//!     "Test error".to_string(),
//!     "SQLite error".to_string(),
//! );
//! aggregator.add_error(error);
//!
//! // Extract all collected errors
//! let all_errors = aggregator.get_errors();
//!
//! // Configure database connection for performance
//! let conn = Connection::open_in_memory()?;
//! set_pragma_synchronous_normal(&conn)?;
//! # Ok(())
//! # }
//! ```

use crate::data_handler::{DataHandler, ErrorDetails};
use anyhow::{Context, Result};
use rusqlite::Connection;
use std::sync::{Arc, Mutex};

/// Thread-safe error aggregator for collecting errors from concurrent operations.
///
/// This structure wraps a vector of ErrorDetails in an Arc<Mutex<T>> to allow
/// multiple threads to safely add errors during parallel processing. After all
/// threads complete, the accumulated errors can be extracted.
///
/// # Thread Safety
///
/// The ErrorAggregator uses a Mutex to ensure only one thread can modify the
/// error collection at a time. This prevents race conditions and data corruption.
///
/// # Example
///
/// ```no_run
/// use ncdac_opi_parser::concurrency::ErrorAggregator;
/// use ncdac_opi_parser::data_handler::ErrorDetails;
///
/// let aggregator = ErrorAggregator::new();
///
/// // Thread 1 adds an error
/// aggregator.add_error(ErrorDetails::new(
///     "FILE1".to_string(),
///     "table1".to_string(),
///     "Error 1".to_string(),
///     "SQLite error 1".to_string(),
/// ));
///
/// // Thread 2 adds an error
/// aggregator.add_error(ErrorDetails::new(
///     "FILE2".to_string(),
///     "table2".to_string(),
///     "Error 2".to_string(),
///     "SQLite error 2".to_string(),
/// ));
///
/// // Extract all errors
/// let all_errors = aggregator.get_errors();
/// assert_eq!(all_errors.len(), 2);
/// ```
#[derive(Debug, Clone)]
pub struct ErrorAggregator {
    errors: Arc<Mutex<Vec<ErrorDetails>>>,
}

impl ErrorAggregator {
    /// Creates a new empty ErrorAggregator.
    ///
    /// # Example
    ///
    /// ```
    /// use ncdac_opi_parser::concurrency::ErrorAggregator;
    ///
    /// let aggregator = ErrorAggregator::new();
    /// assert_eq!(aggregator.get_errors().len(), 0);
    /// ```
    pub fn new() -> Self {
        Self {
            errors: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Adds an error to the aggregator in a thread-safe manner.
    ///
    /// This method acquires a lock on the internal error vector, adds the error,
    /// and releases the lock. If another thread holds the lock, this method will
    /// block until the lock is available.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned (i.e., another thread panicked while holding the lock).
    /// This is intentional as it indicates a serious threading issue that should not be ignored.
    ///
    /// # Arguments
    ///
    /// * `error` - The ErrorDetails to add to the collection
    ///
    /// # Example
    ///
    /// ```
    /// use ncdac_opi_parser::concurrency::ErrorAggregator;
    /// use ncdac_opi_parser::data_handler::ErrorDetails;
    ///
    /// let aggregator = ErrorAggregator::new();
    /// let error = ErrorDetails::new(
    ///     "TEST".to_string(),
    ///     "table".to_string(),
    ///     "msg".to_string(),
    ///     "err".to_string(),
    /// );
    /// aggregator.add_error(error);
    /// assert_eq!(aggregator.get_errors().len(), 1);
    /// ```
    pub fn add_error(&self, error: ErrorDetails) {
        self.errors
            .lock()
            .expect("Error aggregator mutex poisoned")
            .push(error);
    }

    /// Adds multiple errors to the aggregator in a thread-safe manner.
    ///
    /// This is more efficient than calling add_error repeatedly as it only
    /// acquires the lock once.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    ///
    /// # Arguments
    ///
    /// * `errors` - A vector of ErrorDetails to add to the collection
    ///
    /// # Example
    ///
    /// ```
    /// use ncdac_opi_parser::concurrency::ErrorAggregator;
    /// use ncdac_opi_parser::data_handler::ErrorDetails;
    ///
    /// let aggregator = ErrorAggregator::new();
    /// let errors = vec![
    ///     ErrorDetails::new("TEST1".to_string(), "t1".to_string(), "m1".to_string(), "e1".to_string()),
    ///     ErrorDetails::new("TEST2".to_string(), "t2".to_string(), "m2".to_string(), "e2".to_string()),
    /// ];
    /// aggregator.add_errors(errors);
    /// assert_eq!(aggregator.get_errors().len(), 2);
    /// ```
    pub fn add_errors(&self, errors: Vec<ErrorDetails>) {
        self.errors
            .lock()
            .expect("Error aggregator mutex poisoned")
            .extend(errors);
    }

    /// Extracts all collected errors from the aggregator.
    ///
    /// This method returns a cloned copy of all errors, leaving the internal
    /// collection intact. This allows multiple calls to get_errors() if needed.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    ///
    /// # Returns
    ///
    /// A vector containing clones of all collected ErrorDetails.
    ///
    /// # Example
    ///
    /// ```
    /// use ncdac_opi_parser::concurrency::ErrorAggregator;
    /// use ncdac_opi_parser::data_handler::ErrorDetails;
    ///
    /// let aggregator = ErrorAggregator::new();
    /// aggregator.add_error(ErrorDetails::new(
    ///     "TEST".to_string(),
    ///     "table".to_string(),
    ///     "msg".to_string(),
    ///     "err".to_string(),
    /// ));
    ///
    /// let errors = aggregator.get_errors();
    /// assert_eq!(errors.len(), 1);
    /// assert_eq!(errors[0].file_id, "TEST");
    /// ```
    pub fn get_errors(&self) -> Vec<ErrorDetails> {
        self.errors
            .lock()
            .expect("Error aggregator mutex poisoned")
            .clone()
    }

    /// Returns the count of errors currently in the aggregator.
    ///
    /// This is more efficient than calling get_errors().len() as it doesn't
    /// clone the entire vector.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    ///
    /// # Example
    ///
    /// ```
    /// use ncdac_opi_parser::concurrency::ErrorAggregator;
    /// use ncdac_opi_parser::data_handler::ErrorDetails;
    ///
    /// let aggregator = ErrorAggregator::new();
    /// assert_eq!(aggregator.count(), 0);
    ///
    /// aggregator.add_error(ErrorDetails::new(
    ///     "TEST".to_string(),
    ///     "table".to_string(),
    ///     "msg".to_string(),
    ///     "err".to_string(),
    /// ));
    /// assert_eq!(aggregator.count(), 1);
    /// ```
    pub fn count(&self) -> usize {
        self.errors
            .lock()
            .expect("Error aggregator mutex poisoned")
            .len()
    }
}

impl Default for ErrorAggregator {
    fn default() -> Self {
        Self::new()
    }
}

/// Creates a new DataHandler instance with a separate SQLite connection for parallel processing.
///
/// This function implements the connection-per-thread strategy required for SQLite concurrent writes.
/// Each thread in the parallel processing pool should call this function to get its own isolated
/// database connection.
///
/// **SQLite Concurrent Write Limitations:**
/// SQLite has limitations with concurrent writes from multiple connections. To work around this,
/// we use a connection-per-thread strategy where each parallel worker has its own connection.
/// This combined with PRAGMA synchronous=NORMAL provides good write performance while maintaining
/// data integrity.
///
/// **PRAGMA Configuration:**
/// - `PRAGMA foreign_keys=ON` - Enforced on all connections to maintain referential integrity
/// - `PRAGMA synchronous=NORMAL` - Applied to non-reference table connections for performance
///
/// # Arguments
///
/// * `database_path` - Path to the SQLite database file
///
/// # Errors
///
/// Returns an error if:
/// - The database cannot be opened
/// - Foreign key enforcement cannot be enabled
/// - PRAGMA synchronous cannot be set
///
/// # Example
///
/// ```no_run
/// use ncdac_opi_parser::concurrency::create_worker_handler;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Each parallel worker thread creates its own handler
/// let handler = create_worker_handler("database.db")?;
/// # Ok(())
/// # }
/// ```
pub fn create_worker_handler(database_path: &str) -> Result<DataHandler> {
    let handler = DataHandler::new(database_path)
        .with_context(|| format!("Failed to create worker DataHandler for {}", database_path))?;

    set_pragma_synchronous_normal(handler.connection())
        .context("Failed to set PRAGMA synchronous=NORMAL on worker connection")?;

    Ok(handler)
}

/// Sets SQLite PRAGMA synchronous to NORMAL for improved write performance.
///
/// This setting provides a good balance between performance and durability:
/// - Faster than FULL (the default) because it syncs less frequently
/// - Still safer than OFF as it ensures database integrity
/// - Suitable for non-reference tables where the reference table (with FULL) ensures referential integrity
///
/// **Trade-offs:**
/// - NORMAL: Syncs at critical moments. Provides good durability with better performance.
/// - FULL: Syncs at every commit. Maximum durability but slower writes.
///
/// **Recommendation:** Use NORMAL for non-reference table processing in parallel workers,
/// while keeping FULL for the reference table.
///
/// # Arguments
///
/// * `conn` - A reference to the SQLite connection to configure
///
/// # Errors
///
/// Returns an error if the PRAGMA command fails to execute.
///
/// # Example
///
/// ```no_run
/// use ncdac_opi_parser::concurrency::set_pragma_synchronous_normal;
/// use rusqlite::Connection;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let conn = Connection::open_in_memory()?;
/// set_pragma_synchronous_normal(&conn)?;
/// # Ok(())
/// # }
/// ```
pub fn set_pragma_synchronous_normal(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "synchronous", "NORMAL")
        .context("Failed to set PRAGMA synchronous=NORMAL")?;
    Ok(())
}

/// Sets SQLite PRAGMA synchronous to FULL for maximum durability.
///
/// This is the default SQLite setting and provides maximum data safety:
/// - Ensures all data is written to disk before each commit completes
/// - Protects against data corruption even in power loss scenarios
/// - Slower than NORMAL due to more frequent disk syncs
///
/// **Trade-offs:**
/// - FULL: Maximum durability, slower writes (default SQLite behavior)
/// - NORMAL: Good durability, faster writes
///
/// **Recommendation:** Use FULL for the reference table to ensure the foundation
/// of foreign key relationships is maximally durable.
///
/// # Arguments
///
/// * `conn` - A reference to the SQLite connection to configure
///
/// # Errors
///
/// Returns an error if the PRAGMA command fails to execute.
///
/// # Example
///
/// ```no_run
/// use ncdac_opi_parser::concurrency::set_pragma_synchronous_full;
/// use rusqlite::Connection;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let conn = Connection::open_in_memory()?;
/// set_pragma_synchronous_full(&conn)?;
/// # Ok(())
/// # }
/// ```
pub fn set_pragma_synchronous_full(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "synchronous", "FULL")
        .context("Failed to set PRAGMA synchronous=FULL")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_error_aggregator_new() {
        let aggregator = ErrorAggregator::new();
        assert_eq!(aggregator.count(), 0);
        assert_eq!(aggregator.get_errors().len(), 0);
    }

    #[test]
    fn test_error_aggregator_add_single_error() {
        let aggregator = ErrorAggregator::new();
        let error = ErrorDetails::new(
            "TEST".to_string(),
            "table".to_string(),
            "msg".to_string(),
            "err".to_string(),
        );

        aggregator.add_error(error);

        assert_eq!(aggregator.count(), 1);
        let errors = aggregator.get_errors();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].file_id, "TEST");
    }

    #[test]
    fn test_error_aggregator_add_multiple_errors() {
        let aggregator = ErrorAggregator::new();
        let errors = vec![
            ErrorDetails::new(
                "TEST1".to_string(),
                "t1".to_string(),
                "m1".to_string(),
                "e1".to_string(),
            ),
            ErrorDetails::new(
                "TEST2".to_string(),
                "t2".to_string(),
                "m2".to_string(),
                "e2".to_string(),
            ),
        ];

        aggregator.add_errors(errors);

        assert_eq!(aggregator.count(), 2);
        let all_errors = aggregator.get_errors();
        assert_eq!(all_errors[0].file_id, "TEST1");
        assert_eq!(all_errors[1].file_id, "TEST2");
    }

    #[test]
    fn test_error_aggregator_concurrent_access() {
        let aggregator = ErrorAggregator::new();
        let mut handles = vec![];

        for thread_id in 0..4 {
            let agg = aggregator.clone();
            let handle = thread::spawn(move || {
                for i in 0..25 {
                    let error = ErrorDetails::new(
                        format!("THREAD{}_ERROR{}", thread_id, i),
                        "table".to_string(),
                        "msg".to_string(),
                        "err".to_string(),
                    );
                    agg.add_error(error);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        assert_eq!(aggregator.count(), 100);
        let all_errors = aggregator.get_errors();
        assert_eq!(all_errors.len(), 100);
    }

    #[test]
    fn test_error_aggregator_thread_safe_error_merging() {
        let aggregator = ErrorAggregator::new();
        let mut handles = vec![];

        for thread_id in 0..3 {
            let agg = aggregator.clone();
            let handle = thread::spawn(move || {
                let batch = vec![
                    ErrorDetails::new(
                        format!("T{}_E1", thread_id),
                        "t".to_string(),
                        "m".to_string(),
                        "e".to_string(),
                    ),
                    ErrorDetails::new(
                        format!("T{}_E2", thread_id),
                        "t".to_string(),
                        "m".to_string(),
                        "e".to_string(),
                    ),
                ];
                agg.add_errors(batch);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        assert_eq!(aggregator.count(), 6);
    }

    #[test]
    fn test_pragma_synchronous_normal() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        set_pragma_synchronous_normal(&conn)?;

        let sync_mode: i32 = conn.pragma_query_value(None, "synchronous", |row| row.get(0))?;
        assert_eq!(sync_mode, 1);

        Ok(())
    }

    #[test]
    fn test_pragma_synchronous_full() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        set_pragma_synchronous_full(&conn)?;

        let sync_mode: i32 = conn.pragma_query_value(None, "synchronous", |row| row.get(0))?;
        assert_eq!(sync_mode, 2);

        Ok(())
    }

    #[test]
    fn test_connection_per_thread_strategy() -> Result<()> {
        use tempfile::NamedTempFile;

        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path().to_str().unwrap();

        let handler1 = create_worker_handler(path)?;
        let handler2 = create_worker_handler(path)?;

        let fk1: i32 = handler1.connection().pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
        let fk2: i32 = handler2.connection().pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
        assert_eq!(fk1, 1);
        assert_eq!(fk2, 1);

        let sync1: i32 = handler1.connection().pragma_query_value(None, "synchronous", |row| row.get(0))?;
        let sync2: i32 = handler2.connection().pragma_query_value(None, "synchronous", |row| row.get(0))?;
        assert_eq!(sync1, 1, "Worker connections should use PRAGMA synchronous=NORMAL");
        assert_eq!(sync2, 1, "Worker connections should use PRAGMA synchronous=NORMAL");

        Ok(())
    }

    #[test]
    fn test_error_aggregation_across_threads() {
        use std::sync::Arc;

        let aggregator = Arc::new(ErrorAggregator::new());
        let mut handles = vec![];

        for worker_id in 0..3 {
            let agg = Arc::clone(&aggregator);
            let handle = thread::spawn(move || {
                for error_num in 0..5 {
                    agg.add_error(ErrorDetails::new(
                        format!("FILE_WORKER{}", worker_id),
                        format!("table{}", worker_id),
                        format!("Foreign key violation {}", error_num),
                        "SQLite FK error".to_string(),
                    ));
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Worker thread panicked");
        }

        let all_errors = aggregator.get_errors();
        assert_eq!(all_errors.len(), 15, "Should collect all errors from all 3 workers (3 * 5 = 15)");
    }

    #[test]
    fn test_concurrent_error_collection_maintains_order_independence() {
        let aggregator = ErrorAggregator::new();
        let mut handles = vec![];

        for thread_id in 0..4 {
            let agg = aggregator.clone();
            let handle = thread::spawn(move || {
                let error_count = (thread_id + 1) * 3;
                for i in 0..error_count {
                    agg.add_error(ErrorDetails::new(
                        format!("T{}_E{}", thread_id, i),
                        "table".to_string(),
                        "msg".to_string(),
                        "err".to_string(),
                    ));
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        let all_errors = aggregator.get_errors();
        assert_eq!(all_errors.len(), 30);

        let t0_errors: Vec<_> = all_errors.iter().filter(|e| e.file_id.starts_with("T0_")).collect();
        let t1_errors: Vec<_> = all_errors.iter().filter(|e| e.file_id.starts_with("T1_")).collect();
        let t2_errors: Vec<_> = all_errors.iter().filter(|e| e.file_id.starts_with("T2_")).collect();
        let t3_errors: Vec<_> = all_errors.iter().filter(|e| e.file_id.starts_with("T3_")).collect();

        assert_eq!(t0_errors.len(), 3);
        assert_eq!(t1_errors.len(), 6);
        assert_eq!(t2_errors.len(), 9);
        assert_eq!(t3_errors.len(), 12);
    }

    #[test]
    fn test_worker_handler_foreign_key_enforcement() -> Result<()> {
        use tempfile::NamedTempFile;

        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path().to_str().unwrap();

        let handler = create_worker_handler(path)?;

        let fk_enabled: i32 = handler.connection()
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))?;

        assert_eq!(fk_enabled, 1, "Worker handlers must have foreign key enforcement enabled");

        Ok(())
    }

    #[test]
    fn test_worker_handler_pragma_configuration() -> Result<()> {
        use tempfile::NamedTempFile;

        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path().to_str().unwrap();

        let handler = create_worker_handler(path)?;

        let sync_mode: i32 = handler.connection()
            .pragma_query_value(None, "synchronous", |row| row.get(0))?;

        assert_eq!(sync_mode, 1, "Worker handlers should use PRAGMA synchronous=NORMAL for better write performance");

        Ok(())
    }

    #[test]
    fn test_parallel_workers_isolated_connections() -> Result<()> {
        use tempfile::NamedTempFile;

        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path().to_str().unwrap();

        let handlers: Vec<_> = (0..4)
            .map(|_| create_worker_handler(path))
            .collect::<Result<Vec<_>>>()?;

        for handler in &handlers {
            let fk: i32 = handler.connection().pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
            let sync: i32 = handler.connection().pragma_query_value(None, "synchronous", |row| row.get(0))?;

            assert_eq!(fk, 1, "All worker connections must have foreign keys enabled");
            assert_eq!(sync, 1, "All worker connections should use NORMAL synchronous mode");
        }

        assert_eq!(handlers.len(), 4, "Should create 4 independent worker handlers");

        Ok(())
    }

    #[test]
    fn test_error_aggregator_batch_collection_from_parallel_workers() {
        let aggregator = ErrorAggregator::new();
        let mut handles = vec![];

        for worker_id in 0..5 {
            let agg = aggregator.clone();
            let handle = thread::spawn(move || {
                let mut local_errors = Vec::new();
                for i in 0..10 {
                    local_errors.push(ErrorDetails::new(
                        format!("WORKER{}_FILE", worker_id),
                        format!("table_{}", worker_id),
                        format!("FK violation {}", i),
                        "FK error".to_string(),
                    ));
                }
                agg.add_errors(local_errors);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Worker thread panicked");
        }

        let all_errors = aggregator.get_errors();
        assert_eq!(all_errors.len(), 50, "Should collect all errors from 5 workers * 10 errors each");
    }
}
