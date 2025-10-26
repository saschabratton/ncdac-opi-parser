//! NC DAC OPI Parser Library
//!
//! This library provides utilities and functionality for parsing
//! NC DAC Offender Public Information records.

pub mod concurrency;
pub mod data_handler;
pub mod download;
pub mod file_description;
pub mod files;
pub mod parser;
pub mod unzip;
pub mod utilities;

pub use concurrency::{create_worker_handler, ErrorAggregator, set_pragma_synchronous_full, set_pragma_synchronous_normal};
pub use data_handler::{DataHandler, ErrorDetails, ProcessingResults};
pub use file_description::{FieldDefinition, FileDescription};
pub use parser::{DataParser, RecordIterator};
