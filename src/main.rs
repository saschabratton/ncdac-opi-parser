//! NC DAC OPI Parser CLI Application
//!
//! This is the main CLI binary that parses NC DAC Offender Public Information
//! records into a SQLite database.

use anyhow::{Context, Result};
use clap::Parser;
use dialoguer::{theme::ColorfulTheme, Confirm, MultiSelect, Select};
use indicatif::{ProgressBar, ProgressStyle};
use ncdac_opi_parser::{
    concurrency::{create_worker_handler, ErrorAggregator},
    data_handler::DataHandler,
    download::{
        are_decompressed_files_valid, categorize_files, download_data_file, get_data_dir,
        get_file_status, FileStatus,
    },
    files::{get_file_by_id, FILES},
    unzip::{calculate_total_uncompressed_bytes, decompress_with_shared_progress},
    utilities::{count_lines, delete_data_subdirectory, format_count, format_duration},
};
use rayon::prelude::*;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

/// NC DAC Offender Public Information Parser
///
/// Parse NC DAC Offender Public Information records into a SQLite database.
/// This tool processes fixed-width format data files and creates a normalized
/// database with proper foreign key relationships.
#[derive(Parser, Debug)]
#[command(name = "ncdac-opi-parser")]
#[command(about = "Parse NC DAC Offender Public Information records into a SQLite database")]
#[command(version)]
struct Cli {
    /// Output SQLite database file path
    #[arg(short, long)]
    output: PathBuf,

    /// Reference file ID to use as foreign key source
    #[arg(short, long, default_value = "OFNT3AA1")]
    reference: String,

    /// Keep data files after processing
    #[arg(long)]
    keep_data: bool,
}

/// Creates a spinner with the ora-compatible "bouncingBar" style
fn create_spinner(message: &str) -> ProgressBar {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .expect("Invalid template")
            .tick_strings(&["[    ]", "[=   ]", "[==  ]", "[=== ]", "[ ===]", "[  ==]", "[   =]", "[    ]", "[   =]", "[  ==]", "[ ===]", "[====]"]),
    );
    spinner.set_message(message.to_string());
    spinner.enable_steady_tick(std::time::Duration::from_millis(80));
    spinner
}

/// Prompt user to confirm or select a reference file
fn confirm_reference_file(default_reference: &str) -> Result<String> {
    let default_file = get_file_by_id(default_reference)
        .ok_or_else(|| anyhow::anyhow!("Invalid default reference file: {}", default_reference))?;

    let use_default = Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt(format!(
            "Use '{}' ({}) as reference file?",
            default_file.name, default_file.id
        ))
        .default(true)
        .interact()?;

    if use_default {
        return Ok(default_reference.to_string());
    }

    let items: Vec<String> = FILES
        .iter()
        .map(|f| {
            if f.id == default_reference {
                format!("{} ({}) - default", f.name, f.id)
            } else {
                format!("{} ({})", f.name, f.id)
            }
        })
        .collect();

    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Select reference file")
        .items(&items)
        .default(0)
        .interact()?;

    Ok(FILES[selection].id.to_string())
}

/// Main application entry point
#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let epoch = SystemTime::now();

    let reference_id = confirm_reference_file(&args.reference)?;
    println!();

    let reference_file = get_file_by_id(&reference_id);
    if reference_file.is_none() {
        eprintln!("❌ Unknown reference file id: {}", reference_id);
        eprintln!("Available file IDs:");
        for file in &FILES {
            eprintln!("  - {} ({})", file.id, file.name);
        }
        std::process::exit(1);
    }
    let reference_file = reference_file.unwrap();

    match handle_downloads(reference_file) {
        Ok(downloaded) => {
            if downloaded {
                println!();
            }
        }
        Err(e) => {
            eprintln!("❌ Download failed");
            eprintln!("Error: {:#}", e);
            std::process::exit(1);
        }
    }

    let data_handler = match run(&args, reference_file).await {
        Ok(handler) => handler,
        Err(e) => {
            eprintln!("❌ Processing failed");
            eprintln!("Error: {:#}", e);
            std::process::exit(1);
        }
    };

    let total_duration = format_duration(epoch, None)
        .context("Failed to calculate total duration")?;
    println!("✅ Processing complete in {}", total_duration);

    if !data_handler.errors.is_empty() {
        print!(
            "\n⚠️  {} errors encountered while processing. View them? (y/N): ",
            data_handler.errors.len()
        );
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        let answer = input.trim();
        if answer.eq_ignore_ascii_case("y") || answer.eq_ignore_ascii_case("yes") {
            for (index, error_details) in data_handler.errors.iter().enumerate() {
                println!(
                    "\n[{}/{}] {}",
                    index + 1,
                    data_handler.errors.len(),
                    error_details.message
                );
            }
        }
    }

    Ok(())
}

/// Download a file with retry on hash mismatch.
///
/// For reference files: prompts to retry or quit on failure
/// For other files: prompts to retry or skip on failure
fn download_with_retry(
    file: &ncdac_opi_parser::files::FileMetadata,
    data_dir: &std::path::Path,
    is_reference: bool,
) -> Result<bool> {
    loop {
        match download_data_file(file, data_dir) {
            Ok(_) => return Ok(true),
            Err(e) => {
                eprintln!("\n❌ Failed to download {}: {:#}", file.id, e);

                if is_reference {
                    println!("\nThe reference file is required to proceed.");
                    println!("  [r] Retry download");
                    println!("  [q] Quit");
                    print!("\nYour choice (r/q): ");
                } else {
                    println!("\n  [r] Retry download");
                    println!("  [s] Skip this file");
                    print!("\nYour choice (r/s): ");
                }

                io::stdout().flush()?;

                let mut input = String::new();
                io::stdin().read_line(&mut input)?;
                let choice = input.trim().to_lowercase();

                match choice.as_str() {
                    "r" => continue,
                    "q" if is_reference => {
                        eprintln!("Cannot proceed without reference file. Exiting.");
                        std::process::exit(1);
                    }
                    "s" if !is_reference => return Ok(false),
                    _ => {
                        if is_reference {
                            eprintln!("Invalid choice. Please choose 'r' to retry or 'q' to quit.");
                        } else {
                            eprintln!("Invalid choice. Please choose 'r' to retry or 's' to skip.");
                        }
                    }
                }
            }
        }
    }
}

/// Handle file downloads based on CLI arguments and missing files.
///
/// Returns `true` if downloads were performed, `false` otherwise.
fn handle_downloads(reference_file: &ncdac_opi_parser::files::FileMetadata) -> Result<bool> {
    let data_dir = get_data_dir();

    let spinner = create_spinner("Checking for available data files...");
    let file_status = categorize_files(&FILES, &data_dir);
    spinner.finish_and_clear();

    if !file_status.unverifiable.is_empty() {
        println!("\n⚠️  The following files have decompressed data but the ZIP file is missing:");
        println!("    Data cannot be verified for integrity.");
        for file_id in &file_status.unverifiable {
            let file = get_file_by_id(file_id).unwrap();
            println!("   - {} ({})", file.id, file.name);
        }

        println!("\nWould you like to:");
        println!("  [d] Download ZIP files to verify data integrity");
        println!("  [c] Continue without verification (default)");
        print!("\nYour choice (d/c) [c]: ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let choice = input.trim().to_lowercase();

        if choice == "d" {
            println!("\n📥 Downloading ZIP files for verification...\n");
            for file_id in &file_status.unverifiable {
                let file = get_file_by_id(file_id).unwrap();
                download_with_retry(file, &data_dir, false)?;
            }
        } else {
            println!("Continuing without verification.");
        }
    }

    let mut all_problematic: Vec<String> = file_status.missing.clone();
    all_problematic.extend(file_status.incomplete.clone());

    if !all_problematic.is_empty() {
        let reference_missing = all_problematic.contains(&reference_file.id.to_string());

        if reference_missing {
            println!("⚠️  Reference file {} ({}) is required but not found.", reference_file.id, reference_file.name);
            println!("\nThis file must be downloaded to proceed.");
            println!("  [d] Download now");
            println!("  [q] Quit");
            print!("\nYour choice (d/q): ");
            io::stdout().flush()?;

            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let choice = input.trim().to_lowercase();

            match choice.as_str() {
                "d" => {
                    println!("\n📥 Downloading {}...\n", reference_file.name);
                    download_with_retry(reference_file, &data_dir, true)?;
                }
                _ => {
                    eprintln!("Cannot proceed without reference file. Exiting.");
                    std::process::exit(1);
                }
            }
        }

        let other_problematic: Vec<_> = all_problematic
            .iter()
            .filter(|id| id.as_str() != reference_file.id)
            .collect();

        if !other_problematic.is_empty() {
            let other_missing: Vec<_> = file_status.missing
                .iter()
                .filter(|id| id.as_str() != reference_file.id)
                .collect();

            if !other_missing.is_empty() {
                println!("\n📋 The following optional files are missing:");
                for file_id in &other_missing {
                    let file = get_file_by_id(file_id).unwrap();
                    println!("   - {} ({})", file.id, file.name);
                }
            }

            let other_incomplete: Vec<_> = file_status.incomplete
                .iter()
                .filter(|id| id.as_str() != reference_file.id)
                .collect();

            if !other_incomplete.is_empty() {
                println!("\n⚠️  The following files are out-of-date or incomplete (incorrect size):");
                for file_id in &other_incomplete {
                    let file = get_file_by_id(file_id).unwrap();
                    println!("   - {} ({})", file.id, file.name);
                }
            }

            println!("\nWould you like to download them?");
            println!("  [a] Download all (default)");
            println!("  [s] Skip all");
            println!("  [c] Choose which files to download");
            print!("\nYour choice (a/s/c) [a]: ");
            io::stdout().flush()?;

            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let choice = input.trim().to_lowercase();

            match choice.as_str() {
                "s" => {
                    println!("Skipping optional file downloads.");
                }
                "c" => {
                    let options: Vec<String> = other_problematic
                        .iter()
                        .map(|id| {
                            let file = get_file_by_id(id).unwrap();
                            let status = if file_status.incomplete.contains(id) {
                                "out-of-date or incomplete"
                            } else {
                                "missing"
                            };
                            format!("{} ({}) [{}]", file.id, file.name, status)
                        })
                        .collect();

                    let selections = MultiSelect::with_theme(&ColorfulTheme::default())
                        .with_prompt("Select files to download (use Space to select, Enter to confirm)")
                        .items(&options)
                        .interact()?;

                    if !selections.is_empty() {
                        println!("\n📥 Downloading selected files...\n");
                        for idx in selections {
                            let file_id = other_problematic[idx].as_str();
                            let file = get_file_by_id(file_id).unwrap();
                            download_with_retry(file, &data_dir, false)?;
                        }
                    }
                }
                _ => {
                    println!("\n📥 Downloading all missing/out-of-date files...\n");
                    for file_id in &other_problematic {
                        let file = get_file_by_id(file_id).unwrap();
                        download_with_retry(file, &data_dir, false)?;
                    }
                }
            }
        }

        return Ok(true);
    }

    Ok(false)
}

/// Main workflow function
async fn run(
    args: &Cli,
    reference_file: &ncdac_opi_parser::files::FileMetadata,
) -> Result<DataHandler> {
    let data_dir = get_data_dir();

    let mut already_decompressed = Vec::new();
    let mut missing_files = Vec::new();
    let mut incomplete_files = Vec::new();
    let mut files_to_decompress = Vec::new();

    for file in &FILES {
        if are_decompressed_files_valid(file, &data_dir) {
            already_decompressed.push(file.id);
            continue;
        }

        match get_file_status(file, &data_dir) {
            FileStatus::Missing => {
                missing_files.push(file.id);
                continue;
            }
            FileStatus::Incomplete => {
                incomplete_files.push(file.id);
                continue;
            }
            FileStatus::Complete => {
                files_to_decompress.push(*file);
            }
        }
    }

    if files_to_decompress.is_empty() {
        if !missing_files.is_empty() || !incomplete_files.is_empty() {
            for file_id in &missing_files {
                println!(
                    "\x1b[34mℹ\x1b[0m Skipped {} (ZIP file not available)",
                    file_id
                );
            }
            for file_id in &incomplete_files {
                println!(
                    "\x1b[33m⚠\x1b[0m Skipped {} (ZIP file out-of-date or incomplete)",
                    file_id
                );
            }
        }
    } else {
        let total_bytes = calculate_total_uncompressed_bytes(&files_to_decompress, &data_dir)
            .context("Failed to calculate total uncompressed bytes")?;

        let total_mb = total_bytes as f64 / 1_048_576.0;

        let shared_pb = Arc::new(ProgressBar::new(total_bytes));
        shared_pb.set_style(
            ProgressStyle::default_bar()
                .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        shared_pb.set_message(format!(
            "Decompressing {} files concurrently - {:.1} MB total",
            files_to_decompress.len(),
            total_mb
        ));

        let decompression_start = SystemTime::now();

        let result: Result<()> = files_to_decompress
            .par_iter()
            .try_for_each(|file| {
                decompress_with_shared_progress(file.id, file.name, &shared_pb)?;
                Ok(())
            });

        match result {
            Ok(_) => {
                let decompression_duration = format_duration(decompression_start, None)
                    .context("Failed to calculate decompression duration")?;

                shared_pb.finish_with_message(format!(
                    "✓ Decompressed {} files - {:.1} MB total in {}",
                    files_to_decompress.len(),
                    total_mb,
                    decompression_duration
                ));
                println!();
            }
            Err(e) => {
                shared_pb.finish_and_clear();
                eprintln!("❌ Failed to decompress files");
                return Err(e);
            }
        }

        if !missing_files.is_empty() || !incomplete_files.is_empty() {
            for file_id in &missing_files {
                println!(
                    "\x1b[34mℹ\x1b[0m Skipped {} (ZIP file not available)",
                    file_id
                );
            }
            for file_id in &incomplete_files {
                println!(
                    "\x1b[33m⚠\x1b[0m Skipped {} (ZIP file out-of-date or incomplete)",
                    file_id
                );
            }
        }
    }

    let mut data_handler = DataHandler::new(
        args.output
            .to_str()
            .context("Invalid output path")?,
    )
    .context("Failed to create database handler")?;

    let init_start_time = SystemTime::now();

    let ref_dat_path = data_dir.join(reference_file.id).join(format!("{}.dat", reference_file.id));
    let ref_line_count = count_lines(&ref_dat_path)
        .with_context(|| format!("Failed to count lines in {}", ref_dat_path.display()))?;

    let ref_pb = ProgressBar::new(ref_line_count);
    ref_pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} records ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );
    ref_pb.set_message(format!(
        "Processing reference file ({}) - Inserting {} records into {} table",
        reference_file.id,
        format_count(ref_line_count as usize),
        reference_file.name
    ));

    let init_results = data_handler
        .init(reference_file, Some(&ref_pb))
        .context("Failed to initialize with reference file")?;

    let init_duration = format_duration(init_start_time, None)
        .context("Failed to calculate initialization duration")?;

    if !init_results.errors.is_empty() {
        ref_pb.finish_and_clear();
        println!(
            "⚠️  {} errors encountered while processing {} reference file.",
            init_results.errors.len(),
            reference_file.name
        );
    } else {
        ref_pb.finish_with_message(format!(
            "✓ Processed reference file ({}) - Inserted {} records into {} table in {}",
            reference_file.id,
            format_count(init_results.processed),
            reference_file.name,
            init_duration
        ));
    }

    println!("\n📋 Reference file processing complete");

    let files_to_process: Vec<_> = FILES
        .iter()
        .filter(|file| {
            if file.id == reference_file.id {
                return false;
            }
            let file_dir = data_dir.join(file.id);
            file_dir.exists()
        })
        .collect();

    if files_to_process.is_empty() {
        if !args.keep_data {
            let spinner = create_spinner("Cleaning up data files...");
            for file in &FILES {
                delete_data_subdirectory(file.id)
                    .await
                    .with_context(|| format!("Failed to delete data directory for {}", file.id))?;
            }
            spinner.finish_with_message("Cleaned up data files".to_string());
        }
        return Ok(data_handler);
    }

    let mut total_records = 0u64;
    for file in &files_to_process {
        let dat_path = data_dir.join(file.id).join(format!("{}.dat", file.id));
        if let Ok(line_count) = count_lines(&dat_path) {
            total_records += line_count;
        }
    }

    println!("🚀 Starting parallel processing of {} files", files_to_process.len());

    let combined_pb = Arc::new(ProgressBar::new(total_records));
    combined_pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} records ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );
    combined_pb.set_message(format!(
        "Processing {} files concurrently - {} total records",
        files_to_process.len(),
        format_count(total_records as usize)
    ));

    let error_aggregator = Arc::new(ErrorAggregator::new());

    let database_path = args.output.to_str().context("Invalid output path")?;
    let parallel_start_time = SystemTime::now();

    let ref_file = data_handler.reference_file().copied()
        .context("Reference file not set before parallel processing")?;
    let ref_table = data_handler.reference_table_name()
        .context("Reference table not set before parallel processing")?
        .to_string();
    let ref_field = data_handler.reference_field()
        .context("Reference field not set before parallel processing")?
        .to_string();

    files_to_process.par_iter().for_each(|file| {
        let mut worker_handler = match create_worker_handler(database_path) {
            Ok(handler) => handler,
            Err(e) => {
                eprintln!("❌ Failed to create worker handler for {}: {:#}", file.id, e);
                return;
            }
        };

        worker_handler.init_from_reference(&ref_file, &ref_table, &ref_field);

        let pb = Arc::clone(&combined_pb);
        let agg = Arc::clone(&error_aggregator);

        match worker_handler.process_file(file, Some(&pb)) {
            Ok(Some(results)) => {
                if !results.errors.is_empty() {
                    agg.add_errors(results.errors);
                }
            }
            Ok(None) => {
                // File was already processed (shouldn't happen in parallel context)
            }
            Err(e) => {
                eprintln!("❌ Failed to process file {}: {:#}", file.id, e);
            }
        }
    });

    let parallel_duration = format_duration(parallel_start_time, None)
        .context("Failed to calculate parallel processing duration")?;

    combined_pb.finish_with_message(format!(
        "✓ Processed {} files concurrently in {} - {} total records",
        files_to_process.len(),
        parallel_duration,
        format_count(total_records as usize)
    ));

    println!("✅ Parallel processing complete");

    let all_parallel_errors = error_aggregator.get_errors();
    data_handler.errors.extend(all_parallel_errors);

    if !args.keep_data {
        let spinner = create_spinner("Cleaning up data files...");
        for file in &FILES {
            delete_data_subdirectory(file.id)
                .await
                .with_context(|| format!("Failed to delete data directory for {}", file.id))?;
        }
        spinner.finish_with_message("Cleaned up data files".to_string());
    }

    Ok(data_handler)
}
