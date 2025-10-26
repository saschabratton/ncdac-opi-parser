# NC DAC Offender Public Information Parser

This tool processes fixed-width format data files from the North Carolina Department of Adult Correction and creates a normalized SQLite database with proper foreign key relationships.


## Installation

### Building from Source

```bash
cargo build --release
```

The binary will be available at `./target/release/ncdac-opi-parser`.

### Installing Globally

```bash
cargo install --path .
```

## Usage

### Basic Usage

```bash
# Process all files with default settings
# (Will prompt to download if files are missing)
ncdac-opi-parser --output database.db
```

### Command-Line Options

```
Options:
  -o, --output <OUTPUT>
          Output SQLite database file path (required)

  -r, --reference <REFERENCE>
          Reference file ID to use as foreign key source
          [default: OFNT3AA1]

      --keep-data
          Keep data files after processing

  -h, --help
          Print help information

  -V, --version
          Print version information
```

## Data Files

The parser requires NC DAC data files to operate. These files are **not** included in the repository due to their size (~661 MB total). The tool can automatically download them from the official NC DAC website.

### Data Directory

Files are downloaded to `./data/` in the project directory:

```
data/
├── OFNT3AA1.zip    # 35MB - Offender Profile
├── APPT7AA1.zip    # 40MB - Probation/Parole Client
├── APPT9BJ1.zip    # 1MB  - Impact Scheduling
├── INMT4AA1.zip    # 40MB - Inmate Profile
├── INMT4BB1.zip    # 20MB - Sentence Computations
├── INMT4CA1.zip    # 20MB - Parole Analyst Review
├── INMT9CF1.zip    # 60MB - Disciplinary Infractions
├── OFNT1BA1.zip    # 65MB - Financial Obligation
├── OFNT3BB1.zip    # 105MB - Court Commitment
├── OFNT3CE1.zip    # 235MB - Sentence Component
├── OFNT3DE1.zip    # 40MB - Special Conditions
└── OFNT9BE1.zip    # 5MB  - Warrant Issued
```

