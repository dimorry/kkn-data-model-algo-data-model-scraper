# Kinaxis Documentation Scraper

A specialized web scraper for extracting table documentation from Kinaxis help pages. The scraper intelligently extracts table schemas, descriptions, and field information, storing everything in a DuckDB database with Excel export capabilities.

## Features

### 🔍 **Intelligent Content Extraction**
- **Table Names**: Extracts from h1 headers ending with "table"
- **Table Descriptions**: Captures content between h1 and first table
- **Calculated Fields**: Extracts descriptions between h2 and second table
- **Column Data**: Processes both regular and calculated field tables
- **Smart Detection**: Automatically distinguishes calculated vs regular fields

### 💾 **Database Storage**
- **DuckDB Integration**: Stores all extracted data in structured database
- **Merge Functionality**: Handles duplicate runs by merging new data
- **Relational Schema**: Proper table relationships and foreign keys
- **Data Integrity**: Prevents duplicate columns while preserving existing data

### 📊 **Excel Export**
- **Two-Tab Format**: Tables and Columns in separate worksheets
- **Professional Formatting**: Text wrapping, auto-filters, optimized column widths
- **Smart Columns**: Description columns get wider spacing
- **Filter-Ready**: Auto-filters on all columns for easy data analysis

## Installation

1. **Install Dependencies**:
```bash
poetry install
# or
pip install playwright duckdb pandas openpyxl
```

2. **Install Browser**:
```bash
poetry run playwright install msedge
# or
playwright install msedge
```

## Usage

### Method 1: Using Saved Session Data
```python
from scraper import EdgeSessionScraper
from logger_config import LoggerConfig
import logging

def main():
    # Setup logging
    logger_config = LoggerConfig(
        name="KinaxisScraper",
        log_level=logging.INFO,
        log_file="scraper.log"
    )

    # Initialize scraper with database
    scraper = EdgeSessionScraper(logger_config=logger_config, db_path="kinaxis_tables.duckdb")

    # Load saved session and scrape
    if scraper.load_session_data():
        result = scraper.scrape_page("https://help.kinaxis.com/path/to/table_page.htm")
        print(f"Scraped table: {result['table_name']}")
        print(f"Columns extracted: {result['columns_count']}")

    scraper.close()

if __name__ == "__main__":
    main()
```

### Method 2: Manual Session Setup
1. **Start Edge with debugging**:
```bash
msedge.exe --remote-debugging-port=9222
```

2. **Manually authenticate** in the Edge browser

3. **Run scraper** (uncomment the connect_to_existing_edge section in main())

### Export to Excel
```python
from export_to_excel import export_to_excel

# Export database to Excel
success = export_to_excel(
    db_path="kinaxis_tables.duckdb",
    output_file="kinaxis_documentation.xlsx",
    overwrite=True
)

if success:
    print("✅ Excel export completed!")
```

## Project Structure

```
kkn-doc-scraper/
├── scraper.py              # Main scraper class
├── database.py             # DuckDB integration
├── export_to_excel.py      # Excel export functionality
├── logger_config.py        # Logging configuration
├── test_database.py        # Database testing
├── session_data.json       # Saved browser session
├── kinaxis_tables.duckdb   # Database file
└── README.md              # This file
```

## Database Schema

### Tables Table
- `id`: Primary key
- `name`: Table name (extracted from h1)
- `description`: Table description (between h1 and first table)
- `calculated_fields_description`: Calculated fields info (between h2 and second table)
- `created_at`: Timestamp

### Columns Table
- `id`: Primary key
- `table_id`: Foreign key to tables
- `field_name`: Column name
- `description`: Field description
- `data_type`: Data type
- `is_key`: Key information
- `is_calculated`: Boolean (False for regular fields, True for calculated)
- `created_at`: Timestamp

## Logging

The application provides comprehensive logging:
- **INFO**: Major operations and results
- **DEBUG**: Detailed extraction steps
- **ERROR**: Failures and issues
- **Files**: Logs saved to specified log files

## Error Handling

- **Graceful Failures**: Continues processing even if some elements aren't found
- **Merge Logic**: Handles duplicate extractions intelligently
- **Session Management**: Robust browser session handling
- **Database Integrity**: Transaction rollback on errors

## Testing

Run the database test:
```bash
python test_database.py
```

## Configuration

### Browser Setup
- Requires Edge browser with debugging port 9222
- Session data automatically saved and reused
- Handles authentication cookies and state

### Database Configuration
- Default database: `tables.duckdb`
- Configurable path in scraper initialization
- Automatic schema creation and migration

### Export Settings
- Text wrapping on all description columns
- Auto-filters for easy data exploration
- Professional formatting for presentations

## Contributing

1. Follow the existing code structure
2. Add comprehensive logging for new features
3. Include error handling for robustness
4. Update tests for new functionality
5. Document any new configuration options

## License

Internal tool for Kinaxis documentation processing.