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

### 📊 **Advanced Excel Export**
- **🔗 Recursive Reference Expansion**: Automatically expands reference fields to show complete relationship chains
  - Example: `Allocation.ScheduledReceipt.Order.Site.Value` shows the full path through multiple tables
  - Detects and prevents infinite loops with cycle detection
  - Configurable maximum depth (default: 5 levels)
  - Inherits display settings from root reference field
- **📋 Two-Tab Format**: Tables and Columns in separate worksheets with frozen headers
- **🎯 Smart Field Organization**:
  - Custom column order: table_name → is_key → field_name → is_calculated
  - Indented expanded field names (4 spaces) for visual hierarchy
  - Origin table identification in descriptions: `[From TableName]`
- **🔒 Clean Interface**:
  - ID columns (id, table_id, referenced_table_id) hidden but preserved
  - Fixed header rows for easy navigation through large datasets
- **✨ Professional Formatting**:
  - Auto-adjusting row heights based on text content
  - Text wrapping on all cells with optimal column widths
  - Auto-filters on all columns for advanced data analysis
  - Description columns get enhanced spacing (up to 80 characters wide)

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

# Export database to Excel with recursive expansion
success = export_to_excel(
    db_path="mappings.duckdb",
    output_file="kinaxis_tables_export.xlsx",
    overwrite=True
)

if success:
    print("✅ Excel export completed!")
    print("Features included:")
    print("  🔗 Recursive reference field expansion")
    print("  📋 Frozen headers on both worksheets")
    print("  🎯 Smart column organization and indentation")
    print("  🔒 Hidden ID columns for clean viewing")
    print("  ✨ Professional formatting with auto-sizing")
```

### Advanced Export Features
The Excel export includes sophisticated relationship mapping:

```python
# Example of recursive expansion output:
# Original field: Part (Reference)
# Expanded fields:
#   Part.Name                    # Direct field from Part table
#   Part.Site.Value             # Site reference from Part table
#
# Original field: ScheduledReceipt (Reference)
# Expanded fields:
#   ScheduledReceipt.Line        # Direct field
#   ScheduledReceipt.Order.Id    # SupplyOrder reference fields
#   ScheduledReceipt.Order.Type
#   ScheduledReceipt.Order.Site.Value  # Nested Site reference
```

## Project Structure

```
kkn-data-model-algo-data-model-scraper/
├── scraper.py              # Main scraper class with reference extraction
├── database.py             # DuckDB integration with foreign keys
├── export_to_excel.py      # Advanced Excel export with recursive expansion
├── logger_config.py        # Logging configuration
├── test_database.py        # Database testing
├── session_data.json       # Saved browser session
├── mappings.duckdb         # Database file (knx_doc_tables/knx_doc_columns)
├── kinaxis_tables_export.xlsx  # Generated Excel export
└── README.md              # This file
```

## Database Schema

### knx_doc_tables Table
- `id`: Primary key
- `name`: Table name (extracted from h1)
- `description`: Table description (between h1 and first table)
- `calculated_fields_description`: Calculated fields info (between h2 and second table)
- `created_at`: Timestamp

### knx_doc_columns Table
- `id`: Primary key
- `table_id`: Foreign key to knx_doc_tables
- `field_name`: Column name
- `description`: Field description
- `data_type`: Data type (e.g., Reference, String, Integer)
- `is_key`: Key information ("Yes" for key fields)
- `is_calculated`: Boolean (False for regular fields, True for calculated)
- `referenced_table_id`: Foreign key to knx_doc_tables (for Reference fields)
- `display_on_export`: Boolean (controls which fields appear in recursive expansion)
- `created_at`: Timestamp

### Reference Field Expansion
The system automatically:
- Detects Reference data types in column descriptions
- Extracts referenced table names using intelligent parsing
- Links referenced_table_id to the appropriate table
- Marks key fields with display_on_export=True for expansion
- Generates recursive field paths showing complete relationship chains

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
- Default database: `mappings.duckdb`
- Schema: `knx_doc_tables` and `knx_doc_columns` with foreign key relationships
- Configurable path in scraper initialization
- Automatic schema creation and migration
- Reference field linking with intelligent table name extraction

### Export Settings
- **Recursive Expansion**: Maximum depth of 5 levels (configurable)
- **Column Organization**: table_name → is_key → field_name → is_calculated
- **Visual Formatting**: 4-space indentation for expanded fields
- **Hidden Columns**: ID fields hidden but preserved for data integrity
- **Text Handling**: Auto-wrapping with dynamic row heights
- **Navigation**: Frozen headers and auto-filters for large datasets
- **Professional Output**: Optimized for analysis and presentations

## Contributing

1. Follow the existing code structure
2. Add comprehensive logging for new features
3. Include error handling for robustness
4. Update tests for new functionality
5. Document any new configuration options

## Performance & Capabilities

### Current Dataset
- **39 Tables** documented with complete schema information
- **1,251 Total Columns** including recursive expansions
- **Multi-level Relationships** with up to 5 levels of depth
- **Comprehensive Coverage** of Kinaxis data model relationships

### Export Performance
- ✅ **Intelligent Cycle Detection**: Prevents infinite loops in complex relationships
- ✅ **Optimized Queries**: Efficient database operations with proper indexing
- ✅ **Memory Management**: Handles large datasets with streaming processing
- ✅ **Professional Output**: Publication-ready Excel files with advanced formatting

### Key Relationship Examples
- `Allocation.ScheduledReceipt.Order.Site.Value` - 4-level expansion
- `Allocation.Part.Site.Value` - Cross-table site references
- `HistoricalDemandHeader.PartCustomer.Customer.Site.Value` - Complex nested relationships
- `SourceConstraint.PartSource.Source.DestinationSite.Value` - Supply chain mappings

## License

Internal tool for Kinaxis documentation processing.