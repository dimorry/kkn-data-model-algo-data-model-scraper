#!/usr/bin/env python3
"""
Export DuckDB tables content to Excel file with separate tabs
"""
import duckdb
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from logger_config import LoggerConfig


def export_to_excel(db_path="tables.duckdb", output_file="tables_export.xlsx", overwrite=False):
    """Export tables and columns data to Excel with separate tabs"""

    # Setup logging
    logger_config = LoggerConfig(
        name="ExcelExporter",
        log_level=logging.INFO,
        log_file="export.log"
    )
    logger = logger_config.get_logger()

    logger.info("Starting Excel export process")

    # Check if database exists
    if not Path(db_path).exists():
        logger.error(f"Database file {db_path} not found")
        return False

    # Check if output file exists and handle overwrite
    output_path = Path(output_file)
    if output_path.exists():
        if overwrite:
            logger.info(f"Output file {output_file} exists, overwriting...")
            try:
                output_path.unlink()  # Delete existing file
                logger.debug(f"Deleted existing file: {output_file}")
            except Exception as e:
                logger.error(f"Failed to delete existing file {output_file}: {e}")
                return False
        else:
            logger.error(f"Output file {output_file} already exists. Use overwrite=True to replace it.")
            return False

    try:
        # Connect to DuckDB
        con = duckdb.connect(db_path)
        logger.info(f"Connected to database: {db_path}")

        # Query tables data
        logger.info("Querying tables data...")
        tables_df = con.execute("""
            SELECT
                id,
                name as table_name,
                description,
                calculated_fields_description,
                created_at
            FROM tables
            ORDER BY id
        """).fetchdf()

        logger.info(f"Found {len(tables_df)} tables")

        # Query columns data
        logger.info("Querying columns data...")
        columns_df = con.execute("""
            SELECT
                c.id,
                c.table_id,
                t.name as table_name,
                c.field_name,
                c.description,
                c.data_type,
                c.is_key,
                c.is_calculated,
                c.created_at
            FROM columns c
            LEFT JOIN tables t ON c.table_id = t.id
            ORDER BY c.table_id, c.id
        """).fetchdf()

        logger.info(f"Found {len(columns_df)} columns")

        # Create Excel writer with multiple sheets
        logger.info(f"Writing to Excel file: {output_file}")
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Write tables to first tab
            tables_df.to_excel(writer, sheet_name='Tables', index=False)
            logger.info("Written tables data to 'Tables' tab")

            # Write columns to second tab
            columns_df.to_excel(writer, sheet_name='Columns', index=False)
            logger.info("Written columns data to 'Columns' tab")

            # Format worksheets with text wrapping and column sizing
            from openpyxl.styles import Alignment

            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]

                # Auto-adjust column widths and apply text wrapping
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    column_header = column[0].value

                    # Check if this is a description column
                    is_description_column = (column_header and
                                           'description' in str(column_header).lower())

                    for cell in column:
                        # Apply text wrapping to all cells
                        cell.alignment = Alignment(wrap_text=True, vertical='top')

                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass

                    # Set column width based on content type
                    if is_description_column:
                        # Description columns get wider width and taller rows
                        adjusted_width = min(max_length + 2, 80)  # Wider for descriptions
                        worksheet.column_dimensions[column_letter].width = adjusted_width
                    else:
                        # Regular columns get standard width
                        adjusted_width = min(max_length + 2, 30)
                        worksheet.column_dimensions[column_letter].width = adjusted_width

                # Set minimum row height for better text wrapping display
                for row in worksheet.iter_rows():
                    worksheet.row_dimensions[row[0].row].height = 30  # Minimum height for wrapped text

                # Add auto-filter to all columns
                if worksheet.max_row > 1:  # Only add filter if there's data beyond headers
                    from openpyxl.utils import get_column_letter
                    max_col_letter = get_column_letter(worksheet.max_column)
                    worksheet.auto_filter.ref = f"A1:{max_col_letter}{worksheet.max_row}"
                    logger.debug(f"Added auto-filter to {sheet_name} tab: A1:{max_col_letter}{worksheet.max_row}")

        con.close()

        # Log summary
        logger.info("Excel export completed successfully!")
        logger.info(f"Output file: {output_file}")
        logger.info(f"Tables exported: {len(tables_df)}")
        logger.info(f"Columns exported: {len(columns_df)}")

        return True

    except Exception as e:
        logger.error(f"Export failed: {e}")
        return False


def main():
    """Main function to run the export"""
    print("DuckDB to Excel Exporter")
    print("=" * 40)

    # Generate timestamped filename
    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # output_file = f"tables_export_{timestamp}.xlsx"
    output_file = f"kinaxis_tables_export.xlsx"

    success = export_to_excel(output_file=output_file, overwrite=True)

    if success:
        print(f"✅ Export successful! File saved as: {output_file}")
    else:
        print("❌ Export failed. Check the logs for details.")


if __name__ == "__main__":
    main()
