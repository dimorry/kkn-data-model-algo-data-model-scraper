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


def export_to_excel(db_path="mappings.duckdb", output_file="tables_export.xlsx", overwrite=False):
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
            FROM knx_doc_tables
            ORDER BY id
        """).fetchdf()

        logger.info(f"Found {len(tables_df)} tables")

        # Query base columns data
        logger.info("Querying columns data...")
        base_columns_df = con.execute("""
            SELECT
                c.id,
                c.table_id,
                t.name as table_name,
                c.field_name,
                c.description,
                c.data_type,
                c.is_key,
                c.is_calculated,
                rt.name as referenced_table,
                c.display_on_export,
                c.created_at,
                c.referenced_table_id
            FROM knx_doc_columns c
            LEFT JOIN knx_doc_tables t ON c.table_id = t.id
            LEFT JOIN knx_doc_tables rt ON c.referenced_table_id = rt.id
            ORDER BY c.table_id, c.id
        """).fetchdf()

        # Expand reference fields with display_on_export fields from referenced tables
        logger.info("Expanding reference fields with display_on_export fields...")
        expanded_rows = []

        for _, row in base_columns_df.iterrows():
            # Add the original row
            expanded_rows.append(row.to_dict())

            # If this is a reference field and has a referenced table, add expanded fields
            if (row['data_type'] and str(row['data_type']).lower().startswith('reference') and
                row['referenced_table_id'] is not None and not row['is_calculated']):

                logger.debug(f"[{row['table_name']}] Processing reference field '{row['field_name']}' (data_type: {row['data_type']}, referenced_table_id: {row['referenced_table_id']}, referenced_table: {row['referenced_table']})")

                # Get display_on_export fields from the referenced table
                ref_fields_df = con.execute("""
                    SELECT
                        c.field_name,
                        c.description,
                        c.data_type,
                        c.is_key,
                        c.is_calculated,
                        rt2.name as ref_referenced_table
                    FROM knx_doc_columns c
                    LEFT JOIN knx_doc_tables rt2 ON c.referenced_table_id = rt2.id
                    WHERE c.table_id = ? AND c.display_on_export = TRUE
                    ORDER BY c.id
                """, [row['referenced_table_id']]).fetchdf()

                logger.debug(f"[{row['table_name']}] Found {len(ref_fields_df)} display_on_export fields for referenced table ID {row['referenced_table_id']}")
                if len(ref_fields_df) > 0:
                    logger.debug(f"[{row['table_name']}] Display fields: {list(ref_fields_df['field_name'])}")

                # Add expanded fields
                for _, ref_field in ref_fields_df.iterrows():
                    expanded_field_name = f"{row['table_name']}.{row['referenced_table']}.{ref_field['field_name']}"
                    expanded_description = f"[From {row['referenced_table']}] {ref_field['description']}"

                    expanded_row = {
                        'id': f"{row['id']}.{ref_field['field_name']}",  # Unique identifier
                        'table_id': row['table_id'],
                        'table_name': row['table_name'],
                        'field_name': expanded_field_name,
                        'description': expanded_description,
                        'data_type': ref_field['data_type'],
                        'is_key': ref_field['is_key'],
                        'is_calculated': ref_field['is_calculated'],
                        'referenced_table': ref_field['ref_referenced_table'],
                        'display_on_export': True,  # These are expanded because they have display_on_export=True
                        'created_at': row['created_at']
                    }
                    expanded_rows.append(expanded_row)
                    logger.debug(f"[{row['table_name']}] Added expanded field: {expanded_field_name}")

                if len(ref_fields_df) > 0:
                    logger.info(f"[{row['table_name']}] Expanded reference field '{row['field_name']}' with {len(ref_fields_df)} display fields from '{row['referenced_table']}'")
                else:
                    logger.warning(f"[{row['table_name']}] No display_on_export fields found for reference field '{row['field_name']}' -> '{row['referenced_table']}' (ID: {row['referenced_table_id']})")

        # Convert back to DataFrame
        columns_df = pd.DataFrame(expanded_rows)

        # Reorder columns according to specified order: table_name, is_key, field_name, is_calculated
        if not columns_df.empty:
            # Define the desired column order
            desired_order = ['table_name', 'is_key', 'field_name', 'is_calculated']

            # Get remaining columns that aren't in the desired order
            remaining_cols = [col for col in columns_df.columns if col not in desired_order]

            # Create final column order
            final_order = desired_order + remaining_cols

            # Reorder the DataFrame
            columns_df = columns_df[final_order]

            logger.info(f"Reordered columns in specified order: {desired_order}")

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

                # Auto-adjust row heights based on text content
                for row in worksheet.iter_rows():
                    max_lines_in_row = 1
                    row_number = row[0].row

                    # Calculate the maximum number of lines needed in this row
                    for cell in row:
                        if cell.value is not None:
                            cell_text = str(cell.value)
                            column_width = worksheet.column_dimensions[cell.column_letter].width or 10

                            # Estimate lines needed based on text length and column width
                            # Roughly 1.2 characters per width unit in Excel
                            chars_per_line = max(int(column_width * 1.2), 10)
                            lines_needed = max(1, len(cell_text) // chars_per_line + (1 if len(cell_text) % chars_per_line else 0))

                            # Also count explicit line breaks
                            explicit_lines = cell_text.count('\n') + 1
                            lines_needed = max(lines_needed, explicit_lines)

                            max_lines_in_row = max(max_lines_in_row, lines_needed)

                    # Set row height based on content (minimum 20, with 15 points per line)
                    calculated_height = max(20, max_lines_in_row * 15)
                    worksheet.row_dimensions[row_number].height = calculated_height

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
