#!/usr/bin/env python3
"""
Export DuckDB tables content to Excel file with separate tabs
"""
import duckdb
import pandas as pd
import logging
from pathlib import Path
from logger_config import LoggerConfig

TARGET_TABLES = [
    "BillOfMaterial",
    "BOMAlternate",
    "Calendar",
    "CalendarDate",
    "Customer",
    "IndependentDemand",
    "OnHand",
    "Operation",    
    "Part",
    "PartCustomer",
    "PartSource",
    "ReferencePart",
    "Site",
    "ScheduledReceipt",
    "Source",
    "Supplier",
    "Routing",    
]



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

        # Prepare target table filters for ETN CDM export
        target_tables_upper = [name.upper() for name in TARGET_TABLES]

        # Query tables data
        logger.info("Querying tables data...")
        tables_query = """
            SELECT
                id,
                name as table_name,
                description,
                calculated_fields_description,
                created_at
            FROM knx_doc_tables
            ORDER BY name
        """
        logger.info("Exporting tables, columns, and mappings without target table filtering")
        tables_df = con.execute(tables_query).fetchdf()

        logger.info(f"Found {len(tables_df)} tables")

        # Query data directly from knx_doc_expanded table
        logger.info("Querying columns data from knx_doc_expanded table...")
        columns_query = """
            SELECT
                id, table_id, table_name, field_name, description, data_type,
                is_key, is_calculated, referenced_table, display_on_export,
                created_at, referenced_table_id
            FROM knx_doc_expanded
            ORDER BY display_order
        """
        columns_df = con.execute(columns_query).fetchdf()

        # Apply indentation based on decimal ID values
        logger.info("Applying indentation based on decimal ID values...")
        for idx, row in columns_df.iterrows():
            if pd.notna(row['id']) and isinstance(row['id'], (float, int)):
                # Remove any existing indentation first
                field_name = str(row['field_name']).lstrip()

                # Check if ID has decimal part (is expanded field)
                if row['id'] % 1 != 0:  # Has decimal part
                    # Add indentation (4 spaces) to field_name
                    columns_df.at[idx, 'field_name'] = f"    {field_name}"
                else:
                    # No indentation for non-decimal IDs
                    columns_df.at[idx, 'field_name'] = field_name

        logger.info(f"Found {len(columns_df)} columns")

        # Query ETN doc mappings data
        logger.info("Querying ETN doc mappings data...")
        mappings_query = """
            SELECT
                knx_table, original_tab, source_table, source_field,
                special_extract_logic, transformation_table_name, constant_value,
                target_table, target_field, example_value, notes, key,
                show_output, sort_output
            FROM etn_doc_mappings
            ORDER BY id
        """
        mappings_df = con.execute(mappings_query).fetchdf()

        logger.info(f"Found {len(mappings_df)} ETN doc mappings")

        # Query ETN CDM data
        logger.info("Querying ETN CDM data...")
        etn_match_statuses = ["ETN_ONLY", "MATCHED"]
        match_statuses_upper = [status.upper() for status in etn_match_statuses]
        logger.info("Will filter ETN CDM export to match statuses: %s (Maestro keys retained regardless of status)", ", ".join(etn_match_statuses))

        etn_cdm_query = """
            SELECT
                canonical_entity_name,
                maestro_table_name,
                maestro_table_description,
                canonical_attribute_name,
                maestro_field_name,
                erp_technical_table_name,
                erp_technical_field_name,
                maestro_field_description,
                maestro_data_type,
                maestro_is_key,
                information_only,
                standard_maestro_field,
                add_to_etl,
                default_value,
                example_value,
                erp_tcode,
                erp_screen_name,
                erp_screen_field_name,
                erp_technical_table_name_secondary,
                etl_logic,
                etl_transformation_table,
                notes,
                field_output_order,
                match_status,
                match_tier,
                match_details,
                sap_augmentation_strategy
            FROM etn_cdm
        """
        etn_cdm_df = con.execute(etn_cdm_query).fetchdf()

        logger.info(f"Found {len(etn_cdm_df)} ETN CDM records before applying filters")

        if TARGET_TABLES:
            logger.info(
                "Filtering ETN CDM export to target tables: %s and match statuses: %s (retaining Maestro keys regardless of status)",
                ", ".join(TARGET_TABLES),
                ", ".join(etn_match_statuses)
            )
            original_count = len(etn_cdm_df)
            canonical_match = etn_cdm_df['canonical_entity_name'].fillna('').str.upper().isin(target_tables_upper)

            status_match = (
                etn_cdm_df['match_status']
                .fillna('')
                .astype(str)
                .str.strip()
                .str.upper()
                .isin(match_statuses_upper)
            )

            maestro_key_flags = (
                etn_cdm_df['maestro_is_key']
                .fillna('')
                .astype(str)
                .str.strip()
                .str.upper()
                .isin({"TRUE", "YES", "Y", "1"})
            )

            filter_mask = canonical_match & (status_match | maestro_key_flags)
            etn_cdm_df = etn_cdm_df[filter_mask]

            key_only_retained = (canonical_match & maestro_key_flags & ~status_match).sum()
            logger.info(
                "Filtered ETN CDM records from %d to %d (retained %d Maestro key fields outside status filter)",
                original_count,
                len(etn_cdm_df),
                key_only_retained
            )

        etn_cdm_columns = {
            'canonical_entity_name': 'Canonical Entity Name',
            'maestro_table_name': 'Maestro Table Name',
            'maestro_table_description': 'Maestro Table Description',
            'canonical_attribute_name': 'Canonical Attribute Name',
            'maestro_field_name': 'Maestro Field Name',
            'erp_technical_table_name': 'ERP Technical Table Name',
            'maestro_field_description': 'Maestro Field Description',
            'maestro_data_type': 'Maestro Data Type',
            'maestro_is_key': 'Maestro Is Key',
            'information_only': 'Information Only',
            'standard_maestro_field': 'Standard Maestro Field',
            'add_to_etl': 'Add to ETL',
            'default_value': 'Default Value',
            'example_value': 'Example Value',
            'erp_tcode': 'ERP TCode',
            'erp_screen_name': 'ERP Screen Name',
            'erp_screen_field_name': 'ERP Screen Field Name',
            'erp_technical_field_name': 'ERP Technical Field Name',
            'erp_technical_table_name_secondary': 'ERP Technical Table Name Secondary',
            'etl_logic': 'ETL Logic',
            'etl_transformation_table': 'ETL Transformation Table',
            'notes': 'Notes',
            'field_output_order': 'Field Output Order',
            'match_status': 'Match Status',
            'match_tier': 'Match Tier',
            'match_details': 'Match Details',
            'sap_augmentation_strategy': 'SAP Augmentation Strategy',
        }

        etn_cdm_df = etn_cdm_df.rename(columns=etn_cdm_columns)

        # Create Excel writer with multiple sheets
        logger.info(f"Writing to Excel file: {output_file}")
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Write tables to first tab
            tables_df.to_excel(writer, sheet_name='Tables', index=False)
            logger.info("Written tables data to 'Tables' tab")

            # Write columns to second tab
            columns_df.to_excel(writer, sheet_name='knx_doc_extended', index=False)
            logger.info("Written columns data to 'knx_doc_extended' tab")

            # Write ETN doc mappings to third tab
            mappings_df.to_excel(writer, sheet_name='ETN_Mappings', index=False)
            logger.info("Written ETN doc mappings data to 'ETN_Mappings' tab")

            # Write ETN CDM data to fourth tab
            if not etn_cdm_df.empty:
                etn_cdm_df.to_excel(writer, sheet_name='ETN_CDM', index=False)
                logger.info("Written ETN CDM data to 'ETN_CDM' tab")
            else:
                empty_df = pd.DataFrame(columns=list(etn_cdm_columns.values()))
                empty_df.to_excel(writer, sheet_name='ETN_CDM', index=False)
                logger.info("Created empty 'ETN_CDM' tab (no data available)")

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

                # Hide ID columns in the knx_doc_extended sheet
                if sheet_name == 'knx_doc_extended':
                    id_columns_to_hide = ['id', 'table_id', 'referenced_table_id', 'display_on_export']

                    # Find and hide the ID columns
                    for col_idx, column in enumerate(worksheet.iter_cols(1, worksheet.max_column), 1):
                        column_header = column[0].value
                        if column_header and str(column_header).lower() in id_columns_to_hide:
                            col_letter = get_column_letter(col_idx)
                            worksheet.column_dimensions[col_letter].hidden = True
                            logger.debug(f"Hidden column '{column_header}' ({col_letter}) in {sheet_name} tab")


                # Freeze the header row (first row) for both worksheets
                worksheet.freeze_panes = 'A2'  # Freeze everything above row 2 (i.e., row 1)
                logger.debug(f"Froze header row in {sheet_name} tab")

        con.close()

        # Log summary
        logger.info("Excel export completed successfully!")
        logger.info(f"Output file: {output_file}")
        logger.info(f"Tables exported: {len(tables_df)}")
        logger.info(f"Columns exported: {len(columns_df)}")
        logger.info(f"ETN mappings exported: {len(mappings_df)}")

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
