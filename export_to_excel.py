#!/usr/bin/env python3
"""
Export DuckDB tables content to Excel file with separate tabs
"""
import duckdb
import pandas as pd
import logging
import math
from pathlib import Path
from logger_config import LoggerConfig

TARGET_TABLES = [
    "Allocation",
    "AlternatePart",
    "BillOfMaterial",
    "BOMAlternate",
    "BuyerCode",
    "Calendar",
    "CalendarDate",
    "Constraint",
    "ConstrainAvailable",
    "Customer",
    "DemandOrder",
    "HistoricalDemandHeader",
    "HistoricalDemandActual",
    "HistoricalReceipt",
    "HistoricalReceiptHeader",
    "HistoricalSupplyHeader",
    "HistoricalSupplyActual",
    "IndependentDemand",
    "OnHand",
    "Operation",    
    "Part",
    "PartCustomer",
    "PartSource",
    "PartSupplier",
    "PlannerCode",
    "ReferencePart",
    "Site",
    "ScheduledReceipt",
    "Source",
    "SourceConstraint",
    "Supplier",
    "SupplyOrder",
    "Routing", 
]

FIELD_CATEGORY_METADATA = [
    (
        "Identifier",
        "Id or part of the Id to uniquely identify a record (ex. Sales Order and Sales Order Line)",
    ),
    (
        "Critical",
        "These elements perform a core function (in a maestro context, missing/incorrect data in these fields leads to junk/no results) (ex. Purchase Order Confirmation Date)",
    ),
    (
        "Functional Enabler",
        "Element is not needed for core functionality, but if it is missing, some capability(s) will not function properly (ex. Part ABC/XYZ indicator)",
    ),
    (
        "Optional/Reference",
        "Reference data helps users understand context or makes filtering and grouping easier, but otherwise does not impact function (ex. Supplier name)",
    ),
]

CRITICAL_NAME_SUBSTRINGS = ("Name", "Site", "Date", "LeadTime", "Number")



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
        table_desc_lookup = {}
        if not tables_df.empty:
            table_desc_lookup = {
                str(name).strip().upper(): desc
                for name, desc in zip(tables_df['table_name'], tables_df['description'])
                if pd.notna(name)
            }
            logger.info("Prepared table description lookup for %d tables", len(table_desc_lookup))
        else:
            logger.warning("No table metadata available to build Maestro description lookup")

        # Query data directly from knx_doc_extended table
        logger.info("Querying columns data from knx_doc_extended table...")
        columns_query = """
            SELECT
                id, table_id, table_name, field_name, description, data_type,
                is_key, is_calculated, referenced_table, is_extended, display_on_export,
                created_at, referenced_table_id
            FROM knx_doc_extended
            ORDER BY display_order
        """
        columns_df = con.execute(columns_query).fetchdf()
        raw_columns_df = columns_df.copy()

        # Apply indentation based on decimal ID values
        logger.info("Applying indentation based on decimal ID values...")
        for idx, row in columns_df.iterrows():
            if pd.notna(row['id']) and isinstance(row['id'], (float, int)):
                # Remove any existing indentation first
                field_name = str(row['field_name']).lstrip()

                # Check if ID has decimal part (is extended field)
                if row['id'] % 1 != 0:  # Has decimal part
                    # Add indentation (4 spaces) to field_name
                    columns_df.at[idx, 'field_name'] = f"    {field_name}"
                else:
                    # No indentation for non-decimal IDs
                    columns_df.at[idx, 'field_name'] = field_name

        logger.info(f"Found {len(columns_df)} columns")

        # Query Trillium doc augmentation data
        logger.info("Querying trl_doc_augmentation data...")
        comments_query = """
            SELECT
                k.table_name,
                k.field_name,
                c.field_sub_domain,
                c.field_view,
                c.field_business_name,
                c.sap_table,
                c.sap_field,
                c.trillium_comments
            FROM trl_doc_augmentation AS c
            INNER JOIN knx_doc_extended AS k
                ON c.table_name = k.table_name
                AND c.field_name = k.field_name
            ORDER BY k.table_name, k.field_name
        """
        try:
            comments_df = con.execute(comments_query).fetchdf()
            logger.info("Found %d trl_doc_augmentation records", len(comments_df))
        except duckdb.CatalogException:
            logger.warning("trl_doc_augmentation table not found; skipping comments export")
            comments_df = pd.DataFrame(columns=[
                'table_name', 'field_name', 'field_sub_domain', 'field_view',
                'field_business_name', 'sap_table', 'sap_field', 'trillium_comments'
            ])

        # Query Trillium CDM augmentation data
        logger.info("Querying trl_cdm_augmentation data...")
        try:
            cdm_augmentation_df = con.execute("""
                SELECT
                    domain,
                    domain_description,
                    entity,
                    entity_description,
                    applications
                FROM trl_cdm_augmentation
                ORDER BY domain, entity
            """).fetchdf()
            logger.info("Found %d trl_cdm_augmentation records", len(cdm_augmentation_df))
        except duckdb.CatalogException:
            logger.warning("trl_cdm_augmentation table not found; skipping CDM augmentation export")
            cdm_augmentation_df = pd.DataFrame(columns=[
                'domain', 'domain_description', 'entity', 'entity_description', 'applications'
            ])

        # Build lookup for key fields from target tables
        target_tables_upper_set = set(target_tables_upper)
        key_flags = (
            raw_columns_df['is_key']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({'yes', 'true', 'y', '1'})
        )
        key_columns_df = raw_columns_df[
            raw_columns_df['table_name'].fillna('').astype(str).str.upper().isin(target_tables_upper_set) & key_flags
        ].copy()
        key_columns_df['table_name_upper'] = key_columns_df['table_name'].astype(str).str.upper()
        key_columns_df['field_name_upper'] = key_columns_df['field_name'].astype(str).str.strip().str.upper()
        key_field_lookup = set(zip(key_columns_df['table_name_upper'], key_columns_df['field_name_upper']))
        logger.info("Identified %d target table key fields from knx_doc_extended", len(key_field_lookup))

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
                domain_name,
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
        if table_desc_lookup and 'maestro_table_name' in etn_cdm_df.columns:
            maestro_table_normalized = (
                etn_cdm_df['maestro_table_name']
                .fillna('')
                .astype(str)
                .str.strip()
                .str.upper()
            )
            existing_desc = (
                etn_cdm_df['maestro_table_description']
                .fillna('')
                .astype(str)
                .str.strip()
            )
            missing_desc_mask = existing_desc.eq('')
            mapped_descriptions = maestro_table_normalized.map(table_desc_lookup)
            fill_mask = missing_desc_mask & mapped_descriptions.notna()
            if fill_mask.any():
                etn_cdm_df.loc[fill_mask, 'maestro_table_description'] = mapped_descriptions[fill_mask]
                logger.info(
                    "Filled Maestro table descriptions for %d ETN CDM records using knx_doc_tables metadata",
                    int(fill_mask.sum())
                )

        # Guarantee Maestro key flags from knx_doc_extended mapping
        if key_field_lookup:
            canonical_upper = etn_cdm_df['canonical_entity_name'].fillna('').astype(str).str.upper()
            maestro_field_upper = etn_cdm_df['maestro_field_name'].fillna('').astype(str).str.strip().str.upper()
            key_mask = pd.Series(
                [(entity, field) in key_field_lookup for entity, field in zip(canonical_upper, maestro_field_upper)],
                index=etn_cdm_df.index
            )
            key_updates = int(key_mask.sum())
            if key_updates:
                etn_cdm_df.loc[key_mask, 'maestro_is_key'] = True
                logger.info(
                    "Applied Maestro key flag to %d ETN CDM records based on knx_doc_extended keys",
                    key_updates
                )

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
            'domain_name': 'Domain Name',
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

        if 'Domain Name' in etn_cdm_df.columns:
            reordered_columns = ['Domain Name'] + [col for col in etn_cdm_df.columns if col != 'Domain Name']
            etn_cdm_df = etn_cdm_df[reordered_columns]

        if 'Maestro Is Key' in etn_cdm_df.columns:
            maestro_key_series = (
                etn_cdm_df['Maestro Is Key']
                .fillna('')
                .astype(str)
                .str.strip()
                .str.lower()
            )
            etn_cdm_df['Maestro Is Key'] = maestro_key_series.isin({'true', 'yes', 'y', '1', 't'})

        sort_columns = ['Maestro Table Name', 'Maestro Is Key', 'Maestro Field Name']
        missing_sort_cols = [col for col in sort_columns if col not in etn_cdm_df.columns]
        if missing_sort_cols:
            logger.warning("Unable to apply full ETN CDM sorting; missing columns: %s", ", ".join(missing_sort_cols))
        else:
            etn_cdm_df = etn_cdm_df.sort_values(
                by=['Maestro Table Name', 'Maestro Is Key', 'Maestro Field Name'],
                ascending=[True, False, True],
                kind='mergesort'
            )
            etn_cdm_df.reset_index(drop=True, inplace=True)
            logger.info("Ordered ETN CDM records by Maestro table name, key flag, then field name")

        def determine_field_category(row):
            maestro_is_key = bool(row.get('Maestro Is Key', False))
            if maestro_is_key:
                return "Identifier"

            match_status_raw = row.get('Match Status', '')
            match_status = (match_status_raw or '').strip().upper()

            if match_status == 'ETN_ONLY':
                return "Optional/Reference"

            maestro_field_name = row.get('Maestro Field Name') or ''
            if any(substring in maestro_field_name for substring in CRITICAL_NAME_SUBSTRINGS) or 'LT' in maestro_field_name:
                return "Critical"

            if match_status == 'MATCHED':
                return "Functional Enabler"

            return None

        if not etn_cdm_df.empty:
            etn_cdm_df['Field Category'] = etn_cdm_df.apply(determine_field_category, axis=1)
            logger.info("Assigned Field Category values to ETN CDM records")

        field_category_df = pd.DataFrame(FIELD_CATEGORY_METADATA, columns=['Category Name', 'Description'])

        # Create Excel writer with multiple sheets
        logger.info(f"Writing to Excel file: {output_file}")
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Write tables to first tab
            tables_df.to_excel(writer, sheet_name='knx_doc_tables', index=False)
            logger.info("Written tables data to 'knx_doc_tables' tab")

            # Write columns to second tab
            columns_df.to_excel(writer, sheet_name='knx_doc_extended', index=False)
            logger.info("Written columns data to 'knx_doc_extended' tab")

            # Write ETN doc mappings to third tab
            mappings_df.to_excel(writer, sheet_name='etn_doc_mappings', index=False)
            logger.info("Written ETN doc mappings data to 'etn_doc_mappings' tab")

            # Write ETN CDM data to fourth tab
            if not etn_cdm_df.empty:
                etn_cdm_df.to_excel(writer, sheet_name='ETN_CDM', index=False)
                logger.info("Written ETN CDM data to 'ETN_CDM' tab")
            else:
                empty_df = pd.DataFrame(columns=list(etn_cdm_columns.values()))
                empty_df.to_excel(writer, sheet_name='ETN_CDM', index=False)
                logger.info("Created empty 'ETN_CDM' tab (no data available)")

            field_category_df.to_excel(writer, sheet_name='Field Category', index=False)
            logger.info("Written Field Category metadata to 'Field Category' tab")

            # Write Trillium comments linked to KNX doc extended
            if not comments_df.empty:
                comments_df.to_excel(writer, sheet_name='trl_doc_augmentation', index=False)
                logger.info("Written Trillium comments to 'trl_doc_augmentation' tab")
            else:
                comments_df.to_excel(writer, sheet_name='trl_doc_augmentation', index=False)
                logger.info("Created empty 'trl_doc_augmentation' tab (no data available)")

            if not cdm_augmentation_df.empty:
                cdm_augmentation_df.to_excel(writer, sheet_name='trl_cdm_augmentation', index=False)
                logger.info("Written Trillium CDM augmentation data to 'trl_cdm_augmentation' tab")
            else:
                cdm_augmentation_df.to_excel(writer, sheet_name='trl_cdm_augmentation', index=False)
                logger.info("Created empty 'trl_cdm_augmentation' tab (no data available)")

            # Format worksheets with text wrapping and column sizing
            from openpyxl.styles import Alignment

            def estimate_lines(cell_text, column_width):
                if cell_text is None:
                    return 1
                text = str(cell_text)
                if not text:
                    return 1
                approx_chars = max(int((column_width or 10) * 0.9), 10)
                total_lines = 0
                for raw_line in text.splitlines() or ['']:
                    segment = raw_line
                    if not segment.strip():
                        total_lines += 1
                        continue
                    total_lines += max(1, math.ceil(len(segment) / approx_chars))
                return max(total_lines, text.count('\n') + 1)

            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]

                # Auto-adjust column widths and apply text wrapping
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    column_header = column[0].value

                    # Check if this is a description column
                    header_lower = str(column_header).strip().lower() if column_header else ""
                    is_description_column = ('description' in header_lower)
                    is_field_name_column = (header_lower == 'field_name')

                    for cell in column:
                        # Apply text wrapping to all cells
                        cell.alignment = Alignment(wrap_text=True, vertical='top')

                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass

                    # Set column width based on content type
                    if sheet_name == 'knx_doc_extended' and is_field_name_column:
                        adjusted_width = 60
                        worksheet.column_dimensions[column_letter].width = adjusted_width
                    elif is_description_column:
                        # Description columns get wider width and taller rows
                        adjusted_width = min(max_length + 2, 80)  # Wider for descriptions
                        worksheet.column_dimensions[column_letter].width = adjusted_width
                    else:
                        # Regular columns get standard width
                        adjusted_width = min(max_length + 2, 30)
                        worksheet.column_dimensions[column_letter].width = adjusted_width

                # Auto-adjust row heights based on text content
                try:
                    header_row = next(worksheet.iter_rows(min_row=1, max_row=1))
                except StopIteration:
                    header_row = []
                description_columns = {
                    cell.column_letter
                    for cell in header_row
                    if cell.value and 'description' in str(cell.value).strip().lower()
                }

                for row in worksheet.iter_rows():
                    max_lines_in_row = 1
                    row_number = row[0].row

                    # Calculate the maximum number of lines needed in this row
                    for cell in row:
                        if cell.value is not None:
                            cell_text = str(cell.value)
                            column_width = worksheet.column_dimensions[cell.column_letter].width or 10

                            # Estimate lines needed with preference for description columns
                            if sheet_name == 'knx_doc_extended' and cell.column_letter in description_columns:
                                lines_needed = estimate_lines(cell_text, column_width)
                            else:
                                lines_needed = estimate_lines(cell_text, column_width)

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
