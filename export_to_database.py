#!/usr/bin/env python3
"""
Export DuckDB tables content to knx_doc_extended table with same structure as Excel export
"""
import duckdb
import pandas as pd
import logging
from pathlib import Path
from logger_config import LoggerConfig
from etn_cdm_upserter import EtnCdmUpserter


def _clean_description(value):
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def _expand_reference_recursively(con, field_info, current_path, visited_tables, max_depth, root_field_props, logger):
    """
    Recursively expand reference fields to build complete dotted paths

    Args:
        con: Database connection
        field_info: Dict with field information including referenced_table_id, data_type
        current_path: Current dotted path (e.g., "Table.Ref1.Ref2")
        visited_tables: Set of table IDs already visited (cycle detection)
        max_depth: Maximum recursion depth remaining
        root_field_props: Properties from the original reference field (is_key, display_on_export)
        logger: Logger instance

    Returns:
        List of extended field dictionaries
    """
    # Base cases
    if max_depth <= 0:
        logger.debug(f"Max depth reached for path: {current_path}")
        return []

    if field_info.get('referenced_table_id') in visited_tables:
        logger.debug(f"Cycle detected for path: {current_path}, skipping")
        return []

    # If this is not a reference field, return it as a terminal field
    if (not field_info.get('data_type') or
        not str(field_info['data_type']).lower().startswith('reference') or
        field_info.get('is_calculated') or
        not field_info.get('referenced_table_id')):

        # Create terminal extended field
        # Extract the origin table name from the current path (the last table before the final field)
        path_parts = current_path.split('.')
        origin_table = path_parts[-2] if len(path_parts) >= 2 else 'Unknown'

        # Remove the root table name from the display path
        if len(path_parts) > 1:
            display_segments = path_parts[1:]
        else:
            display_segments = path_parts[:]

        root_field_name = root_field_props.get('field_name')
        root_referenced_table = root_field_props.get('referenced_table')

        # Prefix the field name when it differs from the referenced table name
        if (root_field_name and root_referenced_table and
                root_field_name != root_referenced_table and
                (not display_segments or display_segments[0] != root_field_name)):
            display_segments.insert(0, root_field_name)

        display_path = '.'.join(display_segments)
        root_description = _clean_description(root_field_props.get('root_description'))
        origin_description = _clean_description(field_info.get('description'))
        origin_context = f"[From {origin_table}]"
        if origin_description:
            origin_context = f"{origin_context} {origin_description}"

        extended_field = {
            'id': f"extended_{current_path}",
            'table_id': root_field_props['table_id'],
            'table_name': root_field_props['table_name'],
            'field_name': f"    {display_path}",  # Add four spaces indentation
            'description': "\n\n".join(part for part in [root_description, origin_context] if part),
            'data_type': field_info.get('data_type', ''),
            'is_key': root_field_props['is_key'],
            'is_calculated': field_info.get('is_calculated', False),
            'referenced_table': field_info.get('ref_referenced_table'),
            'is_extended': True,
            'display_on_export': root_field_props['display_on_export'],
            'created_at': root_field_props['created_at']
        }
        return [extended_field]

    # This is a reference field, so expand it further
    extended_fields = []
    new_visited = visited_tables.copy()
    new_visited.add(field_info['referenced_table_id'])

    logger.debug(f"Expanding reference field at path: {current_path}, referenced_table_id: {field_info['referenced_table_id']}")

    # Get display_on_export fields and key fields from the referenced table
    ref_fields_df = con.execute("""
        SELECT
            c.field_name,
            c.description,
            c.data_type,
            c.is_key,
            c.is_calculated,
            c.referenced_table_id,
            rt2.name as ref_referenced_table
        FROM knx_doc_columns c
        LEFT JOIN knx_doc_tables rt2 ON c.referenced_table_id = rt2.id
        WHERE c.table_id = ? AND (c.display_on_export = TRUE OR c.is_key = 'True')
        ORDER BY
                CASE WHEN c.is_calculated THEN 1 ELSE 0 END,
                CASE WHEN LOWER(c.is_key) = 'yes' THEN 0 ELSE 1 END,
                c.field_name;
    """, [field_info['referenced_table_id']]).fetchdf()

    logger.debug(f"Found {len(ref_fields_df)} display_on_export and key fields for referenced table ID {field_info['referenced_table_id']}")

    # Recursively expand each display and key field
    for _, ref_field in ref_fields_df.iterrows():
        new_path = f"{current_path}.{ref_field['field_name']}"

        # Recursively expand this field
        sub_extended = _expand_reference_recursively(
            con, ref_field.to_dict(), new_path, new_visited, max_depth - 1, root_field_props, logger
        )
        extended_fields.extend(sub_extended)

    return extended_fields


def export_to_database(db_path="mappings.duckdb"):
    """Export columns data to knx_doc_extended table with same structure as Excel export"""

    # Setup logging
    logger_config = LoggerConfig(
        name="DatabaseExporter",
        log_level=logging.INFO,
        log_file="export_db.log"
    )
    logger = logger_config.get_logger()

    logger.info("Starting database export process")

    # Check if database exists
    if not Path(db_path).exists():
        logger.error(f"Database file {db_path} not found")
        return False

    try:
        # Connect to DuckDB
        con = duckdb.connect(db_path)
        logger.info(f"Connected to database: {db_path}")

        # Clear existing data from knx_doc_extended table
        logger.info("Clearing existing data from knx_doc_extended table...")
        con.execute("truncate table knx_doc_extended;")
        logger.info("Cleared existing data from knx_doc_extended table")

        # Step 1: Query base columns with proper ordering
        logger.info("Step 1: Querying base columns with proper ordering...")
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
                FALSE AS is_extended,
                (t.display_on_export AND c.display_on_export) as display_on_export,
                c.created_at,
                c.referenced_table_id
            FROM knx_doc_columns c
            LEFT JOIN knx_doc_tables t ON c.table_id = t.id
            LEFT JOIN knx_doc_tables rt ON c.referenced_table_id = rt.id
            ORDER BY t.name,
					CASE WHEN c.is_calculated THEN 1 ELSE 0 END,
                    CASE WHEN LOWER(c.is_key) = 'yes' THEN 0 ELSE 1 END,
                    c.field_name;
        """).fetchdf()

        logger.info(f"Found {len(base_columns_df)} base columns")

        # Step 2: Process each column and inject extended references immediately after
        logger.info("Step 2: Processing columns and expanding references...")
        final_rows = []
        extended_sequence = {}  # Track sequence numbers for each original ID

        for _, row in base_columns_df.iterrows():
            # Add the original row first
            base_row = row.to_dict()
            base_row['is_extended'] = bool(base_row.get('is_extended', False))
            final_rows.append(base_row)

            # If this is a reference field, expand it and inject the results immediately after
            if (row['data_type'] and str(row['data_type']).lower().startswith('reference') and
                row['referenced_table_id'] is not None and not row['is_calculated']):

                logger.debug(f"[{row['table_name']}] Processing reference field '{row['field_name']}' -> '{row['referenced_table']}'")

                # Normalize referenced table name for downstream processing
                referenced_table_name = row['referenced_table']
                if pd.isna(referenced_table_name):
                    referenced_table_name = None

                # Set up root field properties to inherit through recursion
                root_field_props = {
                    'table_id': row['table_id'],
                    'table_name': row['table_name'],
                    'is_key': row['is_key'],
                    'display_on_export': row['display_on_export'],
                    'created_at': row['created_at'],
                    'field_name': row['field_name'],
                    'referenced_table': referenced_table_name,
                    'root_description': row['description']
                }

                # Use recursive expansion with max depth of 5 levels
                # Use field name if referenced_table is None
                referenced_name = referenced_table_name if referenced_table_name is not None else row['field_name']
                initial_path = f"{row['table_name']}.{referenced_name}"
                recursive_extended = _expand_reference_recursively(
                    con, row.to_dict(), initial_path, set(), 5, root_field_props, logger
                )

                # Inject extended fields immediately after the parent field
                for extended_field in recursive_extended:
                    # Generate decimal ID for extended field
                    original_id = row['id']
                    if original_id not in extended_sequence:
                        extended_sequence[original_id] = 1
                    else:
                        extended_sequence[original_id] += 1

                    sequence = extended_sequence[original_id]
                    decimal_id = float(f"{original_id}.{sequence:06d}")

                    # Update the extended field with decimal ID
                    extended_field['id'] = decimal_id
                    final_rows.append(extended_field)

                if recursive_extended:
                    logger.info(f"[{row['table_name']}] Extended reference field '{row['field_name']}' into {len(recursive_extended)} fields")
                else:
                    logger.warning(f"[{row['table_name']}] No expansion results for reference field '{row['field_name']}' -> '{row['referenced_table']}'")

        # Convert to DataFrame
        columns_df = pd.DataFrame(final_rows)
        logger.info(f"Total rows after expansion: {len(columns_df)}")
        logger.info(f"Ready to insert {len(columns_df)} total columns")

        # Step 3: Insert data into knx_doc_extended table
        logger.info("Step 3: Inserting data into knx_doc_extended table...")
        insert_count = 0
        display_order = 0

        for _, row in columns_df.iterrows():
            # Handle None values and ensure proper types
            def safe_value(val):
                if pd.isna(val):
                    return None
                return val

            # ID is already properly formatted (decimal IDs generated during expansion)
            id_value = safe_value(row['id'])

            # Insert row into knx_doc_extended table
            display_order += 1
            con.execute("""
                INSERT INTO knx_doc_extended (
                    id, table_id, table_name, field_name, description, data_type,
                    is_key, is_calculated, referenced_table, is_extended, display_on_export,
                    created_at, referenced_table_id, display_order
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                id_value,
                safe_value(row['table_id']),
                safe_value(row['table_name']),
                safe_value(row['field_name']),
                safe_value(row['description']),
                safe_value(row['data_type']),
                safe_value(row['is_key']),
                safe_value(row['is_calculated']),
                safe_value(row['referenced_table']),
                safe_value(row.get('is_extended', False)),
                safe_value(row['display_on_export']),
                safe_value(row['created_at']),
                safe_value(row['referenced_table_id']),
                display_order
            ])

            insert_count += 1

            if insert_count % 100 == 0:
                logger.debug(f"Inserted {insert_count} rows so far...")

        # Commit all insertions
        con.commit()
        logger.info(f"Successfully inserted {insert_count} rows into knx_doc_extended table")

        # Run ETN CDM upsert leveraging the freshly exported data
        upserter_logger = logger.getChild("EtnCdmUpserter")
        etn_cdm_upserter = EtnCdmUpserter(db_path=db_path, logger=upserter_logger)
        etn_cdm_upserter.run(con)
        logger.info("ETN CDM upsert completed")

        con.close()

        # Log summary
        logger.info("Database export completed successfully!")
        logger.info(f"Rows inserted: {insert_count}")

        return True

    except Exception as e:
        logger.error(f"Database export failed: {e}")
        return False


def main():
    """Main function to run the database export"""
    print("DuckDB to Database Exporter")
    print("=" * 40)

    success = export_to_database()

    if success:
        print("✅ Database export successful! Data inserted into knx_doc_extended table.")
    else:
        print("❌ Database export failed. Check the logs for details.")


if __name__ == "__main__":
    main()
