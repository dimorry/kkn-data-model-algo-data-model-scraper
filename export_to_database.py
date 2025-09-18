#!/usr/bin/env python3
"""
Export DuckDB tables content to knx_doc_expanded table with same structure as Excel export
"""
import duckdb
import pandas as pd
import logging
from pathlib import Path
from logger_config import LoggerConfig


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
        List of expanded field dictionaries
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

        # Create terminal expanded field
        # Extract the origin table name from the current path (the last table before the final field)
        path_parts = current_path.split('.')
        origin_table = path_parts[-2] if len(path_parts) >= 2 else 'Unknown'

        expanded_field = {
            'id': f"expanded_{current_path}",
            'table_id': root_field_props['table_id'],
            'table_name': root_field_props['table_name'],
            'field_name': f"    {current_path}",  # Add four spaces indentation
            'description': f"[From {origin_table}] {field_info.get('description', '')}",
            'data_type': field_info.get('data_type', ''),
            'is_key': root_field_props['is_key'],
            'is_calculated': field_info.get('is_calculated', False),
            'referenced_table': field_info.get('ref_referenced_table'),
            'display_on_export': root_field_props['display_on_export'],
            'created_at': root_field_props['created_at']
        }
        return [expanded_field]

    # This is a reference field, so expand it further
    expanded_fields = []
    new_visited = visited_tables.copy()
    new_visited.add(field_info['referenced_table_id'])

    logger.debug(f"Expanding reference field at path: {current_path}, referenced_table_id: {field_info['referenced_table_id']}")

    # Get display_on_export fields from the referenced table
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
        WHERE c.table_id = ? AND c.display_on_export = TRUE
        ORDER BY c.id
    """, [field_info['referenced_table_id']]).fetchdf()

    logger.debug(f"Found {len(ref_fields_df)} display_on_export fields for referenced table ID {field_info['referenced_table_id']}")

    # Recursively expand each display field
    for _, ref_field in ref_fields_df.iterrows():
        new_path = f"{current_path}.{ref_field['field_name']}"

        # Recursively expand this field
        sub_expanded = _expand_reference_recursively(
            con, ref_field.to_dict(), new_path, new_visited, max_depth - 1, root_field_props, logger
        )
        expanded_fields.extend(sub_expanded)

    return expanded_fields


def export_to_database(db_path="mappings.duckdb"):
    """Export columns data to knx_doc_expanded table with same structure as Excel export"""

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

        # Clear existing data from knx_doc_expanded table
        logger.info("Clearing existing data from knx_doc_expanded table...")
        con.execute("DELETE FROM knx_doc_expanded")
        logger.info("Cleared existing data from knx_doc_expanded table")

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
                c.display_on_export,
                c.created_at,
                c.referenced_table_id
            FROM knx_doc_columns c
            LEFT JOIN knx_doc_tables t ON c.table_id = t.id
            LEFT JOIN knx_doc_tables rt ON c.referenced_table_id = rt.id
            ORDER BY t.name,
                     CASE WHEN c.is_key = 'True' THEN 0 ELSE 1 END,
                     c.field_name
        """).fetchdf()

        logger.info(f"Found {len(base_columns_df)} base columns")

        # Step 2: Process each column and inject expanded references immediately after
        logger.info("Step 2: Processing columns and expanding references...")
        final_rows = []
        expanded_sequence = {}  # Track sequence numbers for each original ID

        for _, row in base_columns_df.iterrows():
            # Add the original row first
            final_rows.append(row.to_dict())

            # If this is a reference field, expand it and inject the results immediately after
            if (row['data_type'] and str(row['data_type']).lower().startswith('reference') and
                row['referenced_table_id'] is not None and not row['is_calculated']):

                logger.debug(f"[{row['table_name']}] Processing reference field '{row['field_name']}' -> '{row['referenced_table']}'")

                # Set up root field properties to inherit through recursion
                root_field_props = {
                    'table_id': row['table_id'],
                    'table_name': row['table_name'],
                    'is_key': row['is_key'],
                    'display_on_export': row['display_on_export'],
                    'created_at': row['created_at']
                }

                # Use recursive expansion with max depth of 5 levels
                initial_path = f"{row['table_name']}.{row['referenced_table']}"
                recursive_expanded = _expand_reference_recursively(
                    con, row.to_dict(), initial_path, set(), 5, root_field_props, logger
                )

                # Inject expanded fields immediately after the parent field
                for expanded_field in recursive_expanded:
                    # Generate decimal ID for expanded field
                    original_id = row['id']
                    if original_id not in expanded_sequence:
                        expanded_sequence[original_id] = 1
                    else:
                        expanded_sequence[original_id] += 1

                    sequence = expanded_sequence[original_id]
                    decimal_id = float(f"{original_id}.{sequence:06d}")

                    # Update the expanded field with decimal ID
                    expanded_field['id'] = decimal_id
                    final_rows.append(expanded_field)

                if recursive_expanded:
                    logger.info(f"[{row['table_name']}] Expanded reference field '{row['field_name']}' into {len(recursive_expanded)} fields")
                else:
                    logger.warning(f"[{row['table_name']}] No expansion results for reference field '{row['field_name']}' -> '{row['referenced_table']}'")

        # Convert to DataFrame
        columns_df = pd.DataFrame(final_rows)
        logger.info(f"Total rows after expansion: {len(columns_df)}")

        # Step 3: Final ordering and column arrangement
        if not columns_df.empty:
            # Define the desired column order
            desired_order = ['table_name', 'is_key', 'field_name', 'is_calculated']

            # Get remaining columns that aren't in the desired order
            remaining_cols = [col for col in columns_df.columns if col not in desired_order]

            # Create final column order
            final_order = desired_order + remaining_cols

            # Reorder the DataFrame
            columns_df = columns_df[final_order]

            logger.info(f"Applied column ordering: {desired_order}")

        logger.info(f"Ready to insert {len(columns_df)} total columns")

        # Step 4: Insert data into knx_doc_expanded table
        logger.info("Step 4: Inserting data into knx_doc_expanded table...")
        insert_count = 0

        for _, row in columns_df.iterrows():
            # Handle None values and ensure proper types
            def safe_value(val):
                if pd.isna(val):
                    return None
                return val

            # ID is already properly formatted (decimal IDs generated during expansion)
            id_value = safe_value(row['id'])

            # Insert row into knx_doc_expanded table
            con.execute("""
                INSERT INTO knx_doc_expanded (
                    id, table_id, table_name, field_name, description, data_type,
                    is_key, is_calculated, referenced_table, display_on_export,
                    created_at, referenced_table_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                safe_value(row['display_on_export']),
                safe_value(row['created_at']),
                safe_value(row['referenced_table_id'])
            ])

            insert_count += 1

            if insert_count % 100 == 0:
                logger.debug(f"Inserted {insert_count} rows so far...")

        # Commit all insertions
        con.commit()
        logger.info(f"Successfully inserted {insert_count} rows into knx_doc_expanded table")

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
        print("✅ Database export successful! Data inserted into knx_doc_expanded table.")
    else:
        print("❌ Database export failed. Check the logs for details.")


if __name__ == "__main__":
    main()