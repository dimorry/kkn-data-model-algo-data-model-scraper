"""
Utilities for populating the knx_doc_extended table.
"""
import logging

import duckdb
import pandas as pd


class ExtendKnxDoc:
    """Encapsulates the logic required to populate knx_doc_extended."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, logger=None, max_depth: int = 5):
        self.con = connection
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.max_depth = max_depth

    def run(self) -> int:
        """Populate the knx_doc_extended table and return the number of inserted rows."""
        self._truncate_knx_doc_extended()
        base_columns_df = self._fetch_base_columns()
        columns_df = self._build_final_rows(base_columns_df)
        insert_count = self._insert_rows(columns_df)
        self.con.commit()
        return insert_count

    def _truncate_knx_doc_extended(self):
        self.logger.info("Clearing existing data from knx_doc_extended table...")
        self.con.execute("DELETE FROM knx_doc_extended;")
        self.logger.info("Cleared existing data from knx_doc_extended table")

    def _fetch_base_columns(self) -> pd.DataFrame:
        self.logger.info("Step 1: Querying base columns with proper ordering...")
        query = """
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
        """
        base_columns_df = self.con.execute(query).fetchdf()
        self.logger.info(f"Found {len(base_columns_df)} base columns")
        return base_columns_df

    def _build_final_rows(self, base_columns_df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Step 2: Processing columns and expanding references...")
        final_rows = []
        extended_sequence = {}

        for _, row in base_columns_df.iterrows():
            base_row = row.to_dict()
            base_row['is_extended'] = bool(base_row.get('is_extended', False))
            final_rows.append(base_row)

            if self._is_expandable_reference(row):
                self.logger.debug(f"[{row['table_name']}] Processing reference field '{row['field_name']}' -> '{row['referenced_table']}'")

                referenced_table_name = row['referenced_table']
                if pd.isna(referenced_table_name):
                    referenced_table_name = None

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

                referenced_name = referenced_table_name if referenced_table_name is not None else row['field_name']
                initial_path = f"{row['table_name']}.{referenced_name}"
                recursive_extended = self._expand_reference_recursively(
                    row.to_dict(), initial_path, set(), self.max_depth, root_field_props
                )

                for extended_field in recursive_extended:
                    original_id = row['id']
                    sequence = extended_sequence.get(original_id, 0) + 1
                    extended_sequence[original_id] = sequence
                    decimal_id = float(f"{original_id}.{sequence:06d}")
                    extended_field['id'] = decimal_id
                    final_rows.append(extended_field)

                if recursive_extended:
                    self.logger.info(f"[{row['table_name']}] Extended reference field '{row['field_name']}' into {len(recursive_extended)} fields")
                else:
                    self.logger.warning(f"[{row['table_name']}] No expansion results for reference field '{row['field_name']}' -> '{row['referenced_table']}'")

        columns_df = pd.DataFrame(final_rows)
        self.logger.info(f"Total rows after expansion: {len(columns_df)}")
        self.logger.info(f"Ready to insert {len(columns_df)} total columns")
        return columns_df

    def _insert_rows(self, columns_df: pd.DataFrame) -> int:
        self.logger.info("Step 3: Inserting data into knx_doc_extended table...")
        insert_count = 0
        display_order = 0

        for _, row in columns_df.iterrows():
            display_order += 1
            self.con.execute("""
                INSERT INTO knx_doc_extended (
                    id, table_id, table_name, field_name, description, data_type,
                    is_key, is_calculated, referenced_table, is_extended, display_on_export,
                    created_at, referenced_table_id, display_order
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                self._safe_value(row['id']),
                self._safe_value(row['table_id']),
                self._safe_value(row['table_name']),
                self._safe_value(row['field_name']),
                self._safe_value(row['description']),
                self._safe_value(row['data_type']),
                self._safe_value(row['is_key']),
                self._safe_value(row['is_calculated']),
                self._safe_value(row['referenced_table']),
                self._safe_value(row.get('is_extended', False)),
                self._safe_value(row['display_on_export']),
                self._safe_value(row['created_at']),
                self._safe_value(row['referenced_table_id']),
                display_order
            ])
            insert_count += 1

            if insert_count % 100 == 0:
                self.logger.debug(f"Inserted {insert_count} rows so far...")

        self.logger.info(f"Successfully inserted {insert_count} rows into knx_doc_extended table")
        return insert_count

    def _expand_reference_recursively(self, field_info, current_path, visited_tables, max_depth, root_field_props):
        if max_depth <= 0:
            self.logger.debug(f"Max depth reached for path: {current_path}")
            return []

        referenced_table_id = field_info.get('referenced_table_id')
        if referenced_table_id in visited_tables:
            self.logger.debug(f"Cycle detected for path: {current_path}, skipping")
            return []

        data_type = field_info.get('data_type')
        is_calculated = field_info.get('is_calculated')

        if (not data_type or
            not str(data_type).lower().startswith('reference') or
            is_calculated or
            not referenced_table_id):

            path_parts = current_path.split('.')
            origin_table = path_parts[-2] if len(path_parts) >= 2 else 'Unknown'
            display_segments = path_parts[1:] if len(path_parts) > 1 else path_parts[:]

            root_field_name = root_field_props.get('field_name')
            root_referenced_table = root_field_props.get('referenced_table')

            if (root_field_name and root_referenced_table and
                    root_field_name != root_referenced_table and
                    (not display_segments or display_segments[0] != root_field_name)):
                display_segments.insert(0, root_field_name)

            display_path = '.'.join(display_segments)
            root_description = self._clean_description(root_field_props.get('root_description'))
            origin_description = self._clean_description(field_info.get('description'))
            origin_context = f"[From {origin_table}]"
            if origin_description:
                origin_context = f"{origin_context} {origin_description}"

            return [{
                'id': f"extended_{current_path}",
                'table_id': root_field_props['table_id'],
                'table_name': root_field_props['table_name'],
                'field_name': f"    {display_path}",
                'description': "\n\n".join(
                    part for part in [root_description, origin_context] if part
                ),
                'data_type': field_info.get('data_type', ''),
                'is_key': root_field_props['is_key'],
                'is_calculated': field_info.get('is_calculated', False),
                'referenced_table': field_info.get('ref_referenced_table'),
                'is_extended': True,
                'display_on_export': root_field_props['display_on_export'],
                'created_at': root_field_props['created_at']
            }]

        extended_fields = []
        new_visited = visited_tables.copy()
        new_visited.add(referenced_table_id)

        self.logger.debug(f"Expanding reference field at path: {current_path}, referenced_table_id: {referenced_table_id}")

        ref_fields_df = self.con.execute("""
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
        """, [referenced_table_id]).fetchdf()

        self.logger.debug(f"Found {len(ref_fields_df)} display_on_export and key fields for referenced table ID {referenced_table_id}")

        for _, ref_field in ref_fields_df.iterrows():
            new_path = f"{current_path}.{ref_field['field_name']}"
            sub_extended = self._expand_reference_recursively(
                ref_field.to_dict(), new_path, new_visited, max_depth - 1, root_field_props
            )
            extended_fields.extend(sub_extended)

        return extended_fields

    def _is_expandable_reference(self, row) -> bool:
        if not row['data_type']:
            return False
        if not str(row['data_type']).lower().startswith('reference'):
            return False
        if row['referenced_table_id'] is None:
            return False
        if row['is_calculated']:
            return False
        return True

    @staticmethod
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

    @staticmethod
    def _safe_value(val):
        if pd.isna(val):
            return None
        return val
