import duckdb
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any


class TableDatabase:
    def __init__(self, db_path: str = "mappings.duckdb", logger=None):
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__)
        self.conn = None
        self._initialize_database()

    def _initialize_database(self):
        """Initialize DuckDB connection and create tables"""
        try:
            self.conn = duckdb.connect(self.db_path)
            self._create_tables()
            self.logger.info(f"Database initialized at {self.db_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise

    def _create_tables(self):
        """Create the main knx_doc_tables and knx_doc_columns tables"""
        # Create knx_doc_tables table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS knx_doc_tables (
                id INTEGER,
                name VARCHAR,
                description TEXT,
                calculated_fields_description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create knx_doc_columns table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS knx_doc_columns (
                id INTEGER,
                table_id INTEGER,
                field_name VARCHAR,
                description TEXT,
                data_type VARCHAR,
                is_key VARCHAR,
                is_calculated BOOLEAN,
                referenced_table_id INTEGER,
                display_on_export BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (table_id) REFERENCES knx_doc_tables(id),
                FOREIGN KEY (referenced_table_id) REFERENCES knx_doc_tables(id)
            )
        """)

        self.conn.commit()
        self.logger.debug("Database tables created successfully")

    def insert_table_data(self, table_name: str, description: str = "",
                         calculated_fields_description: str = "",
                         columns_data: List[List[Any]] = None) -> int:
        """Insert or update table data and return the table ID"""
        try:
            # Check if table already exists
            existing_table = self.get_table_by_name(table_name)

            if existing_table:
                # Update existing table
                table_id = existing_table['id']
                self.logger.info(f"Table '{table_name}' already exists with ID {table_id}, updating...")

                # Update table metadata (merge descriptions if they're different)
                updated_description = self._merge_descriptions(existing_table['description'], description)
                updated_calc_desc = self._merge_descriptions(existing_table['calculated_fields_description'], calculated_fields_description)

                self.conn.execute("""
                    UPDATE knx_doc_tables
                    SET description = ?, calculated_fields_description = ?
                    WHERE id = ?
                """, [updated_description, updated_calc_desc, table_id])

                self.logger.info(f"Updated table '{table_name}' metadata")

                # Merge columns data if provided
                if columns_data:
                    self._merge_columns_data(table_id, columns_data)

            else:
                # Insert new table
                result = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM knx_doc_tables").fetchone()
                table_id = result[0]

                self.conn.execute("""
                    INSERT INTO knx_doc_tables (id, name, description, calculated_fields_description)
                    VALUES (?, ?, ?, ?)
                """, [table_id, table_name, description, calculated_fields_description])

                self.logger.info(f"Inserted new table '{table_name}' with ID {table_id}")

                # Insert columns data if provided
                if columns_data:
                    self._insert_columns_data(table_id, columns_data)

            self.conn.commit()
            return table_id

        except Exception as e:
            self.logger.error(f"Failed to insert/update table data: {e}")
            self.conn.rollback()
            raise

    def _merge_descriptions(self, existing: str, new: str) -> str:
        """Merge two descriptions, preferring non-empty content"""
        existing = existing or ""
        new = new or ""

        # If new description is longer or existing is empty, use new
        if not existing or len(new) > len(existing):
            return new

        # Otherwise keep existing
        return existing

    def _merge_columns_data(self, table_id: int, new_columns_data: List[List[Any]]):
        """Merge new columns data with existing columns"""
        try:
            # Get existing columns
            existing_columns = self.get_columns_for_table(table_id)
            existing_field_names = {col['field_name'] for col in existing_columns}

            # Only insert columns that don't already exist
            new_columns_count = 0
            for column_data in new_columns_data:
                if len(column_data) >= 5:
                    field_name = column_data[0] if len(column_data) > 0 else ""

                    if field_name and field_name not in existing_field_names:
                        # Get the next available column ID
                        result = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM knx_doc_columns").fetchone()
                        column_id = result[0]

                        description = column_data[1] if len(column_data) > 1 else ""
                        data_type = column_data[2] if len(column_data) > 2 else ""
                        is_key = column_data[3] if len(column_data) > 3 else ""
                        is_calculated = column_data[4] if len(column_data) > 4 else False
                        referenced_table_id = column_data[5] if len(column_data) > 5 else None
                        display_on_export = column_data[6] if len(column_data) > 6 else False

                        self.conn.execute("""
                            INSERT INTO knx_doc_columns (
                                id, table_id, field_name, description, data_type, is_key,
                                is_calculated, referenced_table_id, display_on_export
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
                            column_id, table_id, field_name, description, data_type, is_key,
                            is_calculated, referenced_table_id, display_on_export
                        ])

                        new_columns_count += 1
                        self.logger.debug(f"Added new column: {field_name} (calculated: {is_calculated})")
                    else:
                        self.logger.debug(f"Column '{field_name}' already exists, skipping")

            if new_columns_count > 0:
                self.logger.info(f"Added {new_columns_count} new columns to table {table_id}")
            else:
                self.logger.info("No new columns to add")

        except Exception as e:
            self.logger.error(f"Failed to merge columns data: {e}")
            raise

    def _insert_columns_data(self, table_id: int, columns_data: List[List[Any]]):
        """Insert columns data for a table"""
        for column_data in columns_data:
            # Ensure we have at least the required fields
            if len(column_data) >= 5:
                # Get the next available column ID
                result = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM knx_doc_columns").fetchone()
                column_id = result[0]

                field_name = column_data[0] if len(column_data) > 0 else ""
                description = column_data[1] if len(column_data) > 1 else ""
                data_type = column_data[2] if len(column_data) > 2 else ""
                is_key = column_data[3] if len(column_data) > 3 else ""
                is_calculated = column_data[4] if len(column_data) > 4 else False
                referenced_table_id = column_data[5] if len(column_data) > 5 else None
                display_on_export = column_data[6] if len(column_data) > 6 else False

                self.conn.execute("""
                    INSERT INTO knx_doc_columns (
                        id, table_id, field_name, description, data_type, is_key,
                        is_calculated, referenced_table_id, display_on_export
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    column_id, table_id, field_name, description, data_type, is_key,
                    is_calculated, referenced_table_id, display_on_export
                ])

                self.logger.debug(f"Inserted column: {field_name} (calculated: {is_calculated})")

    def get_table_by_name(self, table_name: str) -> Optional[Dict]:
        """Get table data by name"""
        try:
            result = self.conn.execute("""
                SELECT id, name, description, calculated_fields_description, created_at
                FROM knx_doc_tables WHERE name = ?
            """, [table_name]).fetchone()

            if result:
                return {
                    'id': result[0],
                    'name': result[1],
                    'description': result[2],
                    'calculated_fields_description': result[3],
                    'created_at': result[4]
                }
            return None

        except Exception as e:
            self.logger.error(f"Failed to get table by name: {e}")
            return None

    def get_table_id_by_name(self, table_name: str) -> Optional[int]:
        """Get table ID by name"""
        try:
            result = self.conn.execute("""
                SELECT id FROM knx_doc_tables WHERE name = ?
            """, [table_name]).fetchone()

            return result[0] if result else None

        except Exception as e:
            self.logger.error(f"Failed to get table ID by name: {e}")
            return None

    def get_columns_for_table(self, table_id: int) -> List[Dict]:
        """Get all columns for a table"""
        try:
            results = self.conn.execute("""
                SELECT field_name, description, data_type, is_key, is_calculated, referenced_table_id, display_on_export
                FROM knx_doc_columns WHERE table_id = ?
                ORDER BY id
            """, [table_id]).fetchall()

            columns = []
            for row in results:
                columns.append({
                    'field_name': row[0],
                    'description': row[1],
                    'data_type': row[2],
                    'is_key': row[3],
                    'is_calculated': row[4],
                    'referenced_table_id': row[5],
                    'display_on_export': row[6]
                })

            return columns

        except Exception as e:
            self.logger.error(f"Failed to get columns for table: {e}")
            return []

    def list_all_tables(self) -> List[Dict]:
        """List all tables in the database"""
        try:
            results = self.conn.execute("""
                SELECT id, name, description, calculated_fields_description, created_at
                FROM knx_doc_tables ORDER BY created_at DESC
            """).fetchall()

            tables = []
            for row in results:
                tables.append({
                    'id': row[0],
                    'name': row[1],
                    'description': row[2],
                    'calculated_fields_description': row[3],
                    'created_at': row[4]
                })

            return tables

        except Exception as e:
            self.logger.error(f"Failed to list tables: {e}")
            return []

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")