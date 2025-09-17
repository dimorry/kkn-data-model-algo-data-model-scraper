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
            # Log the absolute path to verify we're using the right database
            from pathlib import Path
            abs_path = Path(self.db_path).resolve()
            self.logger.info(f"Connecting to database at: {abs_path}")

            self.conn = duckdb.connect(self.db_path)
            self._create_tables()
            self.logger.info(f"Database initialized successfully at {abs_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise

    def _create_tables(self):
        """Create the main knx_doc_tables and knx_doc_columns tables"""
        # Create knx_doc_tables table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS knx_doc_tables (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                description TEXT,
                calculated_fields_description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create knx_doc_columns table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS knx_doc_columns (
                id INTEGER PRIMARY KEY,
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

        # Create knx_doc_expanded table with same columns as Excel export
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS knx_doc_expanded (
                id INTEGER PRIMARY KEY,
                table_id INTEGER,
                table_name VARCHAR,
                field_name VARCHAR,
                description TEXT,
                data_type VARCHAR,
                is_key VARCHAR,
                is_calculated BOOLEAN,
                referenced_table VARCHAR,
                display_on_export BOOLEAN,
                created_at TIMESTAMP,
                referenced_table_id INTEGER,
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
            # Get existing table ID or create new one
            existing_table = self.get_table_by_name(table_name)

            if existing_table:
                table_id = existing_table['id']
                action = "Updated"
            else:
                result = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM knx_doc_tables").fetchone()
                table_id = result[0]
                action = "Added"

            # Always do full merge - replace all table data
            self.conn.execute("""
                INSERT INTO knx_doc_tables (
                    id, name, description, calculated_fields_description, created_at
                ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    calculated_fields_description = EXCLUDED.calculated_fields_description
            """, [table_id, table_name, description, calculated_fields_description])

            self.logger.info(f"{action} table '{table_name}' with ID {table_id}")

            # Always do full merge for columns data if provided
            if columns_data:
                self._merge_columns_data(table_id, columns_data)

            self.conn.commit()
            return table_id

        except Exception as e:
            self.logger.error(f"Failed to insert/update table data: {e}")
            self.conn.rollback()
            raise


    def _merge_columns_data(self, table_id: int, new_columns_data: List[List[Any]]):
        """Merge new columns data with existing columns"""
        try:
            new_columns_count = 0
            for column_data in new_columns_data:

                if len(column_data) >= 5:
                    field_name = column_data[0] if len(column_data) > 0 else ""

                    if field_name:
                        description = column_data[1] if len(column_data) > 1 else ""
                        data_type = column_data[2] if len(column_data) > 2 else ""
                        is_key = column_data[3] if len(column_data) > 3 else ""
                        is_calculated = column_data[4] if len(column_data) > 4 else False
                        referenced_table_id = column_data[5] if len(column_data) > 5 else None
                        display_on_export = column_data[6] if len(column_data) > 6 else False

                        # Get existing column ID if it exists, otherwise get new ID
                        existing_result = self.conn.execute("""
                            SELECT id FROM knx_doc_columns
                            WHERE table_id = ? AND field_name = ?
                        """, [table_id, field_name]).fetchone()

                        if existing_result:
                            column_id = existing_result[0]
                            action = "Updated"
                        else:
                            # Get the next available column ID for new columns
                            result = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM knx_doc_columns").fetchone()
                            column_id = result[0]
                            action = "Added"
                            new_columns_count += 1

                        # Always do a full merge - replace all fields with new data
                        try:
                            self.conn.execute("""
                                INSERT INTO knx_doc_columns (
                                    id, table_id, field_name, description, data_type, is_key,
                                    is_calculated, referenced_table_id, display_on_export, created_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                                ON CONFLICT (id) DO UPDATE SET
                                    table_id = EXCLUDED.table_id,
                                    field_name = EXCLUDED.field_name,
                                    description = EXCLUDED.description,
                                    data_type = EXCLUDED.data_type,
                                    is_key = EXCLUDED.is_key,
                                    is_calculated = EXCLUDED.is_calculated,
                                    referenced_table_id = EXCLUDED.referenced_table_id,
                                    display_on_export = EXCLUDED.display_on_export
                            """, [column_id, table_id, field_name, description, data_type, is_key,
                                 is_calculated, referenced_table_id, display_on_export])

                        except Exception as sql_error:
                            self.logger.error(f"Failed to upsert column '{field_name}' in table {table_id}: {sql_error}")
                            raise

                        self.logger.debug(f"{action} column: {field_name}")

            if new_columns_count > 0:
                self.logger.info(f"Added {new_columns_count} new columns to table {table_id}")
            else:
                self.logger.info("No new columns to add")

        except Exception as e:
            self.logger.error(f"Failed to merge columns data: {e}")
            raise


    def get_table_by_name(self, table_name: str) -> Optional[Dict]:
        """Get table data by name (case-insensitive)"""
        try:
            result = self.conn.execute("""
                SELECT id, name, description, calculated_fields_description, created_at
                FROM knx_doc_tables WHERE LOWER(name) = LOWER(?)
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
        """Get table ID by name (case-insensitive)"""
        try:
            result = self.conn.execute("""
                SELECT id FROM knx_doc_tables WHERE LOWER(name) = LOWER(?)
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