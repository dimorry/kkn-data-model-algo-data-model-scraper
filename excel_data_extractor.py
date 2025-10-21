#!/usr/bin/env python3
"""
Excel Data Extractor for SAP ECC Kinaxis Integration Map
"""

import pandas as pd
import duckdb
import logging
from pathlib import Path
from typing import Dict, List, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ExcelDataExtractor:
    """
    Extracts data from SAP ECC Kinaxis Integration Map Excel file and loads it into DuckDB.
    """

    def __init__(self, excel_path: str, db_path: str = "mappings.duckdb"):
        """
        Initialize the Excel Data Extractor.

        Args:
            excel_path: Path to the Excel file
            db_path: Path to the DuckDB database
        """
        self.excel_path = Path(excel_path)
        self.db_path = db_path

        # Tab names to extract
        self.tab_names = [
            "Customer", "Part", "HistDmdActual_Ship", "HistoricalReceipt",
            "HistoricalSupplyActual", "Supplier", "PartSource_MatMstr", "Source",
            "BillOfMaterial", "Constraint", "ConstraintAvailable", "SourceConstraint",
            "IndDmd_Open", "OnHand", "SchdRcpt_PO", "Allocation_WO",
            "AggregatePartCustomer", "SP_PartCustomer"
        ]

        # Tab name mappings for knx_table column
        self.tab_mappings = {
            "HistDmdActual_Ship": "HistoricalDemandActual",
            "PartSource_MatMstr": "PartSource",
            "IndDmd_Open": "IndependentDemand",
            "SchdRcpt_PO": "ScheduledReceipt",
            "Allocation_WO": "Allocation",
            "SP_PartCustomer": "PartCustomer",
            "Operations": "SourceConstraint", 
        }

        # Wave implementation column variations to exclude
        self.wave_columns = [
            "Wave implementation", "Wave Implementation", "WAVE IMPLEMENTATION",
            "wave implementation", "Wave_implementation", "Wave_Implementation"
        ]

        # Specific columns to extract (case insensitive, handle newlines)
        self.target_columns = [
            "source table", "source field", "special extract logic", "constant value",
            "target table", "target field", "example value", "notes", "key",
            "show output", "sort output", "transformation table", "transformation table name"
        ]

    def _normalize_column_name(self, col_name: str) -> str:
        """
        Normalize column names for database storage.

        Args:
            col_name: Original column name

        Returns:
            Normalized column name
        """
        if pd.isna(col_name) or col_name == "":
            return "unnamed_column"

        # Convert to string and clean up
        normalized = str(col_name).strip()
        # Replace spaces and special characters with underscores
        normalized = normalized.replace(" ", "_").replace("-", "_").replace(".", "_")
        # Remove any remaining problematic characters
        normalized = "".join(c for c in normalized if c.isalnum() or c == "_")
        # Ensure it starts with a letter or underscore
        if normalized and not (normalized[0].isalpha() or normalized[0] == "_"):
            normalized = "col_" + normalized

        return normalized.lower() if normalized else "unnamed_column"

    def _get_mapped_table_name(self, tab_name: str) -> str:
        """
        Get the mapped table name for knx_table column.

        Args:
            tab_name: Original tab name

        Returns:
            Mapped table name
        """
        return self.tab_mappings.get(tab_name, tab_name)

    def _extract_tab_data(self, tab_name: str) -> Optional[pd.DataFrame]:
        """
        Extract data from a specific tab starting at row 8.

        Args:
            tab_name: Name of the tab to extract

        Returns:
            DataFrame with extracted data or None if extraction fails
        """
        try:
            logger.info(f"Extracting data from tab: {tab_name}")

            # Read the Excel tab starting from row 8 (index 7)
            df = pd.read_excel(self.excel_path, sheet_name=tab_name, header=7)

            logger.info(f"Read {len(df)} rows from {tab_name}")
            logger.debug(f"Columns in {tab_name}: {list(df.columns)}")

            # Filter rows where "Show Output" column has value "Y"
            show_output_mask = pd.Series([False] * len(df))

            # Look for "Show Output" column (case insensitive, handle newlines)
            show_output_col = None
            for col in df.columns:
                # Normalize column name by removing all whitespace and newlines
                normalized_col = str(col).replace('\n', ' ').replace('\r', ' ').strip().lower()
                normalized_col = ' '.join(normalized_col.split())  # Replace multiple spaces with single space

                if normalized_col in ['show output', 'showoutput', 'show_output']:
                    show_output_col = col
                    break

            if show_output_col is not None:
                # Filter rows where Show Output = Y (case insensitive)
                show_output_mask = df[show_output_col].astype(str).str.strip().str.upper() == 'Y'
                logger.info(f"Found 'Show Output' column: {show_output_col}")
            else:
                logger.warning(f"'Show Output' column not found in tab {tab_name}")
                # List available columns for debugging
                logger.debug(f"Available columns: {list(df.columns)}")

            # Keep rows where Show Output = Y
            filtered_df = df[show_output_mask].copy()

            logger.info(f"After filtering (Show Output = Y): {len(filtered_df)} rows")

            if filtered_df.empty:
                logger.warning(f"No data found in tab {tab_name} after filtering")
                return None

            # Select only target columns and exclude Wave implementation columns
            columns_to_keep = []
            original_columns = list(filtered_df.columns)

            # Map actual column names to target columns
            column_mapping = {}

            for col in original_columns:
                # Skip Wave implementation columns
                col_str = str(col).strip()
                if any(wave_col.lower() in col_str.lower() for wave_col in self.wave_columns):
                    logger.info(f"Excluding Wave implementation column: {col}")
                    continue

                # Normalize column name for comparison
                normalized_col = str(col).replace('\n', ' ').replace('\r', ' ').strip().lower()
                normalized_col = ' '.join(normalized_col.split())  # Replace multiple spaces with single space

                # Check if this column matches any of our target columns
                for target_col in self.target_columns:
                    if normalized_col == target_col:
                        columns_to_keep.append(col)
                        column_mapping[col] = target_col
                        logger.debug(f"Matched column: '{col}' -> '{target_col}'")
                        break

            if not columns_to_keep:
                logger.warning(f"No target columns found in tab {tab_name}")
                return None

            # Keep only the target columns
            filtered_df = filtered_df[columns_to_keep]
            logger.info(f"Selected {len(columns_to_keep)} target columns from {len(original_columns)} total columns")

            # Normalize column names using the target column mapping to ensure consistency
            normalized_columns = {}
            for col in filtered_df.columns:
                # Use the target column name if we have a mapping, otherwise normalize
                if col in column_mapping:
                    # Convert target column name to database format
                    target_col = column_mapping[col]
                    normalized = target_col.replace(" ", "_").lower()
                else:
                    normalized = self._normalize_column_name(col)
                normalized_columns[col] = normalized

            filtered_df = filtered_df.rename(columns=normalized_columns)

            # Add knx_table column with mapped table name
            mapped_table_name = self._get_mapped_table_name(tab_name)
            filtered_df['knx_table'] = mapped_table_name

            # Add original_tab column to track source tab
            filtered_df['original_tab'] = tab_name

            logger.info(f"Successfully extracted {len(filtered_df)} rows from {tab_name}")
            logger.debug(f"Final columns: {list(filtered_df.columns)}")

            return filtered_df

        except Exception as e:
            logger.error(f"Error extracting data from tab {tab_name}: {e}")
            return None

    def _create_table_if_not_exists(self, con: duckdb.DuckDBPyConnection, sample_df: pd.DataFrame):
        """
        Create the etn_doc_mappings table if it doesn't exist.

        Args:
            con: DuckDB connection
            sample_df: Sample DataFrame to infer schema
        """
        try:
            # Check if table exists and drop it to recreate with proper schema
            tables = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='etn_doc_mappings'").fetchall()

            if tables:
                logger.info("Dropping existing etn_doc_mappings table to recreate with proper schema")
                con.execute("DROP TABLE etn_doc_mappings")

            # Create table with dynamic schema based on all possible columns

            # Create table with dynamic schema based on all possible columns
            logger.info("Creating etn_doc_mappings table...")

            # Get all unique columns from all tabs first
            all_columns = set()
            for tab_name in self.tab_names:
                try:
                    temp_df = self._extract_tab_data(tab_name)
                    if temp_df is not None:
                        all_columns.update(temp_df.columns)
                except:
                    continue

            # Create table with VARCHAR for all columns (safest approach)
            column_defs = []
            for col in sorted(all_columns):
                if col not in ['knx_table', 'original_tab']:
                    column_defs.append(f"{col} VARCHAR")

            # Add our standard columns
            column_defs.extend([
                "knx_table VARCHAR",
                "original_tab VARCHAR",
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            ])

            # Create sequence and table separately for DuckDB
            con.execute("CREATE SEQUENCE IF NOT EXISTS etn_doc_mappings_id_seq")

            create_sql = f"""
                CREATE TABLE etn_doc_mappings (
                    id INTEGER PRIMARY KEY DEFAULT nextval('etn_doc_mappings_id_seq'),
                    {', '.join(column_defs)}
                )
            """

            con.execute(create_sql)
            logger.info("Successfully created etn_doc_mappings table")

        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def extract_all_data(self) -> bool:
        """
        Extract data from all specified tabs and load into database.

        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.excel_path.exists():
                logger.error(f"Excel file not found: {self.excel_path}")
                return False

            logger.info(f"Starting data extraction from {self.excel_path}")

            # Connect to DuckDB
            con = duckdb.connect(self.db_path)
            logger.info(f"Connected to database: {self.db_path}")

            # Extract data from all tabs
            all_data = []
            successful_tabs = []

            for tab_name in self.tab_names:
                tab_data = self._extract_tab_data(tab_name)
                if tab_data is not None:
                    all_data.append(tab_data)
                    successful_tabs.append(tab_name)
                    logger.info(f"✅ Successfully extracted {len(tab_data)} rows from {tab_name}")
                else:
                    logger.warning(f"❌ Failed to extract data from {tab_name}")

            if not all_data:
                logger.error("No data extracted from any tabs")
                return False

            # Combine all data into a single DataFrame
            logger.info("Combining data from all tabs...")
            combined_df = pd.concat(all_data, ignore_index=True, sort=False)

            # Fill NaN values with empty strings for database compatibility
            combined_df = combined_df.fillna("")

            logger.info(f"Combined data: {len(combined_df)} total rows from {len(successful_tabs)} tabs")

            # Create table if it doesn't exist
            self._create_table_if_not_exists(con, combined_df)

            # Clear existing data
            logger.info("Clearing existing data from etn_doc_mappings table...")
            con.execute("DELETE FROM etn_doc_mappings")

            # Insert data into database
            logger.info("Inserting data into etn_doc_mappings table...")

            # Get table columns (exclude id and created_at as they are auto-generated)
            table_info = con.execute("PRAGMA table_info(etn_doc_mappings)").fetchall()
            table_columns = [col[1] for col in table_info if col[1] not in ['id', 'created_at']]

            insert_count = 0
            batch_size = 100

            for i in range(0, len(combined_df), batch_size):
                batch_df = combined_df.iloc[i:i+batch_size].copy()

                # Ensure all table columns exist in the DataFrame
                for col in table_columns:
                    if col not in batch_df.columns:
                        batch_df[col] = ""

                # Select only the columns that exist in the table (excluding id and created_at)
                batch_df = batch_df[table_columns]

                # Create column list for INSERT statement
                column_list = ", ".join(table_columns)
                placeholders = ", ".join(["?" for _ in table_columns])

                # Insert batch row by row to handle any data type issues
                for _, row in batch_df.iterrows():
                    values = [str(row[col]) if pd.notna(row[col]) else "" for col in table_columns]
                    con.execute(f"INSERT INTO etn_doc_mappings ({column_list}) VALUES ({placeholders})", values)
                    insert_count += 1

                if insert_count % 500 == 0:
                    logger.info(f"Inserted {insert_count} rows so far...")

            con.commit()
            logger.info(f"Successfully inserted {insert_count} total rows")

            # Log summary by table
            logger.info("\n=== Extraction Summary ===")
            for tab_name in successful_tabs:
                mapped_name = self._get_mapped_table_name(tab_name)
                count = con.execute("SELECT COUNT(*) FROM etn_doc_mappings WHERE original_tab = ?", [tab_name]).fetchone()[0]
                logger.info(f"{tab_name} -> {mapped_name}: {count} rows")

            con.close()
            logger.info("Data extraction completed successfully!")
            return True

        except Exception as e:
            logger.error(f"Error during data extraction: {e}")
            return False


def main():
    """Main function to run the Excel data extraction."""
    excel_file = "SAPECC_Kinaxis Integration Map.xlsx"

    extractor = ExcelDataExtractor(excel_file)
    success = extractor.extract_all_data()

    if success:
        print("✅ Excel data extraction completed successfully!")
    else:
        print("❌ Excel data extraction failed!")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
