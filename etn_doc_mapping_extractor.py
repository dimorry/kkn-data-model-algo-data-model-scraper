"""
Extractor for ETN doc mappings data sourced from the SAP ECC Kinaxis Integration Map workbook.
"""
import logging
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd


class EtnDocMappingExtractor:
    """Handles extracting ETN doc mappings tabs from Excel and loading them into DuckDB."""

    TAB_NAMES: List[str] = [
        "Customer", "Part", "HistDmdActual_Ship", "HistoricalReceipt",
        "HistoricalSupplyActual", "Supplier", "PartSource_MatMstr", "Source",
        "BillOfMaterial", "Constraint", "ConstraintAvailable", "SourceConstraint",
        "IndDmd_Open", "OnHand", "SchdRcpt_PO", "Allocation_WO",
        "AggregatePartCustomer", "SP_PartCustomer"
    ]

    TAB_MAPPINGS: Dict[str, str] = {
        "HistDmdActual_Ship": "HistoricalDemandActual",
        "PartSource_MatMstr": "PartSource",
        "IndDmd_Open": "IndependentDemand",
        "SchdRcpt_PO": "ScheduledReceipt",
        "Allocation_WO": "Allocation",
        "SP_PartCustomer": "PartCustomer",
        "Operations": "SourceConstraint",
    }

    WAVE_COLUMNS: List[str] = [
        "Wave implementation", "Wave Implementation", "WAVE IMPLEMENTATION",
        "wave implementation", "Wave_implementation", "Wave_Implementation"
    ]

    TARGET_COLUMNS: List[str] = [
        "source table", "source field", "special extract logic", "constant value",
        "target table", "target field", "example value", "notes", "key",
        "show output", "sort output", "transformation table", "transformation table name"
    ]

    def __init__(self, excel_path: Path, db_path: str = "mappings.duckdb", logger: Optional[logging.Logger] = None):
        self.excel_path = Path(excel_path)
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__ + ".EtnDocMappingExtractor")

    # --------------------------------------------------------------------- #
    # Helpers
    # --------------------------------------------------------------------- #
    def _normalize_column_name(self, col_name: str) -> str:
        if pd.isna(col_name) or col_name == "":
            return "unnamed_column"

        normalized = str(col_name).strip()
        normalized = normalized.replace(" ", "_").replace("-", "_").replace(".", "_")
        normalized = "".join(c for c in normalized if c.isalnum() or c == "_")
        if normalized and not (normalized[0].isalpha() or normalized[0] == "_"):
            normalized = "col_" + normalized
        return normalized.lower() if normalized else "unnamed_column"

    def _get_mapped_table_name(self, tab_name: str) -> str:
        return self.TAB_MAPPINGS.get(tab_name, tab_name)

    def _extract_tab_data(self, workbook: pd.ExcelFile, tab_name: str) -> Optional[pd.DataFrame]:
        try:
            self.logger.info("Extracting data from tab: %s", tab_name)
            df = workbook.parse(sheet_name=tab_name, header=7)
        except ValueError:
            self.logger.warning("Tab '%s' not found; skipping.", tab_name)
            return None
        except Exception as err:
            self.logger.error("Failed to read tab '%s': %s", tab_name, err)
            return None

        if df.empty:
            self.logger.warning("Tab '%s' is empty after load; skipping.", tab_name)
            return None

        show_output_mask = pd.Series([False] * len(df))
        show_output_col = None
        for col in df.columns:
            normalized_col = str(col).replace('\n', ' ').replace('\r', ' ').strip().lower()
            normalized_col = ' '.join(normalized_col.split())
            if normalized_col in {'show output', 'showoutput', 'show_output'}:
                show_output_col = col
                break

        if show_output_col is not None:
            show_output_mask = df[show_output_col].astype(str).str.strip().str.upper() == 'Y'
            self.logger.info("Found 'Show Output' column: %s", show_output_col)
        else:
            self.logger.warning("'Show Output' column not found in tab %s", tab_name)

        filtered_df = df[show_output_mask].copy()
        if filtered_df.empty:
            self.logger.warning("No rows flagged for output in tab %s", tab_name)
            return None

        columns_to_keep: List[str] = []
        column_mapping: Dict[str, str] = {}

        for col in filtered_df.columns:
            col_str = str(col).strip()
            if any(wave_col.lower() in col_str.lower() for wave_col in self.WAVE_COLUMNS):
                self.logger.info("Excluding Wave implementation column: %s", col)
                continue

            normalized_col = str(col).replace('\n', ' ').replace('\r', ' ').strip().lower()
            normalized_col = ' '.join(normalized_col.split())

            for target_col in self.TARGET_COLUMNS:
                if normalized_col == target_col:
                    columns_to_keep.append(col)
                    column_mapping[col] = target_col
                    break

        if not columns_to_keep:
            self.logger.warning("No target columns found in tab %s", tab_name)
            return None

        filtered_df = filtered_df[columns_to_keep]
        normalized_columns = {}
        for col in filtered_df.columns:
            if col in column_mapping:
                normalized_columns[col] = column_mapping[col].replace(" ", "_").lower()
            else:
                normalized_columns[col] = self._normalize_column_name(col)

        filtered_df = filtered_df.rename(columns=normalized_columns)
        filtered_df['knx_table'] = self._get_mapped_table_name(tab_name)
        filtered_df['original_tab'] = tab_name
        self.logger.info("Successfully extracted %d rows from %s", len(filtered_df), tab_name)
        return filtered_df

    def _create_table_if_not_exists(self, con: duckdb.DuckDBPyConnection, combined_df: pd.DataFrame):
        try:

            tables = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='etn_doc_mappings'"
            ).fetchall()
            if tables:
                self.logger.info("Dropping existing etn_doc_mappings table.")
                con.execute("DROP TABLE etn_doc_mappings")

            self.logger.info("Creating etn_doc_mappings table...")
            all_columns = [col for col in combined_df.columns if col not in {'knx_table', 'original_tab'}]

            column_defs = [f"{col} VARCHAR" for col in sorted(set(all_columns))]
            column_defs.extend([
                "knx_table VARCHAR",
                "original_tab VARCHAR",
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            ])

            con.execute("CREATE SEQUENCE IF NOT EXISTS etn_doc_mappings_id_seq")
            create_sql = f"""
                CREATE TABLE etn_doc_mappings (
                    id INTEGER PRIMARY KEY DEFAULT nextval('etn_doc_mappings_id_seq'),
                    {', '.join(column_defs)}
                )
            """
            con.execute(create_sql)
            self.logger.info("etn_doc_mappings table created.")

        except Exception as err:
            self.logger.error("Error creating etn_doc_mappings table: %s", err)
            raise

    # --------------------------------------------------------------------- #
    # Public API
    # --------------------------------------------------------------------- #
    def run(self) -> Dict[str, object]:
        """
        Execute the ETN doc mappings extraction.

        Returns:
            Dict with extraction metadata:
                {
                    "success": bool,
                    "rows_inserted": int,
                    "tab_counts": Dict[str, int]
                }
        """
        result = {
            "success": False,
            "rows_inserted": 0,
            "tab_counts": {}
        }

        if not self.excel_path.exists():
            self.logger.error("Excel file not found: %s", self.excel_path)
            return result

        try:
            workbook = pd.ExcelFile(self.excel_path)
        except Exception as err:
            self.logger.error("Unable to open workbook %s: %s", self.excel_path, err)
            return result

        try:
            con = duckdb.connect(self.db_path)
        except Exception as err:
            self.logger.error("Unable to connect to DuckDB at %s: %s", self.db_path, err)
            return result

        try:
            all_data: List[pd.DataFrame] = []
            tab_counts: Dict[str, int] = {}

            for tab_name in self.TAB_NAMES:
                tab_df = self._extract_tab_data(workbook, tab_name)
                if tab_df is not None:
                    all_data.append(tab_df)
                    tab_counts[tab_name] = len(tab_df)

            if not all_data:
                self.logger.error("No ETN doc mapping data extracted from any tabs.")
                return result

            combined_df = pd.concat(all_data, ignore_index=True, sort=False)
            combined_df = combined_df.fillna("")

            self._create_table_if_not_exists(con, combined_df)
            self.logger.info("Clearing existing data from etn_doc_mappings table...")
            con.execute("DELETE FROM etn_doc_mappings")

            table_info = con.execute("PRAGMA table_info(etn_doc_mappings)").fetchall()
            table_columns = [col[1] for col in table_info if col[1] not in {'id', 'created_at'}]

            insert_count = 0
            batch_size = 100
            for i in range(0, len(combined_df), batch_size):
                batch_df = combined_df.iloc[i:i + batch_size].copy()
                for col in table_columns:
                    if col not in batch_df.columns:
                        batch_df[col] = ""
                batch_df = batch_df[table_columns]

                column_list = ", ".join(table_columns)
                placeholders = ", ".join(["?" for _ in table_columns])

                for _, row in batch_df.iterrows():
                    values = [str(row[col]) if pd.notna(row[col]) else "" for col in table_columns]
                    con.execute(f"INSERT INTO etn_doc_mappings ({column_list}) VALUES ({placeholders})", values)
                    insert_count += 1

                if insert_count % 500 == 0:
                    self.logger.info("Inserted %d rows so far...", insert_count)

            con.commit()
            self.logger.info("Successfully inserted %d rows into etn_doc_mappings", insert_count)
            result.update({
                "success": True,
                "rows_inserted": insert_count,
                "tab_counts": tab_counts
            })
            return result

        except Exception as err:
            self.logger.error("ETN doc mapping extraction failed: %s", err)
            return result
        finally:
            con.close()
