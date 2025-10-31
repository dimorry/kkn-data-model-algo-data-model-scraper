"""
Extractor for Trillium augmentation workbook tabs.
"""
import logging
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd


class TrlAugmentationExtractor:
    """Loads Trillium augmentation metadata into DuckDB tables."""

    DOC_SHEET_NAMES = ["trl_doc_augmentation"]
    CDM_SHEET_NAMES = ["trl_cdm_augmentation", "trl_cdm_aumentation"]

    def __init__(
        self,
        augmentation_excel_path: Path,
        db_path: str = "mappings.duckdb",
        logger: Optional[logging.Logger] = None
    ):
        self.augmentation_excel_path = Path(augmentation_excel_path)
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__ + ".TrlAugmentationExtractor")

        self.doc_expected_columns = [
            "table_name",
            "field_name",
            "field_sub_domain",
            "field_view",
            "field_business_name",
            "sap_table",
            "sap_field",
            "trillium_comments"
        ]

        self.cdm_expected_columns = [
            "domain",
            "domain_description",
            "entity",
            "entity_description",
            "applications"
        ]

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _normalize_column_name(self, col: str) -> str:
        normalized = str(col).strip()
        normalized = normalized.replace(" ", "_").replace("-", "_").replace(".", "_")
        normalized = "".join(c for c in normalized if c.isalnum() or c == "_")
        if normalized and not normalized[0].isalpha():
            normalized = f"col_{normalized}"
        return normalized.lower() if normalized else "unnamed_column"

    def _load_sheet(self, workbook: pd.ExcelFile, sheet_names: List[str]) -> Optional[pd.DataFrame]:
        for name in sheet_names:
            try:
                df = workbook.parse(sheet_name=name)
                self.logger.info("Loaded sheet '%s' with %d rows", name, len(df))
                return df
            except ValueError:
                continue
            except Exception as err:
                self.logger.error("Failed to read sheet '%s': %s", name, err)
                return None
        self.logger.warning("None of sheets %s found in workbook.", sheet_names)
        return None

    def _prepare_doc_dataframe(
        self,
        doc_df: pd.DataFrame,
        con: duckdb.DuckDBPyConnection
    ) -> pd.DataFrame:
        doc_df = doc_df.rename(columns=lambda col: self._normalize_column_name(col))
        missing_cols = [col for col in self.doc_expected_columns if col not in doc_df.columns]
        if missing_cols:
            self.logger.error(
                "Doc augmentation sheet is missing expected columns: %s",
                ", ".join(missing_cols)
            )
            return pd.DataFrame()

        doc_df = doc_df[self.doc_expected_columns].copy()
        doc_df = doc_df.dropna(how="all")
        if doc_df.empty:
            self.logger.warning("Doc augmentation sheet is empty after cleanup.")
            return pd.DataFrame()

        lookup_df = con.execute("""
            SELECT id, table_name, field_name
            FROM knx_doc_extended
        """).fetchdf()

        if lookup_df.empty:
            self.logger.warning("knx_doc_extended is empty; cannot resolve augmentation rows.")
            return pd.DataFrame()

        doc_df['table_key'] = (
            doc_df['table_name'].fillna('').astype(str).str.strip().str.lower()
        )
        doc_df['field_key'] = (
            doc_df['field_name'].fillna('').astype(str).str.strip().str.lower()
        )

        lookup_df['table_key'] = (
            lookup_df['table_name'].fillna('').astype(str).str.strip().str.lower()
        )
        lookup_df['field_key'] = (
            lookup_df['field_name'].fillna('').astype(str).str.strip().str.lower()
        )

        merged_df = doc_df.merge(
            lookup_df[['id', 'table_key', 'field_key']],
            on=['table_key', 'field_key'],
            how='left',
            suffixes=('', '_lookup')
        )

        missing_mask = merged_df['id'].isna()
        if missing_mask.any():
            missing_rows = merged_df[missing_mask][['table_name', 'field_name']]
            self.logger.warning(
                "Unable to map %d doc augmentation rows to knx_doc_extended ids. Examples: %s",
                len(missing_rows),
                missing_rows.head(5).to_dict(orient='records')
            )
            merged_df = merged_df[~missing_mask]

        if merged_df.empty:
            self.logger.warning("No doc augmentation rows remain after resolving IDs.")
            return pd.DataFrame()

        merged_df = merged_df.drop(columns=['table_key', 'field_key'])
        merged_df = merged_df.rename(columns={'id': 'knx_doc_extended_id'})
        merged_df = merged_df.drop_duplicates(subset=['knx_doc_extended_id'])

        return merged_df

    def _prepare_cdm_dataframe(self, cdm_df: pd.DataFrame) -> pd.DataFrame:
        cdm_df = cdm_df.rename(columns=lambda col: self._normalize_column_name(col))
        missing_cols = [col for col in self.cdm_expected_columns if col not in cdm_df.columns]
        if missing_cols:
            self.logger.error(
                "CDM augmentation sheet is missing expected columns: %s",
                ", ".join(missing_cols)
            )
            return pd.DataFrame()

        cdm_df = cdm_df[self.cdm_expected_columns].copy()
        cdm_df = cdm_df.dropna(how="all")
        if cdm_df.empty:
            self.logger.warning("CDM augmentation sheet is empty after cleanup.")
            return pd.DataFrame()

        return cdm_df

    def _upsert_doc_table(self, con: duckdb.DuckDBPyConnection, data: pd.DataFrame) -> int:
        if data.empty:
            return 0

        con.execute("DELETE FROM trl_doc_augmentation")
        columns = ['knx_doc_extended_id'] + self.doc_expected_columns
        placeholders = ", ".join(["?" for _ in columns])
        column_list = ", ".join(columns)
        insert_sql = f"INSERT INTO trl_doc_augmentation ({column_list}) VALUES ({placeholders})"

        inserted = 0
        for _, row in data.iterrows():
            values = []
            for column in columns:
                value = row.get(column)
                if pd.isna(value) or value == "":
                    values.append(None)
                else:
                    values.append(value)
            con.execute(insert_sql, values)
            inserted += 1
        return inserted

    def _upsert_cdm_table(self, con: duckdb.DuckDBPyConnection, data: pd.DataFrame) -> int:
        if data.empty:
            return 0

        con.execute("DELETE FROM trl_cdm_augmentation")
        columns = self.cdm_expected_columns
        placeholders = ", ".join(["?" for _ in columns])
        column_list = ", ".join(columns)
        insert_sql = f"INSERT INTO trl_cdm_augmentation ({column_list}) VALUES ({placeholders})"

        inserted = 0
        for _, row in data.iterrows():
            values = []
            for column in columns:
                value = row.get(column)
                if pd.isna(value) or value == "":
                    values.append(None)
                else:
                    values.append(value)
            con.execute(insert_sql, values)
            inserted += 1
        return inserted

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def run(self) -> Dict[str, object]:
        """
        Perform the Trillium augmentation extraction.

        Returns:
            Dict with metadata:
            {
                "success": bool,
                "doc_rows": int,
                "cdm_rows": int
            }
        """
        result = {
            "success": False,
            "doc_rows": 0,
            "cdm_rows": 0
        }

        if not self.augmentation_excel_path.exists():
            self.logger.warning(
                "Augmentation workbook not found at %s. Skipping Trillium extraction.",
                self.augmentation_excel_path
            )
            return result

        try:
            workbook = pd.ExcelFile(self.augmentation_excel_path)
        except Exception as err:
            self.logger.error(
                "Unable to open augmentation workbook %s: %s",
                self.augmentation_excel_path, err
            )
            return result

        try:
            con = duckdb.connect(self.db_path)
        except Exception as err:
            self.logger.error("Unable to connect to DuckDB at %s: %s", self.db_path, err)
            return result

        try:
            doc_df_raw = self._load_sheet(workbook, self.DOC_SHEET_NAMES)
            cdm_df_raw = self._load_sheet(workbook, self.CDM_SHEET_NAMES)

            doc_rows = 0
            cdm_rows = 0

            if doc_df_raw is not None:
                prepared_doc = self._prepare_doc_dataframe(doc_df_raw, con)
                doc_rows = self._upsert_doc_table(con, prepared_doc)
                self.logger.info("Inserted %d rows into trl_doc_augmentation", doc_rows)

            if cdm_df_raw is not None:
                prepared_cdm = self._prepare_cdm_dataframe(cdm_df_raw)
                cdm_rows = self._upsert_cdm_table(con, prepared_cdm)
                self.logger.info("Inserted %d rows into trl_cdm_augmentation", cdm_rows)

            con.commit()
            result.update({
                "success": bool(doc_rows or cdm_rows),
                "doc_rows": doc_rows,
                "cdm_rows": cdm_rows
            })
            return result

        except Exception as err:
            self.logger.error("Trillium augmentation extraction failed: %s", err)
            return result
        finally:
            con.close()
