#!/usr/bin/env python3
"""
Excel Data Extractor orchestrator for Kinaxis data mapping workflows.
"""

import logging
from pathlib import Path
from typing import Optional

from etn_doc_mapping_extractor import EtnDocMappingExtractor
from trl_augmentation_extractor import TrlAugmentationExtractor

# Configure root logging once for the CLI entrypoint
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ExcelDataExtractor:
    """
    High-level orchestrator that drives the individual extractors responsible for
    ETN doc mappings and Trillium augmentation tables.
    """

    def __init__(
        self,
        excel_path: str,
        db_path: str = "mappings.duckdb",
        augmentation_excel_path: Optional[str] = None
    ):
        self.excel_path = Path(excel_path)
        self.db_path = db_path
        self.augmentation_excel_path = Path(
            augmentation_excel_path or "kinaxis_tables_export.xlsx"
        )
        self.logger = logger.getChild("ExcelDataExtractor")

        self._etn_extractor = EtnDocMappingExtractor(
            excel_path=self.excel_path,
            db_path=self.db_path,
            logger=self.logger.getChild("EtnDocMappingExtractor")
        )

        self._trl_extractor = TrlAugmentationExtractor(
            augmentation_excel_path=self.augmentation_excel_path,
            db_path=self.db_path,
            logger=self.logger.getChild("TrlAugmentationExtractor")
        )

    def extract_all_data(self) -> bool:
        """
        Execute all configured extractors.

        Returns:
            True when the ETN doc mapping extraction succeeds; Trillium augmentation
            best-effort results are logged but do not affect the return value.
        """
        self.logger.info("Starting Excel data extraction process.")

        etn_result = self._etn_extractor.run()
        if etn_result.get("success"):
            self.logger.info("ETN doc mappings inserted: %d", etn_result["rows_inserted"])
            for tab, count in sorted(etn_result.get("tab_counts", {}).items()):
                mapped_tab = EtnDocMappingExtractor.TAB_MAPPINGS.get(tab, tab)
                self.logger.info("%s -> %s: %d rows", tab, mapped_tab, count)
        else:
            self.logger.error("ETN doc mappings extraction failed.")

        trl_result = self._trl_extractor.run()
        if trl_result.get("success"):
            self.logger.info(
                "Trillium augmentation rows inserted - doc: %d, cdm: %d",
                trl_result.get("doc_rows", 0),
                trl_result.get("cdm_rows", 0)
            )
        else:
            self.logger.info(
                "Trillium augmentation extraction skipped or produced no rows."
            )

        success = etn_result.get("success", False)
        if success:
            self.logger.info("Excel data extraction completed successfully.")
        else:
            self.logger.error("Excel data extraction encountered blocking errors.")
        return success


def main() -> int:
    """CLI entrypoint."""
    excel_file = "SAPECC_Kinaxis Integration Map.xlsx"

    extractor = ExcelDataExtractor(excel_file)
    success = extractor.extract_all_data()

    if success:
        print("✅ Excel data extraction completed successfully!")
        return 0

    print("❌ Excel data extraction failed!")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
