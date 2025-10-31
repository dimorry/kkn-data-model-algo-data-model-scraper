#!/usr/bin/env python3
"""
Export DuckDB tables content to knx_doc_extended table with same structure as Excel export
"""
import duckdb
import logging
from pathlib import Path

from logger_config import LoggerConfig
from etn_cdm_upserter import EtnCdmMappingUpserter, EtnCdmUpserter
from extend_knx_doc import ExtendKnxDoc


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

        extender_logger = logger.getChild("ExtendKnxDoc")
        extender = ExtendKnxDoc(connection=con, logger=extender_logger)
        insert_count = extender.run()
        logger.info("Extend Kinaxis Doc completed")

        # Run ETN CDM mapping upsert using the freshly exported data
        mapping_upserter_logger = logger.getChild("EtnCdmMappingUpserter")
        mapping_upserter = EtnCdmMappingUpserter(db_path=db_path, logger=mapping_upserter_logger)
        mapping_upserter.run(con)
        logger.info("ETN CDM mapping upsert completed")

        # Summarize Trillium augmentation into the etn_cdm table
        cdm_upserter_logger = logger.getChild("EtnCdmUpserter")
        etn_cdm_upserter = EtnCdmUpserter(db_path=db_path, logger=cdm_upserter_logger)
        etn_cdm_upserter.run(con)
        logger.info("ETN CDM aggregation upsert completed")

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
