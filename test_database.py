#!/usr/bin/env python3
"""
Test script for database integration
"""
import logging
from database import TableDatabase
from logger_config import LoggerConfig

def test_database():
    """Test the database functionality"""
    # Setup logging
    logger_config = LoggerConfig(
        name="DatabaseTest",
        log_level=logging.DEBUG,
        log_file="test_database.log"
    )
    logger = logger_config.get_logger()

    logger.info("Starting database test")

    # Initialize database
    db = TableDatabase("test_tables.duckdb", logger)

    # Test data
    test_table_name = "Part"
    test_description = "This table contains part information for the system."
    test_calc_desc = "Calculated fields provide derived values based on other columns."

    # Test columns data (mix of regular and calculated fields)
    test_columns = [
        ["PartKey", "Unique identifier", "INTEGER", "Primary", False],
        ["PartName", "Name of the part", "VARCHAR", "", False],
        ["TotalValue", "Calculated total value", "DECIMAL", "", True],
        ["LastUpdate", "Last modification date", "TIMESTAMP", "", True]
    ]

    try:
        # Insert test data
        logger.info("Inserting test table data...")
        table_id = db.insert_table_data(
            table_name=test_table_name,
            description=test_description,
            calculated_fields_description=test_calc_desc,
            columns_data=test_columns
        )

        logger.info(f"Test table inserted with ID: {table_id}")

        # Retrieve and verify data
        logger.info("Retrieving table data...")
        table_data = db.get_table_by_name(test_table_name)

        if table_data:
            logger.info(f"Retrieved table: {table_data['name']}")
            logger.info(f"Description: {table_data['description'][:50]}...")

            # Get columns
            columns = db.get_columns_for_table(table_id)
            logger.info(f"Found {len(columns)} columns")

            for col in columns:
                calc_status = "calculated" if col['is_calculated'] else "regular"
                logger.info(f"  - {col['field_name']} ({col['data_type']}) - {calc_status}")

        # List all tables
        all_tables = db.list_all_tables()
        logger.info(f"Total tables in database: {len(all_tables)}")

        logger.info("Database test completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Database test failed: {e}")
        return False

    finally:
        db.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    success = test_database()
    print(f"Database test {'PASSED' if success else 'FAILED'}")