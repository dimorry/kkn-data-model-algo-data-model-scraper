import json
import logging
import random
import re
import time
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright

from logger_config import LoggerConfig
from database import TableDatabase


class EdgeSessionScraper:

    def _extract_table_name_from_text(self, text):
        """Extract table name by finding the earliest delimiter position"""
        # Find the earliest delimiter position
        delimiters = [' ', '(', ')', '.', '\n', ';', ',', 'table', 'Table']
        earliest_pos = len(text)  # Default to end of string

        for delimiter in delimiters:
            pos = text.find(delimiter)
            if pos != -1 and pos < earliest_pos:
                earliest_pos = pos

        # Extract text up to the earliest delimiter
        if earliest_pos < len(text):
            text = text[:earliest_pos]

        return text.strip()

    def _clean_name(self, name: str) -> str:
        """Clean table and field names by removing whitespace while preserving leading spaces for expanded fields"""
        if not name:
            return ""

        original_name = name

        # Check if this is an expanded field name (starts with spaces)
        leading_spaces = ""
        if name.startswith("    "):  # 4 leading spaces for expanded fields
            leading_spaces = "    "
            # Remove the leading spaces for cleaning, we'll add them back
            name_to_clean = name[4:]
        else:
            name_to_clean = name

        # Remove all types of whitespace characters from the name content
        # This includes spaces, tabs, line breaks, and other Unicode whitespace
        cleaned = re.sub(r'\s+', '', name_to_clean)

        # Remove any remaining non-alphanumeric characters except underscores, hyphens, dots, and parentheses
        # Keep dots for field paths like "Allocation.Part.Name"
        # Keep parentheses for cases like "Supplier(Mfg)"
        cleaned = re.sub(r'[^\w\-\.\(\)]', '', cleaned)

        # Restore leading spaces for expanded fields
        final_cleaned = leading_spaces + cleaned

        # Log the cleaning operation if there was a change
        if final_cleaned != original_name.strip():
            self.logger.debug(f"Cleaned name: '{original_name}' → '{final_cleaned}'")

        return final_cleaned

    def __init__(self, logger_config=None, db_path="mappings.duckdb"):
        self.browser = None
        self.context = None
        self.page = None
        self.playwright = None
        self.logger_config = logger_config or LoggerConfig()
        self.logger = self.logger_config.get_logger()
        self.db = TableDatabase(db_path, self.logger)

    def load_session_data(self, filepath="session_data.json"):
        """Load previously saved session data"""
        if not Path(filepath).exists():
            self.logger.error(f"Session file {filepath} not found")
            return False

        self.logger.info(f"Loading session data from {filepath}")
        try:
            with open(filepath, 'r') as f:
                storage_state = json.load(f)

            # Create new context with saved state
            self.playwright = sync_playwright().start()
            self.logger.debug("Starting new browser instance with saved session")
            self.browser = self.playwright.chromium.launch(channel="msedge", headless=False)
            self.context = self.browser.new_context(storage_state=storage_state)
            self.page = self.context.new_page()

            self.logger.info("Session data loaded successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to load session data: {e}")
            return False

    def scrape_page(self, url, selectors=None):
        """Scrape data from a page and extract table name from h1 elements ending with 'table'"""
        if not self.page:
            self.logger.error("No page available. Connect to browser first.")
            return None

        self.logger.info(f"Scraping page: {url}")
        try:
            start_time = datetime.now()
            self.page.goto(url)
            self.page.wait_for_load_state('networkidle')
            load_time = (datetime.now() - start_time).total_seconds()
            self.logger.debug(f"Page loaded in {load_time:.2f} seconds")

            # Extract table name from h1 elements ending with 'table'
            table_name = self._extract_table_name()
            if not table_name:
                self.logger.warning("No table name found, using URL as fallback")
                fallback_name = f"table_from_{url.split('/')[-1].replace('.htm', '')}"
                table_name = self._clean_name(fallback_name)

            self.logger.info(f"Extracted table name: {table_name}")

            # Extract calculated fields description from h2 elements
            calculated_fields_desc = self._extract_calculated_fields_description() or ""
            if calculated_fields_desc:
                self.logger.info(f"Extracted calculated fields description")

            # Extract table description from paragraph between h1 and table
            table_desc = self._extract_table_description() or ""
            if table_desc:
                self.logger.info(f"Extracted table description")

            # Extract table columns data
            table_columns = self._extract_table_columns()
            if not table_columns:
                self.logger.warning("No table columns found")
                table_columns = []

            # Save to database
            try:
                table_id = self.db.insert_table_data(
                    table_name=table_name,
                    description=table_desc,
                    calculated_fields_description=calculated_fields_desc,
                    columns_data=table_columns
                )

                self.logger.info(f"Successfully saved table '{table_name}' to database with ID {table_id}")

                # Return summary of saved data
                results = {
                    'table_id': table_id,
                    'table_name': table_name,
                    'description': table_desc,
                    'calculated_fields_description': calculated_fields_desc,
                    'columns_count': len(table_columns),
                    'status': 'saved_to_database'
                }

                # Handle additional selectors if provided
                if selectors:
                    self.logger.debug(f"Processing additional selectors: {list(selectors.keys())}")
                    for key, selector in selectors.items():
                        elements = self.page.query_selector_all(selector)
                        results[key] = []
                        self.logger.debug(f"Found {len(elements)} elements for selector '{key}': {selector}")

                        for element in elements:
                            text = element.inner_text()
                            results[key].append(text.strip())

                return results

            except Exception as e:
                self.logger.error(f"Failed to save data to database: {e}")
                return {
                    'error': f"Database save failed: {e}",
                    'table_name': table_name,
                    'status': 'failed'
                }

        except Exception as e:
            self.logger.error(f"Error scraping page {url}: {e}")
            return None

    def _extract_table_name(self):
        """Extract table name from h1 elements with names ending in 'table'"""
        try:
            # Wait for h1 elements to be present
            self.logger.debug("Waiting for h1 elements to load...")
            self.page.wait_for_selector('h1', timeout=10000)

            # Look specifically for h1 elements
            h1_elements = self.page.query_selector_all('h1')
            self.logger.debug(f"Found {len(h1_elements)} h1 elements")

            for h1 in h1_elements:
                h1_text = h1.inner_text()
                original_text = h1_text.strip()

                # Check if the h1 text ends with 'table' (case insensitive)
                if original_text.lower().endswith('table'):
                    self.logger.debug(f"Found h1 ending with 'table': {original_text}")
                    # Remove 'table' suffix and trim
                    table_name = original_text[:-5].strip()  # Remove last 5 characters ('table')
                    # Clean the table name of whitespace and special characters
                    clean_table_name = self._clean_name(table_name)
                    self.logger.debug(f"Processed table name: {table_name} → {clean_table_name}")
                    return clean_table_name

            self.logger.debug("No h1 element ending with 'table' found")
            return None

        except Exception as e:
            self.logger.error(f"Error extracting table name: {e}")
            return None

    def _extract_calculated_fields_description(self):
        """Extract calculated fields description from paragraphs between h2 and second table"""
        try:
            self.logger.debug("Looking for calculated fields description between h2 and second table...")

            # Wait for h2 elements to be present
            try:
                self.page.wait_for_selector('h2', timeout=5000)
            except:
                self.logger.debug("No h2 elements found within timeout")
                return None

            # Wait for tables to be present
            try:
                self.page.wait_for_selector('table', timeout=10000)
            except:
                self.logger.debug("No tables found within timeout")
                return None

            # Get all tables to identify the second one
            tables = self.page.query_selector_all('table')
            if len(tables) < 2:
                self.logger.debug("Less than 2 tables found, cannot identify second table")
                return None

            # Use a more precise CSS selector to find content between h2 and second table
            # Look for the h2 element and find content that follows it until the second table
            try:
                # Find paragraphs that are specifically positioned between h2 and second table
                # Using evaluate to run JavaScript that can traverse DOM more precisely
                description_text = self.page.evaluate("""
                    () => {
                        const h2Elements = document.querySelectorAll('h2');
                        const tables = document.querySelectorAll('table');

                        if (h2Elements.length === 0 || tables.length < 2) {
                            return null;
                        }

                        const h2 = h2Elements[0]; // First h2
                        const secondTable = tables[1]; // Second table

                        // Find all paragraphs between h2 and second table
                        const paragraphs = [];
                        let currentElement = h2.nextElementSibling;

                        while (currentElement && currentElement !== secondTable) {
                            if (currentElement.tagName === 'P') {
                                const text = currentElement.innerText.trim();
                                if (text.length > 10) { // Only substantial content
                                    paragraphs.push(text);
                                }
                            }
                            currentElement = currentElement.nextElementSibling;
                        }

                        return paragraphs.length > 0 ? paragraphs.join('\\n\\n') : null;
                    }
                """)

                if description_text:
                    self.logger.debug(f"Found calculated fields description: {description_text[:100]}...")
                    return description_text

            except Exception as e:
                self.logger.debug(f"JavaScript evaluation failed, falling back to simple approach: {e}")

            self.logger.debug("No calculated fields description found")
            return None

        except Exception as e:
            self.logger.error(f"Error extracting calculated fields description: {e}")
            return None

    def _extract_table_description(self):
        """Extract table description from paragraph between h1 and table elements"""
        try:
            self.logger.debug("Looking for table description between h1 and table...")

            # Find h1 element ending with 'table'
            h1_elements = self.page.query_selector_all('h1')
            target_h1 = None

            for h1 in h1_elements:
                h1_text = h1.inner_text().strip()
                if h1_text.lower().endswith('table'):
                    target_h1 = h1
                    break

            if not target_h1:
                self.logger.debug("No h1 ending with 'table' found for table description extraction")
                return None

            # Look for the first paragraph after the h1 but before any table
            # Use CSS selector to find p elements that come after the h1
            paragraphs = self.page.query_selector_all('p')

            for p in paragraphs:
                # Get the paragraph text
                p_text = p.inner_text().strip()

                # Skip empty paragraphs
                if not p_text:
                    continue

                # Check if this paragraph comes after our target h1
                # We'll use a simple approach: check if the paragraph is visible and has content
                try:
                    if p.is_visible():
                        self.logger.debug(f"Found potential table description: {p_text[:100]}...")
                        return p_text
                except:
                    # If is_visible() fails, just check if it has text content
                    if p_text:
                        self.logger.debug(f"Found potential table description: {p_text[:100]}...")
                        return p_text

            self.logger.debug("No suitable paragraph found for table description")
            return None

        except Exception as e:
            self.logger.error(f"Error extracting table description: {e}")
            return None

    def _extract_table_columns(self):
        """Extract table columns data from both tables with appropriate IsCalculated flags"""
        try:
            self.logger.debug("Looking for tables with column data...")

            # Wait for tables to be present
            try:
                self.page.wait_for_selector('table', timeout=10000)
            except:
                self.logger.debug("No tables found within timeout")
                return None

            # Find all tables
            tables = self.page.query_selector_all('table')
            if not tables:
                self.logger.debug("No tables found on page")
                return None

            columns_data = []

            # Process each table
            for table_index, table in enumerate(tables[:2]):  # Process only first 2 tables
                self.logger.debug(f"Processing table {table_index + 1}...")

                # Get all rows from the table
                rows = table.query_selector_all('tr')
                if len(rows) < 2:  # Need at least header + 1 data row
                    self.logger.debug(f"Table {table_index + 1} has insufficient rows")
                    continue

                # Determine IsCalculated based on table number
                is_calculated = (table_index == 1)  # First table (index 0) = False, Second table (index 1) = True

                # Skip the header row and process data rows
                for i, row in enumerate(rows[1:], 1):  # Skip header row
                    cells = row.query_selector_all('td, th')

                    if len(cells) >= 4:  # Ensure we have at least 4 columns
                        field_name = self._clean_name(cells[0].inner_text().strip())
                        description = cells[1].inner_text().strip()
                        data_type = cells[2].inner_text().strip()
                        key = cells[3].inner_text().strip()

                        # Extract referenced table for reference columns
                        referenced_table_id = None
                        if (data_type and data_type.lower().startswith('reference') and not is_calculated):
                            # Look for "Referenced table:" in the description (case-insensitive)
                            ref_table_text = "Referenced table:"
                            ref_start_pos = description.lower().find(ref_table_text.lower())
                            if ref_start_pos != -1:
                                # Extract text after "Referenced table:" using the actual case from description
                                ref_start = ref_start_pos + len(ref_table_text)
                                # Find the end of the table name (until next sentence, period, or newline)
                                ref_text = description[ref_start:].strip()  # Strip any leading/trailing spaces
                                raw_referenced_table_name = self._extract_table_name_from_text(ref_text)
                                referenced_table_name = self._clean_name(raw_referenced_table_name)

                                # Look up the table ID by name
                                referenced_table_id = self.db.get_table_id_by_name(referenced_table_name)
                                if referenced_table_id:
                                    self.logger.debug(f"Extracted referenced table '{referenced_table_name}' (ID: {referenced_table_id}) for field '{field_name}'")
                                else:
                                    self.logger.warning(f"Referenced table '{referenced_table_name}' not found in database for field '{field_name}'")

                        # Determine if this field should be displayed on export (True for key fields)
                        display_on_export = key.lower() in ['true', 'yes', '1', 'key', 'primary'] if key else False

                        # Create column data matching the data structure format
                        column_data = [
                            field_name,           # FieldName
                            description,          # Description
                            data_type,           # Data Type
                            key,                 # is_key (renamed from Key)
                            is_calculated,       # IsCalculated (False for first table, True for second)
                            referenced_table_id, # referenced_table_id
                            display_on_export,   # display_on_export
                        ]

                        columns_data.append(column_data)
                        calc_status = "calculated" if is_calculated else "regular"
                        self.logger.debug(f"Extracted {calc_status} field from table {table_index + 1}, row {i}: {field_name}")

            self.logger.info(f"Successfully extracted {len(columns_data)} total columns from {len(tables)} tables")
            return columns_data

        except Exception as e:
            self.logger.error(f"Error extracting table columns: {e}")
            return None

    def close(self):
        """Close browser and database connections"""
        if self.browser:
            self.logger.info("Closing browser connection")
            try:
                self.browser.close()
                self.logger.info("Browser connection closed successfully")
            except Exception as e:
                self.logger.error(f"Error closing browser: {e}")

        if self.playwright:
            try:
                self.playwright.stop()
                self.logger.debug("Playwright stopped")
            except Exception as e:
                self.logger.error(f"Error stopping playwright: {e}")

        if self.db:
            try:
                self.db.close()
                self.logger.debug("Database connection closed")
            except Exception as e:
                self.logger.error(f"Error closing database: {e}")


# Example usage
def main():
    # Set up logging (you can adjust log level and add log file)
    logger_config = LoggerConfig(
        name="EdgeSessionScraper",
        log_level=logging.DEBUG,
        log_file="scraper.log"
    )
    scraper = EdgeSessionScraper(logger_config=logger_config)
    logger = logger_config.get_logger()

    logger.info("Starting scraper application")

    # Method 2: Use saved session data
    logger.info("Attempting to load session data")
    pages_to_scrape = [
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/allocation_table_.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/aggregatepartcustomer_ta.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/alternatepart_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/alternaterouting_table_.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/billofmaterial(mfg)_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/bonusschedule_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/bomalternate_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/constraint_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/constraintavailable_tabl.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/customer_table_.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/customerdestination_tabl.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/crpoperation_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/demandorder_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/engineeringchange_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/forecastdetail_table.htm",        
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/historicaldemandactual_t.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/historicaldemandheader_t.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/historicaldemandorder_ta.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/historicalreceiptheader_t.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/historicalreceipt_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/historicalsupplyactual_t.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/historicalsupplyheader_t.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/historicalsupplyorder_ta.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/independentdemand_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/model_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/onhand_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/part_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/partsource_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/partcustomer.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/partsolution_table.htm",        
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/partsupplier.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/penaltyschedule_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/pool_table.htm",        
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/project_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/control/projecttype_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/routing_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/scheduledreceipt_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/site_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/source_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/sourceconstraint_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/substitutegroup_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/supplier_table.htm",
        "https://help.kinaxis.com/20162/datamodel/content/rr_datamodel/input/supplyorder_table.htm",
        ]
    if scraper.load_session_data():
        logger.info(f"Starting to scrape {len(pages_to_scrape)} pages")
        for i, url in enumerate(pages_to_scrape, 1):
            if i > 1:  # Skip wait for the first page
                wait_time = random.randint(5, 38)
                logger.info(f"Waiting {wait_time} seconds before scraping page {i}...")
                time.sleep(wait_time)
            logger.info(f"Scraping page {i}/{len(pages_to_scrape)}: {url}")
            data = scraper.scrape_page(url)
            if data:
                logger.info(f"Successfully scraped page {i}: {data.get('table_name', 'Unknown')}")
            else:
                logger.error(f"Failed to scrape page {i}: {url}")
        logger.info("All pages processed")

    scraper.close()
    logger.info("Scraper application finished")


if __name__ == "__main__":
    main()