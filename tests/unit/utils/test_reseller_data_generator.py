import os
import re
import csv
import unittest
import tempfile
import shutil
from src.utils.reseller_data_generator import ResellerDataGenerator


class TestResellerDataGenerator(unittest.TestCase):
    """Unit test suite for evaluating the programmatic data contract constraints of ResellerDataGenerator."""

    def setUp(self):

        # Dynamically allocate a secure temporary directory on the host machine
        self.test_dir = tempfile.mkdtemp()
        self.generator = ResellerDataGenerator(target_dir=self.test_dir)

    def tearDown(self):

        # Atomically wipe the temporary directory structure to clean filesystem clutter
        shutil.rmtree(self.test_dir)

    def test_file_count_generation(self):
        """Verifies the batch generator creates the exact payload volume requested."""

        target_volume = 2
        self.generator.generate_daily_sales_files(total_files=target_volume)

        generated_files = os.listdir(self.test_dir)
        self.assertEqual(len(generated_files), target_volume)

    def test_filename_naming_convention(self):
        """Validates output file strings match the DailySales_MMDDYYYY_RESELLERID.csv constraint."""

        self.generator.generate_daily_sales_files(total_files=2)
        generated_files = os.listdir(self.test_dir)
        filename_regex = r"^DailySales_\d{8}_[A-Z0-9]+\.csv$"

        for filename in generated_files:
            self.assertIsNotNone(
                re.match(filename_regex, filename),
                f"Filename '{filename}' violates the required client naming convention regex matrix."
            )

    def test_csv_schema_headers(self):
        """Confirms the internal structure yields a 9-column grid with correct headers."""

        self.generator.generate_daily_sales_files(total_files=1)
        filename = os.listdir(self.test_dir)[0]
        file_path = os.path.join(self.test_dir, filename)

        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)

            self.assertEqual(len(headers), 9, "CSV column matrix schema length mismatched.")
            self.assertEqual(headers[0], "Transaction ID")
            self.assertEqual(headers[3], "Total amount")

    def test_quarantine_validation_traps(self):
        """Ensures logic traps (negative values, 0 counts) exist for testing quarantine engines."""
        self.generator.generate_daily_sales_files(total_files=1)
        filename = os.listdir(self.test_dir)[0]
        file_path = os.path.join(self.test_dir, filename)

        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            rows = list(reader)

            # Look for point-of-sale anomalies inside data frames
            has_negative_amount = any(row[3].startswith("-$") for row in rows)
            has_zero_tickets = any(row[2] == "0" for row in rows)

            self.assertTrue(
                has_negative_amount,
                f"File '{filename}' missed appending the negative value data quality test trap."
            )
            self.assertTrue(
                has_zero_tickets,
                f"File '{filename}' missed appending the zero-quantity validation engine trap."
            )


if __name__ == '__main__':
    unittest.main()
