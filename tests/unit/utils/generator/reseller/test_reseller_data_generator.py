import os
import re
import csv
import unittest
import tempfile
import shutil

from src.utils.generator.master_data_registry import MasterDataFactory
from src.utils.generator.reseller.reseller_data_generator import ResellerDataGenerator


class TestResellerDataGenerator(unittest.TestCase):
    """Unit test suite verifying the contract compliance of the ResellerDataGenerator output files."""

    def setUp(self):
        # 1. Allocate a temporary directory on the local filesystem
        self.test_dir = tempfile.mkdtemp()

        # 2. Instantiate and inject dependencies cleanly
        self.factory = MasterDataFactory()
        self.generator = ResellerDataGenerator(data_factory=self.factory, target_dir=self.test_dir)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_file_generation_count(self):
        """Verifies that the generator outputs the exact number of batch files requested."""
        requested_count = 3
        self.generator.generate_daily_sales_files(total_files=requested_count)

        generated_files = os.listdir(self.test_dir)
        self.assertEqual(len(generated_files), requested_count)

    def test_filename_convention_matches_specification(self):
        """Validates that filenames perfectly adhere to the DailySales_MMDDYYYY_RESELLERID.csv regex."""
        self.generator.generate_daily_sales_files(total_files=2)
        generated_files = os.listdir(self.test_dir)

        filename_regex = r"^DailySales_\d{8}_[A-Z0-9]+\.csv$"

        for filename in generated_files:
            self.assertIsNotNone(
                re.match(filename_regex, filename),
                f"Filename '{filename}' violates the required client naming convention structure."
            )

    def test_csv_schema_headers(self):
        """Confirms that the output CSV schema yields exactly 9 columns with the proper headers."""
        self.generator.generate_daily_sales_files(total_files=1)
        filename = os.listdir(self.test_dir)[0]
        file_path = os.path.join(self.test_dir, filename)

        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)

            self.assertEqual(len(headers), 9, "The output CSV schema length does not contain exactly 9 columns.")
            self.assertEqual(headers[0], "Transaction ID")
            self.assertEqual(headers[1], "Event name")
            self.assertEqual(headers[3], "Total amount")
            self.assertEqual(headers[8], "Created Date")

    def test_quarantine_anomalies_are_injected(self):
        """Ensures that validation traps (negative total amounts and 0 ticket quantities) are present in every file."""
        self.generator.generate_daily_sales_files(total_files=1)
        filename = os.listdir(self.test_dir)[0]
        file_path = os.path.join(self.test_dir, filename)

        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Bypass the header row
            rows = list(reader)

            # Evaluate row arrays against our business quarantine rules
            has_negative_amount = any(row[3].startswith("-$") for row in rows)
            has_zero_tickets = any(row[2] == "0" for row in rows)

            self.assertTrue(
                has_negative_amount,
                f"File '{filename}' failed to inject the negative financial value validation trap."
            )
            self.assertTrue(
                has_zero_tickets,
                f"File '{filename}' failed to inject the zero-quantity item ledger validation trap."
            )


if __name__ == '__main__':
    unittest.main()
