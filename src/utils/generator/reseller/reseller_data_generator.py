import os
import csv
import random
import logging
from pathlib import Path
from typing import List
from src.utils.generator.master_data_registry import MasterDataRegistry

logger = logging.getLogger(__name__)


class ResellerDataGenerator:
    """
    Generates 3rd-party vendor software CSV data outputs.
    Depends on a MasterDataFactory instance to maintain global enterprise dimension alignment.
    """

    def __init__(self, data_factory: MasterDataRegistry, target_dir: str = None) -> None:
        self.factory = data_factory

        project_root = Path(__file__).resolve().parent.parent.parent.parent
        self.target_dir = target_dir or str(project_root / "data" / "inbound")

        self.current_tx_id = 10005000
        self.headers: List[str] = [
            "Transaction ID", "Event name", "Number of purchased tickets",
            "Total amount", "Sales channel", "Customer first name",
            "Customer last name", "Office location", "Created Date"
        ]

    def _generate_anomalous_rows(self, date_str: str) -> List[List]:
        """Injects targeted data traps explicitly testing pipeline quarantine logic."""
        self.current_tx_id += 1
        return [
            [self.current_tx_id, "Corrupted Concert", 1, "-$50.00", "web", "Bad", "Data", None, date_str],
            [self.current_tx_id + 1, "Empty Event", 0, "$0.00", "app", "Ghost", "Buyer", None, date_str]
        ]

    def generate_daily_sales_files(self, total_files: int = 5) -> None:
        """Generates dynamic CSV file payloads using unified master data variables."""

        logger.info("Starting daily sales file generation for %d files in %s", total_files, self.target_dir)

        try:
            os.makedirs(self.target_dir, exist_ok=True)
        except OSError as e:
            logger.error("Failed to create target directory %s: %s", self.target_dir, e)
            raise

        for _ in range(total_files):
            file_date = self.factory.generate_random_date()
            file_date_str = file_date.strftime("%Y-%m-%d")

            try:
                reseller_id = random.choice(self.factory.reseller_ids)
                reseller_data = self.factory.resellers[reseller_id]
            except (IndexError, KeyError) as e:
                logger.error("Failed to extract reseller data from factory: %s", e)
                raise

            filename = f"DailySales_{file_date.strftime('%m%d%Y')}_{reseller_id}.csv"
            file_path = os.path.join(self.target_dir, filename)

            rows = []
            for _ in range(random.randint(5, 15)):
                self.current_tx_id += 1
                customer = random.choice(self.factory.customer_tuples)
                first_name, last_name = customer[0].split(" ", 1)

                tickets = random.randint(1, 4)
                total_amount = tickets * random.choice([50.00, 120.00])

                rows.append([
                    self.current_tx_id,
                    random.choice(self.factory.event_names),
                    tickets,
                    f"${total_amount:.2f}",
                    random.choice(self.factory.channels),
                    first_name,
                    last_name,
                    reseller_data.region,
                    file_date_str
                ])

            # Append validation traps
            rows.extend(self._generate_anomalous_rows(file_date_str))

            try:
                with open(file_path, mode='w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(self.headers)
                    writer.writerows(rows)
                logger.info("Successfully generated daily sales file: %s", filename)
            except OSError as e:
                logger.error("Failed to write daily sales file %s: %s", file_path, e)
                raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    shared_factory = MasterDataRegistry()
    generator = ResellerDataGenerator(data_factory=shared_factory)
    try:
        generator.generate_daily_sales_files(total_files=5)
    except Exception as _:
        logger.exception("Failed to complete daily sales file generation")
