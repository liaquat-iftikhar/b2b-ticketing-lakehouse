import os
import csv
import random
from pathlib import Path
from typing import List
from src.utils.generator.master_data_factory import MasterDataFactory


class ResellerDataGenerator:
    """
    Generates 3rd-party vendor software CSV data outputs.
    Depends on a MasterDataFactory instance to maintain global enterprise dimension alignment.
    """

    def __init__(self, data_factory: MasterDataFactory, target_dir: str = None) -> None:
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
            [self.current_tx_id, "Corrupted Concert", 1, "-$50.00", "web", "Bad", "Data", "Berlin", date_str],
            [self.current_tx_id + 1, "Empty Event", 0, "$0.00", "app", "Ghost", "Buyer", "Munich", date_str]
        ]

    def generate_daily_sales_files(self, total_files: int = 5) -> None:
        """Generates dynamic CSV file payloads using unified master data variables."""
        os.makedirs(self.target_dir, exist_ok=True)

        for _ in range(total_files):
            file_date = self.factory.generate_random_date()
            file_date_str = file_date.strftime("%Y-%m-%d")

            # Extract synchronized identifiers from the injected factory
            reseller_id = random.choice(self.factory.reseller_ids)

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
                    "Berlin",
                    file_date_str
                ])

            # Append validation traps
            rows.extend(self._generate_anomalous_rows(file_date_str))

            with open(file_path, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(self.headers)
                writer.writerows(rows)


if __name__ == "__main__":
    shared_factory = MasterDataFactory()
    generator = ResellerDataGenerator(data_factory=shared_factory)
    generator.generate_daily_sales_files(total_files=5)
