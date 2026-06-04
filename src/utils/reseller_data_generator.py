import os
import csv
import random

from datetime import datetime
from pathlib import Path
from typing import List, Dict


class ResellerDataGenerator:
    """
    Simulates external 3rd-party vendor software data outputs.
    Generates daily CSV sales files with deterministic dates and intentional data anomalies
    to test pipeline integration and data quarantine architectures.
    """

    def __init__(self, target_dir: str = None):
        project_root = Path(__file__).resolve().parent.parent.parent
        if target_dir is None:
            self.target_dir = str(project_root / "data" / "inbound")
        else:
            self.target_dir = target_dir
        self.current_tx_id = 10005000

        # Configuration matrices
        self.resellers: List[str] = ["RES001", "RES002", "RES003"]
        self.channels: List[str] = ["web", "mobile app", "office"]
        self.locations: List[str] = ["New York", "Berlin", "Hamburg", "Chicago", "Seattle"]
        self.events: Dict[str, str] = {
            "Rock Festival 2019": "Concert",
            "Rock Festival 2020": "Concert",
            "Tech Conference 2019": "Conference",
            "Tech Conference 2020": "Conference",
            "Broadway Gala": "Theater",
            "Championship Finals": "Sports"
        }
        self.customer_first_names: List[str] = ["John", "Jane", "Alice", "Bob", "Michael", "Emma"]
        self.customer_last_names: List[str] = ["Smith", "Doe", "Müller", "Schneider", "Davis"]
        self.price_per_ticket: List[float] = [45.00, 75.00, 120.00, 250.00]

        # Standard schema layout mandated by EventPlatform-02-DE specifications
        self.headers: List[str] = [
            "Transaction ID", "Event name", "Number of purchased tickets",
            "Total amount", "Sales channel", "Customer first name",
            "Customer last name", "Office location", "Created Date"
        ]

    @staticmethod
    def _get_target_dates() -> List[datetime]:
        """Generates deterministic target evaluation dates across Feb 2019 and Feb 2020."""
        target_dates = []
        for day in [14, 15, 16]:
            target_dates.append(datetime(2019, 2, day))
            target_dates.append(datetime(2020, 2, day))
        return target_dates

    def _generate_anomalous_rows(self, file_date_str: str) -> List[List]:
        """Generates specific bad data records explicitly targeting quarantine logic."""
        self.current_tx_id += 1
        negative_amount_row = [
            self.current_tx_id, "Corrupted Concert", 1, "-$50.00",
            "web", "Bad", "Data", "Berlin", file_date_str
        ]

        self.current_tx_id += 1
        zero_tickets_row = [
            self.current_tx_id, "Empty Event", 0, "$0.00",
            "mobile app", "Ghost", "Buyer", "Seattle", file_date_str
        ]

        return [negative_amount_row, zero_tickets_row]

    def generate_daily_sales_files(self, total_files: int = 5) -> None:
        """
        Creates mock reseller files following the standard convention:
        DailySales_MMDDYYYY_RESELLERID.CSV
        """
        os.makedirs(self.target_dir, exist_ok=True)
        target_dates = ResellerDataGenerator._get_target_dates()

        for _ in range(total_files):
            file_date = random.choice(target_dates)
            reseller_id = random.choice(self.resellers)
            file_date_str = file_date.strftime("%Y-%m-%d")

            # Format filename matching strict client requirements
            filename = f"DailySales_{file_date.strftime('%m%d%Y')}_{reseller_id}.csv"
            file_path = os.path.join(self.target_dir, filename)

            rows: List[List] = []
            num_rows = random.randint(10, 20)

            for _ in range(num_rows):
                self.current_tx_id += 1
                event_name = random.choice(list(self.events.keys()))
                tickets = random.randint(1, 5)

                price_per_ticket = random.choice(self.price_per_ticket)
                total_amount = tickets * price_per_ticket

                rows.append([
                    self.current_tx_id,
                    event_name,
                    tickets,
                    f"${total_amount:.2f}",
                    random.choice(self.channels),
                    random.choice(self.customer_first_names),
                    random.choice(self.customer_last_names),
                    random.choice(self.locations),
                    file_date_str
                ])

            # Inject validation traps (Quarantine test matrix)
            rows.extend(self._generate_anomalous_rows(file_date_str))

            # Commit payload to CSV asset directory
            with open(file_path, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(self.headers)
                writer.writerows(rows)

        print(f" Successfully generated {total_files} mock reseller files in '{self.target_dir}'.")


if __name__ == "__main__":
    generator = ResellerDataGenerator()
    generator.generate_daily_sales_files(total_files=5)
