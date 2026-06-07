import csv
import os
import random
import uuid
from datetime import date
from typing import Any, Dict, List

from src.utils.generator.common.base_generator import BaseGenerator


class ResellerSalesCSVGenerator(BaseGenerator):
    """Handles the production of mock row data and writes it out to disk."""

    def _generate_daily_rows(self, count: int, sale_date: date) -> List[Dict[str, Any]]:
        """Internal worker method to build randomized rows for a target date."""
        rows = []
        for _ in range(count):
            qty = random.randint(1, 8)
            unit_price = round(random.uniform(*self.settings.ticket_price_range), 2)
            total = round(qty * unit_price, 2)

            rows.append({
                "Transaction ID": str(uuid.uuid4()),
                "Event name": random.choice(self.settings.event_names),
                "Number of purchased tickets": qty,
                "Total amount": f"{total:.2f}",
                "Sales channel": random.choice(self.settings.sales_channels),
                "Customer first name": self.fake.first_name(),
                "Customer last name": self.fake.last_name(),
                "Office location": random.choice(self.settings.office_locations),
                "Created Date": sale_date.strftime("%Y-%m-%d"),
            })
        return rows

    def populate(self, **dependencies: Any) -> int:
        """Generates dynamic daily sales batches and saves them into a target CSV file."""
        filepath = dependencies["filepath"]
        row_count = dependencies["row_count"]
        sale_date = dependencies["sale_date"]

        rows = self._generate_daily_rows(row_count, sale_date)

        with open(filepath, mode="w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.settings.fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return row_count