import os
import random
import uuid
from datetime import timedelta
from typing import List, Tuple

from faker import Faker

from reseller_sales_csv_settings import ResellerSalesCSVSettings
from data_generator import ResellerSalesCSVGenerator


class ResellerCSVOrchestrator:
    """Coordinates directory setup, configurations, and schedules export pipelines."""

    def __init__(self, settings: ResellerSalesCSVSettings):
        self._settings = settings
        self._fake = Faker()
        self._fake.seed_instance(self._settings.seed)
        random.seed(self._settings.seed)

        self._generator = ResellerSalesCSVGenerator(self._fake, self._settings)

    def _generate_reseller_ids(self) -> List[str]:
        return [str(uuid.uuid4()) for _ in range(self._settings.num_resellers)]

    def run(self) -> None:
        """Executes full operational sweep across parameters configured inside settings."""
        os.makedirs(self._settings.output_dir, exist_ok=True)

        reseller_ids = self._generate_reseller_ids()
        all_exported_files: List[Tuple[str, int]] = []

        print("Beginning Reseller Data Engine extraction process...")

        for reseller_id in reseller_ids:

            day_offsets = random.sample(range(0, self._settings.total_days_range), self._settings.days_to_generate)

            for offset in sorted(day_offsets):
                sale_date = self._settings.date_start + timedelta(days=offset)
                date_str = sale_date.strftime("%m%d%Y")

                filename = f"DailySales_{date_str}_{reseller_id}.CSV"
                filepath = os.path.join(self._settings.output_dir, filename)

                n_rows = random.randint(*self._settings.sales_per_day)

                written_rows = self._generator.populate(
                    filepath=filepath,
                    row_count=n_rows,
                    sale_date=sale_date
                )

                all_exported_files.append((filename, written_rows))
                print(f"{filename}  ({written_rows} rows generated)")

        total_files = len(all_exported_files)
        total_rows = sum(rows for _, rows in all_exported_files)

        print(f"{total_files} files successfully written to '{self._settings.output_dir}/'")
        print(f"Total combined system rows processed: {total_rows}")


if __name__ == "__main__":
    runtime_settings = ResellerSalesCSVSettings()
    orchestrator = ResellerCSVOrchestrator(settings=runtime_settings)
    orchestrator.run()