import os
import random
import uuid
from datetime import timedelta
from typing import List, Tuple

from faker import Faker

from src.config.config_loader import ConfigLoader
from src.utils.generator.reseller_vendor_data.reseller_sales_csv_settings import ResellerSalesCSVSettings
from src.utils.generator.reseller_vendor_data.data_generator import ResellerSalesCSVGenerator


class ResellerCSVOrchestrator:
    """
    Coordinates directory setup and schedules the CSV export pipeline.

    The output directory is injected at construction time from the centralized
    AppConfig (storage.inbound_path), keeping storage concerns out of settings.
    """

    def __init__(self, settings: ResellerSalesCSVSettings, output_dir: str) -> None:
        self._settings = settings
        self._output_dir = output_dir
        self._fake = Faker()
        self._fake.seed_instance(self._settings.seed)
        random.seed(self._settings.seed)

        self._generator = ResellerSalesCSVGenerator(self._fake, self._settings)

    def _generate_reseller_ids(self) -> List[str]:
        return [str(uuid.uuid4()) for _ in range(self._settings.num_resellers)]

    def run(self) -> None:
        """Executes full operational sweep across parameters configured inside settings."""
        os.makedirs(self._output_dir, exist_ok=True)

        reseller_ids = self._generate_reseller_ids()
        all_exported_files: List[Tuple[str, int]] = []

        print(f"Beginning Reseller Data Engine extraction → output: '{self._output_dir}'")

        for reseller_id in reseller_ids:
            day_offsets = random.sample(
                range(0, self._settings.total_days_range),
                self._settings.days_to_generate,
            )

            for offset in sorted(day_offsets):
                sale_date = self._settings.date_start + timedelta(days=offset)
                date_str = sale_date.strftime("%m%d%Y")

                filename = f"DailySales_{date_str}_{reseller_id}.CSV"
                filepath = os.path.join(self._output_dir, filename)

                n_rows = random.randint(*self._settings.sales_per_day)

                written_rows = self._generator.populate(
                    filepath=filepath,
                    row_count=n_rows,
                    sale_date=sale_date,
                )

                all_exported_files.append((filename, written_rows))
                print(f"  {filename}  ({written_rows} rows)")

        total_files = len(all_exported_files)
        total_rows = sum(rows for _, rows in all_exported_files)
        print(f"\n{total_files} files written to '{self._output_dir}/'")
        print(f"Total rows generated: {total_rows}")


if __name__ == "__main__":
    _app_config = ConfigLoader.load()
    _settings = ResellerSalesCSVSettings()
    _orchestrator = ResellerCSVOrchestrator(
        settings=_settings,
        output_dir=_app_config.storage.inbound_path,
    )
    _orchestrator.run()
