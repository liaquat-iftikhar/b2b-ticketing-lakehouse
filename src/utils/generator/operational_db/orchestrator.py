import logging
import random
from pathlib import Path
from faker import Faker
import psycopg2

from src.config.app_config import DatabaseConfig
from src.config.config_loader import ConfigLoader
from src.utils.generator.common.base_generator import BaseGeneratorSettings
from src.utils.generator.operational_db.core.db_connection_manager import (
    DatabaseConnectionManager,
)
from src.utils.generator.operational_db.core.db_generator_settings import (
    DBGeneratorSettings,
)
from src.utils.generator.operational_db.data_generators import (
    OrganizerGenerator,
    ResellerGenerator,
    CommissionAgreementGenerator,
    CustomerGenerator,
    EventGenerator,
    TicketGenerator,
    SaleGenerator,
)


class PlatformOrchestrator:
    """Orchestrates seed environments and schedules sequential data generation tasks."""

    def __init__(self, db_config: DatabaseConfig, settings: BaseGeneratorSettings):
        self._db_config = db_config
        self._settings = settings
        self.logger = logging.getLogger(__name__)
        self._fake = Faker()
        self._fake.seed_instance(self._settings.seed)
        random.seed(self._settings.seed)

    def execute_schema_initialization(self) -> None:
        """Reads the source relational DDL structure from disk and applies it to the DB."""
        schema_path = Path(__file__).resolve().parent / "schema.sql"
        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                ddl_script = f.read()
        except OSError as e:
            self.logger.error(
                "Failed to read schema SQL file from %s: %s", schema_path, e
            )
            raise

        self.logger.info("Initializing database schema using %s...", schema_path)

        db_manager = DatabaseConnectionManager(self._db_config)
        try:
            with db_manager as conn:
                with conn.cursor() as cursor:
                    cursor.execute(ddl_script)
            self.logger.info("Database schema initialized successfully.")
        except psycopg2.Error as error:
            self.logger.error("Database schema initialization failed: %s", error)
            raise

    def run(self) -> None:
        print("Verifying and applying database structural schema...")
        self.execute_schema_initialization()

        print("Initializing database secure pipe context...")
        db_manager = DatabaseConnectionManager(self._db_config)

        with db_manager as connection:
            with connection.cursor() as cursor:
                print("Generating domain records across infrastructure targets...")

                # Instantiations
                organizer_gen = OrganizerGenerator(self._fake, self._settings)
                reseller_gen = ResellerGenerator(self._fake, self._settings)
                agreement_gen = CommissionAgreementGenerator(self._fake, self._settings)
                customer_gen = CustomerGenerator(self._fake, self._settings)
                event_gen = EventGenerator(self._fake, self._settings)
                ticket_gen = TicketGenerator(self._fake, self._settings)
                sale_gen = SaleGenerator(self._fake, self._settings)

                org_ids = organizer_gen.populate(cursor)
                res_ids = reseller_gen.populate(cursor)
                agreements = agreement_gen.populate(
                    cursor, organizer_ids=org_ids, reseller_ids=res_ids
                )
                cust_ids = customer_gen.populate(cursor)
                events = event_gen.populate(cursor, organizer_ids=org_ids)
                tickets = ticket_gen.populate(cursor, events=events)

                sale_gen.populate(
                    cursor,
                    tickets=tickets,
                    events=events,
                    customer_ids=cust_ids,
                    organizer_ids=org_ids,
                    reseller_ids=res_ids,
                    agreements=agreements,
                    count=self._settings.num_sales,
                    source="platform",
                )


if __name__ == "__main__":
    _app_config = ConfigLoader.load()
    _settings = DBGeneratorSettings()
    _orchestrator = PlatformOrchestrator(
        db_config=_app_config.database, settings=_settings
    )
    _orchestrator.run()
