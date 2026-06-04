import os
import logging
from src.utils.generator.seller.purchase_data_generator import PurchaseDataGenerator
from src.utils.generator.seller.database_repository import DatabaseRepository, DatabaseConfig

logger = logging.getLogger(__name__)


class OperationalDataGenerator:
    """
    High-level orchestrator class. Implements Dependency Injection principles
    to drive the data generation factory and storage tier safely.
    """

    def __init__(self, generator: PurchaseDataGenerator, repository: DatabaseRepository) -> None:
        self._generator = generator
        self._repository = repository

    def generate_operational_data(self, purchase_volume: int = 200, append: bool = False) -> None:
        """Orchestrates sequential ingestion loops to stand up the B2B mock state."""

        logger.info("Starting B2B environment seeding (purchase volume = %d, append = %s)...", purchase_volume, append)
        try:
            self._repository.execute_schema_initialization()

            purchase_data = self._generator.create_purchase_payloads(purchase_volume)
            purchase_data_tuples = [purchase.to_tuple() for purchase in purchase_data]

            organizer_data = self._generator.organizer_tuples
            reseller_data = self._generator.reseller_tuples
            customer_data = self._generator.customer_tuples

            self._repository.batch_insert_entities(
                table="organizers",
                query="INSERT INTO organizers (name, _created_at, _updated_at) VALUES (%s, NOW(), NOW());",
                data=organizer_data
            )
            self._repository.batch_insert_entities(
                table="resellers",
                query="""
                    INSERT INTO resellers (name, commission_rate, location_region, _created_at, _updated_at) 
                    VALUES (%s, %s, %s, NOW(), NOW());
                """,
                data=reseller_data
            )
            self._repository.batch_insert_entities(
                table="customers",
                query="INSERT INTO customers (name, email, _created_at, _updated_at) VALUES (%s, %s, NOW(), NOW());",
                data=customer_data
            )

            self._repository.batch_insert_entities(
                table="purchases",
                query="""
                    INSERT INTO purchases (
                        organizer_id, reseller_id, customer_id, purchase_date, 
                        sales_channel, ticket_type, quantity, total_amount,
                        _created_at, _updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW());
                """,
                data=purchase_data_tuples
            )
            logger.info("Successfully completed B2B environment seeding.")

        except Exception as exc:
            logger.error("Environment seeding failed: %s", exc)
            raise


if __name__ == "__main__":
    # Setup connection profile using standardized environment lookups
    CONNECTION_PROFILE = DatabaseConfig(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "event_platform"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        port=int(os.getenv("DB_PORT", "5432"))
    )

    db_repository = DatabaseRepository(connection_config=CONNECTION_PROFILE)
    data_generator = PurchaseDataGenerator()
    orchestrator = OperationalDataGenerator(generator=data_generator, repository=db_repository)

    try:
        orchestrator.generate_operational_data()
    except Exception as e:
        logger.exception("Failed to run operational data generator seeding")
