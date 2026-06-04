import unittest
from unittest.mock import MagicMock

from src.utils.generator.seller.operational_data_generator import OperationalDataGenerator
from src.utils.generator.seller.purchase_data_generator import PurchaseDataGenerator
from src.utils.generator.seller.database_repository import DatabaseRepository
from src.utils.generator.model.purchase import Purchase


class TestOperationalDataGenerator(unittest.TestCase):
    """Unit tests for OperationalDataGenerator verifying seeding and appending behavior."""

    def setUp(self) -> None:
        self.mock_generator = MagicMock(spec=PurchaseDataGenerator)
        self.mock_repository = MagicMock(spec=DatabaseRepository)
        self.orchestrator = OperationalDataGenerator(
            generator=self.mock_generator,
            repository=self.mock_repository
        )

        # Setup mock factory properties
        self.mock_generator.organizer_tuples = [("Org A",)]
        self.mock_generator.reseller_tuples = [("Reseller A", 0.1, "Region A")]
        self.mock_generator.customer_tuples = [("Customer A", "customer@example.com")]

        # Mock create_purchase_payloads
        self.mock_purchase = MagicMock(spec=Purchase)
        self.mock_purchase.to_tuple.return_value = (1, 1, 1, "2026-06-04", "web", "VIP", 2, 200.0)
        self.mock_generator.create_purchase_payloads.return_value = [self.mock_purchase]

    def test_generate_operational_data(self) -> None:
        """Verifies that test_generate_operational_data initializes schema and inserts all collections."""
        self.orchestrator.generate_operational_data(purchase_volume=5, append=False)

        # Schema initialization should be called
        self.mock_repository.execute_schema_initialization.assert_called_once()

        # Check call counts to batch_insert_entities
        self.assertEqual(self.mock_repository.batch_insert_entities.call_count, 4)

        # Verify correct calls were made to batch_insert_entities
        calls = self.mock_repository.batch_insert_entities.call_args_list
        tables_inserted = [call[1]["table"] for call in calls]
        self.assertEqual(tables_inserted, ["organizers", "resellers", "customers", "purchases"])


if __name__ == "__main__":
    unittest.main()
