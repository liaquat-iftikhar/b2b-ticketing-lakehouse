import unittest
from src.utils.generator.master_data_registry import MasterDataRegistry
from src.utils.generator.seller.purchase_data_generator import PurchaseDataGenerator
from src.utils.generator.model.purchase import Purchase


class TestPurchaseDataGenerator(unittest.TestCase):
    """Unit tests for PurchaseDataGenerator."""

    def setUp(self) -> None:
        self.master_registry = MasterDataRegistry()
        self.purchase_generator = PurchaseDataGenerator(master_registry=self.master_registry)

    def test_delegated_properties(self) -> None:
        """Verifies that composition properties correctly delegate to the underlying MasterDataRegistry."""
        self.assertEqual(self.purchase_generator.organizers, self.master_registry.organizers)
        self.assertEqual(self.purchase_generator.organizer_tuples, self.master_registry.organizer_tuples)
        self.assertEqual(self.purchase_generator.resellers, self.master_registry.resellers)
        self.assertEqual(self.purchase_generator.reseller_tuples, self.master_registry.reseller_tuples)
        self.assertEqual(self.purchase_generator.customer_tuples, self.master_registry.customer_tuples)
        self.assertEqual(self.purchase_generator.customers, self.master_registry.customers)
        self.assertEqual(self.purchase_generator.channels, self.master_registry.channels)
        self.assertEqual(self.purchase_generator.ticket_types, self.master_registry.ticket_types)

    def test_create_purchase_payloads_count(self) -> None:
        """Verifies that the generated purchase payloads count matches the requested volume."""
        volume = 10
        payloads = self.purchase_generator.create_purchase_payloads(volume)
        self.assertEqual(len(payloads), volume)
        for item in payloads:
            self.assertIsInstance(item, Purchase)

    def test_purchase_payload_contents(self) -> None:
        """Checks if generated purchase payloads conform to expected model validation bounds."""
        payloads = self.purchase_generator.create_purchase_payloads(5)
        for p in payloads:
            self.assertTrue(1 <= p.organizer_id <= len(self.master_registry.organizers))
            self.assertTrue(1 <= p.customer_id <= len(self.master_registry.customers))
            if p.reseller_id is not None:
                self.assertTrue(1 <= p.reseller_id <= len(self.master_registry.resellers))
            self.assertIn(p.sales_channel, self.master_registry.channels)
            self.assertIn(p.ticket_type, self.master_registry.ticket_types)
            self.assertTrue(1 <= p.quantity <= 5)
            self.assertGreater(p.total_amount, 0)


if __name__ == "__main__":
    unittest.main()
