import random
import logging
from typing import List, Tuple, Dict

from src.utils.generator.master_data_registry import MasterDataRegistry
from src.utils.generator.model.purchase import Purchase

logger = logging.getLogger(__name__)


class PurchaseDataGenerator:
    """
    Generates random transaction payload data using composition with MasterDataRegistry.
    """

    def __init__(self, master_registry: MasterDataRegistry = None, direct_sale_ratio: float = 0.30) -> None:
        self.master_registry = master_registry or MasterDataRegistry()
        self.direct_sale_ratio = direct_sale_ratio
        self.price_points: List[float] = [50.00, 100.00, 250.00]
        logger.debug("Initialized PurchaseDataGenerator with direct_sale_ratio=%f", direct_sale_ratio)

    @property
    def organizers(self) -> List[str]:
        return self.master_registry.organizers

    @property
    def organizer_tuples(self) -> List[Tuple[str]]:
        return self.master_registry.organizer_tuples

    @property
    def resellers(self) -> Dict:
        return self.master_registry.resellers

    @property
    def reseller_tuples(self) -> List[Tuple]:
        return self.master_registry.reseller_tuples

    @property
    def customer_tuples(self) -> List[Tuple[str, str]]:
        return self.master_registry.customer_tuples

    @property
    def customers(self) -> List:
        return self.master_registry.customers

    @property
    def channels(self) -> List[str]:
        return self.master_registry.channels

    @property
    def ticket_types(self) -> List[str]:
        return self.master_registry.ticket_types

    def create_purchase_payloads(self, volume: int) -> List[Purchase]:
        """Generates domain-mapped Purchase entities matching business rules."""
        logger.info("Generating %d purchase payloads...", volume)
        purchases: List[Purchase] = []

        for _ in range(volume):
            org_id = random.randint(1, len(self.master_registry.organizers))
            customer_id = random.randint(1, len(self.master_registry.customers))

            # Evaluate direct vs third-party channel routing constraints logic
            is_direct = random.random() < self.direct_sale_ratio
            reseller_id = None if is_direct else random.randint(1, len(self.master_registry.resellers))

            quantity = random.randint(1, 5)
            base_price = random.choice(self.price_points)

            purchase_entity = Purchase(
                organizer_id=org_id,
                reseller_id=reseller_id,
                customer_id=customer_id,
                purchase_date=self.master_registry.generate_random_date(),
                sales_channel=random.choice(self.master_registry.channels),
                ticket_type=random.choice(self.master_registry.ticket_types),
                quantity=quantity,
                total_amount=float(quantity * base_price)
            )
            purchases.append(purchase_entity)

        logger.info("Successfully generated %d purchase payloads.", len(purchases))
        return purchases
