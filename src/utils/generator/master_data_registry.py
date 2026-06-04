import random
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any, Optional


class MasterDataFactory:
    """
    The Single Source of Truth for all mock enterprise dimensions. Feeds uniform data structures to data generator.
    """

    def __init__(self) -> None:
        # Master Dimensions
        self._organizers: List[str] = ["Grand Arena Productions", "Echo Stage Group", "Summit Tech Foundations"]
        self._channels: List[str] = ["web", "app", "on-the-site"]

        # Resellers mapped with IDs, commission rates, and operational regions
        self._resellers: Dict[str, Dict[str, Any]] = {
            "RES001": {"name": "TicketMastery Premium", "rate": 0.12, "region": "Europe-Central"},
            "RES002": {"name": "PassGateway Global", "rate": 0.10, "region": "US-East"},
            "RES003": {"name": "LocalVibe Events", "rate": 0.08, "region": "Europe-West"}
        }

        self._customers: List[Tuple[str, str]] = [
            ("Sarah Jenkins", "sarah.j@example.com"),
            ("Michael Müller", "m.mueller@example.de"),
            ("David Chang", "dchang@example.com")
        ]

        # Event matrix mapping names to types
        self._events: Dict[str, str] = {
            "Rock Festival 2019": "Concert",
            "Rock Festival 2020": "Concert",
            "Tech Conference 2019": "Conference",
            "Broadway Gala": "Theater"
        }

        self._ticket_types: List[str] = ["General Admission", "VIP Pass", "Early Bird"]

    @property
    def organizer_tuples(self) -> List[Tuple[str]]:
        return [(name,) for name in self._organizers]

    @property
    def reseller_tuples(self) -> List[Tuple[str, float, str]]:
        """Formats resellers for relational database insertions."""
        return [(data["name"], data["rate"], data["region"]) for data in self._resellers.values()]

    @property
    def reseller_ids(self) -> List[str]:
        """Exposes token keys ('RES001', etc.) for CSV filenames and schemas."""
        return list(self._resellers.keys())

    @property
    def customer_tuples(self) -> List[Tuple[str, str]]:
        return self._customers

    @property
    def channels(self) -> List[str]:
        return self._channels

    @property
    def event_names(self) -> List[str]:
        return list(self._events.keys())

    @property
    def ticket_types(self) -> List[str]:
        return self._ticket_types

    @staticmethod
    def generate_random_date(start_year: int = 2019, end_year: int = 2020) -> datetime:
        """Calculates a randomized date within the required analytical boundary windows."""
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)
        delta_days = (end_date - start_date).days
        return start_date + timedelta(days=random.randrange(delta_days))


    def create_purchase_payloads(self, volume: int) -> List[Tuple]:
        """Generates transactional row definitions matching required target fields."""
        purchases: List[Tuple] = []

        for _ in range(volume):
            org_id = random.randint(1, len(self._organizers))
            customer_id = random.randint(1, len(self._customers))

            # 30% chance for direct organizer sales (reseller_id becomes None/NULL)
            reseller_id: Optional[int] = None if random.random() < 0.30 else random.randint(1, len(self._resellers))

            purchase_date = self.generate_random_date()
            channel = random.choice(self._channels)
            ticket_type = random.choice(self._ticket_types)
            quantity = random.randint(1, 5)

            base_price = random.choice([50.00, 100.00, 250.00])
            total_amount = quantity * base_price

            purchases.append((
                org_id, reseller_id, customer_id, purchase_date,
                channel, ticket_type, quantity, total_amount
            ))

        return purchases
