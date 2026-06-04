import logging
import random

from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from src.utils.generator.model.customer import Customer
from src.utils.generator.model.reseller import Reseller

logger = logging.getLogger(__name__)


class MasterDataRegistry:
    """
    The Single Source of Truth for all mock enterprise dimensions. Feeds uniform data structures to data generator.
    """

    def __init__(self) -> None:
        logger.debug("Initializing MasterDataRegistry")
        self._organizers: List[str] = [
            "Grand Arena Productions", "Echo Stage Group", "Summit Tech Foundations"
        ]
        self._channels: List[str] = ["web", "app", "on-the-site"]
        self._ticket_types: List[str] = ["General Admission", "VIP Pass", "Early Bird"]

        self._events: Dict[str, str] = {
            "Rock Festival 2019": "Concert",
            "Rock Festival 2020": "Concert",
            "Tech Conference 2019": "Conference",
            "Broadway Gala": "Theater"
        }

        self._resellers: Dict[str, Reseller] = {
            "RES001": Reseller("RES001", "TicketMastery Premium", 0.12, "Europe-Central"),
            "RES002": Reseller("RES002", "PassGateway Global", 0.10, "US-East"),
            "RES003": Reseller("RES003", "LocalVibe Events", 0.08, "Europe-West")
        }

        self._customers: List[Customer] = [
            Customer("Sarah Jenkins", "sarah.j@example.com"),
            Customer("Michael Müller", "m.mueller@example.de"),
            Customer("David Chang", "dchang@example.com")
        ]

    @property
    def organizers(self) -> List[str]:
        return self._organizers

    @property
    def organizer_tuples(self) -> List[Tuple[str]]:
        return [(name,) for name in self._organizers]

    @property
    def resellers(self) -> Dict[str, Reseller]:
        return self._resellers

    @property
    def reseller_tuples(self) -> List[Tuple[str, float, str]]:
        return [reseller.to_relational_tuple() for reseller in self._resellers.values()]

    @property
    def reseller_ids(self) -> List[str]:
        """Exposes token keys ('RES001', etc.) for CSV filenames and schemas."""
        return list(self._resellers.keys())

    @property
    def customers(self) -> List[Customer]:
        return self._customers

    @property
    def customer_tuples(self) -> List[Tuple[str, str]]:
        return [customer.to_tuple() for customer in self._customers]

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
    def generate_random_date(start_year: int = 2019, end_year: int = 2020) -> datetime.date:
        """Calculates a randomized date within specified boundary windows."""
        logger.debug("Generating random date between %d and %d", start_year, end_year)
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)
        delta_days = (end_date - start_date).days
        return (start_date + timedelta(days=random.randrange(delta_days))).date()
