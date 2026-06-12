from dataclasses import dataclass, field
from datetime import date
from typing import List

from src.utils.generator.common.base_generator import BaseGeneratorSettings


@dataclass
class DBGeneratorSettings(BaseGeneratorSettings):
    """Data generation scale configurations and lookup bounds."""

    num_organizers: int = 10
    num_resellers: int = 20
    num_customers: int = 200
    num_events: int = 40
    num_tickets: int = 120
    num_sales: int = 1000
    num_csv_sales: int = 200

    date_start: date = date(2019, 1, 1)
    date_end: date = date(2020, 12, 31)
    seed: int = 42

    regions: List[str] = field(
        default_factory=lambda: [
            "DE",
            "US-East",
            "US-West",
            "UK",
            "APAC",
            "FR",
            "NL",
            "PL",
        ]
    )
    event_types: List[str] = field(
        default_factory=lambda: [
            "Concert",
            "Conference",
            "Sports",
            "Theatre",
            "Festival",
            "Exhibition",
            "Workshop",
        ]
    )
    ticket_categories: List[str] = field(
        default_factory=lambda: [
            "VIP",
            "General Admission",
            "Early Bird",
            "Student",
            "Group",
            "Backstage Pass",
        ]
    )
    channels: List[str] = field(
        default_factory=lambda: ["on-site", "web", "mobile-app", "phone"]
    )
    commission_rates: List[float] = field(
        default_factory=lambda: [5.0, 7.5, 10.0, 12.5, 15.0]
    )
