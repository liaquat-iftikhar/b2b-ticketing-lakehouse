from dataclasses import dataclass, field
from datetime import date
from typing import List, Tuple

from src.utils.generator.common.base_generator import BaseGeneratorSettings


@dataclass
class ResellerSalesCSVSettings(BaseGeneratorSettings):
    """
    Data generation parameters for the reseller CSV generator.
    """

    num_resellers: int = 5
    days_to_generate: int = 10
    sales_per_day: Tuple[int, int] = (5, 20)
    ticket_price_range: Tuple[float, float] = (15.00, 450.00)

    date_start: date = date(2019, 1, 1)
    date_end: date = date(2020, 12, 31)
    seed: int = 99

    # Static Lookup Metadata using safe default factories
    event_names: List[str] = field(
        default_factory=lambda: [
            "Future-proofed methodical pricing structure 2019",
            "Profound multi-state standardization 2020",
            "Re-contextualized mission-critical throughput 2019",
            "Switchable multi-state forecast 2020",
            "Vision-oriented impactful help-desk 2020",
            "Persevering impactful orchestration 2020",
            "Synergistic tangible emulation 2020",
            "Vision-oriented context-sensitive productivity 2020",
            "Customer-focused next generation archive 2019",
            "User-friendly systemic core 2020",
            "Grass-roots local contingency 2020",
            "Fully-configurable regional help-desk 2020",
            "Diverse bandwidth-monitored data-warehouse 2020",
            "Grass-roots mobile contingency 2020",
            "Seamless solution-oriented model 2020",
            "Reduced grid-enabled hardware 2019",
            "Profound eco-centric monitoring 2019",
            "Programmable 24/7 support 2019",
            "Enhanced intangible methodology 2020",
            "Configurable systematic protocol 2019",
        ]
    )

    sales_channels: List[str] = field(
        default_factory=lambda: ["web", "mobile-app", "phone"]
    )

    office_locations: List[str] = field(
        default_factory=lambda: [
            "Djibouti",
            "Fiji",
            "Jordan",
            "Reunion",
            "Latvia",
            "Qatar",
            "Madagascar",
            "New Caledonia",
            "Kuwait",
            "Japan",
            "Gabon",
            "Italy",
            "Kazakhstan",
            "Bermuda",
            "Iran",
            "United Kingdom",
            "Moldova",
        ]
    )

    fieldnames: List[str] = field(
        default_factory=lambda: [
            "Transaction ID",
            "Event name",
            "Number of purchased tickets",
            "Total amount",
            "Sales channel",
            "Customer first name",
            "Customer last name",
            "Office location",
            "Created Date",
        ]
    )

    @property
    def total_days_range(self) -> int:
        """Number of days between date_start and date_end, used as the sampling pool size."""
        return (self.date_end - self.date_start).days
