from dataclasses import dataclass
from datetime import datetime
from typing import Tuple, Optional


@dataclass(frozen=True)
class Purchase:
    organizer_id: int
    reseller_id: Optional[int]
    customer_id: int
    purchase_date: datetime.date
    sales_channel: str
    ticket_type: str
    quantity: int
    total_amount: float

    def to_tuple(self) -> Tuple:
        """Flattens the structured domain model into an unboxed operational tuple."""
        return (
            self.organizer_id, self.reseller_id, self.customer_id,
            self.purchase_date, self.sales_channel, self.ticket_type,
            self.quantity, self.total_amount
        )