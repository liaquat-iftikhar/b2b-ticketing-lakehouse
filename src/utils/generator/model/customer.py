from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class Customer:
    name: str
    email: str

    def to_tuple(self) -> Tuple[str, str]:
        return self.name, self.email
