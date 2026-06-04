from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class Reseller:
    id: str
    name: str
    rate: float
    region: str

    def to_relational_tuple(self) -> Tuple[str, float, str]:
        """Formats entity attributes for relational database tuple inputs."""
        return self.name, self.rate, self.region