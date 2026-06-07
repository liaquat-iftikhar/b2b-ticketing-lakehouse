import random
from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import Any
from faker import Faker


class BaseGeneratorSettings:
    pass


class BaseGenerator(ABC):
    """Abstract interface defining standard dependencies and date helpers for entities."""

    def __init__(self, fake: Faker, settings: GeneratorSettings):
        self.fake = fake
        self.settings = settings

    protected_date_delta = property(lambda self: (self.settings.date_end - self.settings.date_start).days)

    def _generate_random_date(self) -> date:
        random_days = random.randint(0, self.protected_date_delta)
        return self.settings.date_start + timedelta(days=random_days)

    @abstractmethod
    def populate(self, cursor: Any, **dependencies: Any) -> Any:
        """Execute mock generation strategies and persist batch executions into data engine."""
        pass