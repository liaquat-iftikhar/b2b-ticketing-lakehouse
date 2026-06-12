import random
from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import Any, Optional

from faker import Faker


class BaseGeneratorSettings:
    pass


class BaseGenerator(ABC):
    """Base class for all data generators, providing shared Faker and date helpers."""

    def __init__(self, fake: Faker, settings: BaseGeneratorSettings):
        self.fake = fake
        self.settings = settings

    @property
    def _date_range_days(self) -> int:
        return (self.settings.date_end - self.settings.date_start).days

    def _generate_random_date(self) -> date:
        return self.settings.date_start + timedelta(days=random.randint(0, self._date_range_days))

    @abstractmethod
    def populate(self, cursor: Optional[Any] = None, **dependencies: Any) -> Any:
        """
        Generate and persist a batch of records.

        Args:
            cursor: A database cursor for SQL-backed generators. CSV-based generators
                    receive None here and pass all inputs through dependencies.
            **dependencies: Generator-specific inputs (e.g. foreign-key ID lists).
        """
