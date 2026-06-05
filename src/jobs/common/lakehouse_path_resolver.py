import os
import logging
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


@dataclass(frozen=True)
class LakehousePaths:
    target_dir: str
    checkpoint_dir: str
    quarantine_dir: str


class LakehousePathResolver:
    """
    A path resolution manager that maps Medallion architecture layers, quarantine buckets, and ETL contexts to
    structured storage paths.
    """

    def __init__(self, custom_base_path: Optional[str] = None) -> None:
        self.custom_base_path = custom_base_path
        self.logger = logging.getLogger(__name__)

    def _determine_base_directory(self, etl_name: str) -> str:
        if self.custom_base_path:
            return self.custom_base_path
        project_root = Path(__file__).resolve().parent.parent.parent.parent
        return str(project_root / "data" / "lakehouse" / etl_name)

    def resolve(self, layer: str, etl_name: str) -> LakehousePaths:
        """
        Resolves storage coordinates across targets, checkpoints, and quarantines.

        Args:
            layer (str): The target data tier (e.g., 'bronze', 'silver', 'gold').
            etl_name (str): The logical name of the ETL pipeline domain.

        Returns:
            LakehousePaths: Dataclass instance containing all verified storage coordinates.

        Raises:
            ValueError: If inputs fail basic string population validation requirements.
            RuntimeError: If underlying system operations encounter structural path failures.
        """
        if not layer or not etl_name:
            raise ValueError("Parameters 'layer' and 'etl_name' must be populated strings.")

        try:
            sanitized_layer = layer.strip().lower()
            sanitized_etl = etl_name.strip().lower()

            base_path = self._determine_base_directory(sanitized_etl)
            checkpoint_suffix = f"{sanitized_etl}_to_{sanitized_layer}"

            if base_path.startswith("s3a://"):
                base_uri = base_path.rstrip("/")
                target_dir = f"{base_uri}/{sanitized_layer}"
                checkpoint_dir = f"{base_uri}/_checkpoints/{checkpoint_suffix}"
                quarantine_dir = f"{base_uri}/quarantine/{sanitized_layer}"
            else:
                target_dir = os.path.join(base_path, sanitized_layer)
                checkpoint_dir = os.path.join(base_path, "_checkpoints", checkpoint_suffix)
                quarantine_dir = os.path.join(base_path, "quarantine", sanitized_layer)

            self.logger.debug("Successfully compiled path layout configurations via factory schema mapper.")

            # Instantiating the clean, immutable container structure
            return LakehousePaths(
                target_dir=target_dir,
                checkpoint_dir=checkpoint_dir,
                quarantine_dir=quarantine_dir,
            )

        except Exception as exc:
            raise RuntimeError(f"Lakehouse path resolution failure for: {etl_name}/{layer}") from exc
