import os
import logging
from pathlib import Path
from typing import Tuple, Optional


class LakehousePathResolver:
    """
    A path resolution manager that maps Medallion architecture layers
    and ETL contexts to structured local filesystems or cloud storage directory paths.
    """

    def __init__(self, custom_base_path: Optional[str] = None) -> None:
        """
        Initializes the path resolver with an environment storage context.

        Args:
            custom_base_path (Optional[str]): An optional overriding root directory or
                                               cloud storage bucket URI (e.g., 's3a://my-bucket').
        """
        self.custom_base_path = custom_base_path
        self.logger = logging.getLogger(self.__class__.__name__)

    def _determine_base_directory(self, etl_name: str) -> str:
        """
        Private helper to resolve the base root coordinate.
        Prioritizes custom injection strings over internal project text rollups.
        """
        if self.custom_base_path:
            return self.custom_base_path

        # Fallback to local project workspace tracking directories
        project_root = Path(__file__).resolve().parent.parent.parent
        return str(project_root / "data" / "lakehouse" / etl_name)

    def resolve(self, layer: str, etl_name: str) -> Tuple[str, str]:
        """
        Resolves the target storage directory and the corresponding state checkpoint
        URI paths dynamically for a specific processing execution window.

        Args:
            layer (str): The target data tier (e.g., 'bronze', 'silver', 'gold').
            etl_name (str): The logical name of the ETL pipeline domain.

        Returns:
            Tuple[str, str]: A pair containing (target_directory_path, checkpoint_directory_path).

        Raises:
            ValueError: If empty or null values are supplied to critical lookup arguments.
            RuntimeError: If structural string formatting encounters a lower-level platform exception.
        """
        if not layer or not etl_name:
            raise ValueError("Parameters 'layer' and 'etl_name' must be populated strings.")

        try:
            # 1. Sanitize system data contract tokens
            sanitized_layer = layer.strip().lower()
            sanitized_etl = etl_name.strip().lower()

            # 2. Extract base coordinates and build deterministic token suffixes
            base_path = self._determine_base_directory(sanitized_etl)
            checkpoint_suffix = f"{sanitized_etl}_to_{sanitized_layer}"

            # 3. Route structural builds uniformly across cloud (S3) or localized disk partitions
            if base_path.startswith("s3a://"):
                base_uri = base_path.rstrip("/")
                target_dir = f"{base_uri}/{sanitized_layer}"
                checkpoint_dir = f"{base_uri}/_checkpoints/{checkpoint_suffix}"
            else:
                target_dir = os.path.join(base_path, sanitized_layer)
                checkpoint_dir = os.path.join(base_path, "_checkpoints", checkpoint_suffix)

            self.logger.debug(
                "Successfully compiled storage mappings for ETL '%s' [%s]. Target: %s, Checkpoint: %s",
                sanitized_etl, sanitized_layer, target_dir, checkpoint_dir
            )
            return target_dir, checkpoint_dir

        except Exception as exc:
            self.logger.error(
                "Structural exception caught during pathway lookup calculations for layer '%s' "
                "under ETL execution scope '%s'. Context: %s",
                layer, etl_name, exc, exc_info=True
            )
            raise RuntimeError(f"Lakehouse path resolution failure for target coordinate context: {etl_name}/{layer}") from exc
