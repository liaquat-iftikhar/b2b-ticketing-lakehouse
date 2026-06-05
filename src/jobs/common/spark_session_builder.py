import os
import logging
from typing import Optional, Dict
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


class SparkSessionFactory:
    """
    A factory class responsible for configuring and instantiating SparkSession contexts.
    Supports modular configuration adjustments for local disk execution and AWS cloud S3.
    """

    def __init__(
            self,
            app_name: str,
            target_dir: Optional[str] = None,
            extra_configs: Optional[Dict[str, str]] = None
    ) -> None:
        self.app_name = app_name
        self.target_dir = target_dir
        self.extra_configs = extra_configs or {}
        self.logger = logging.getLogger(__name__)

    def _apply_s3_configurations(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        """Helper to inject Hadoop-AWS file system drivers and security tokens."""
        self.logger.info("Cloud storage target detected. Injecting Hadoop-AWS driver configs...")

        return (
            builder
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        )

    def get_or_create(self) -> SparkSession:
        """
        Compiles all properties and initializes an active SparkSession context engine.
        """
        self.logger.info("Compiling initialization matrix for application context: '%s'", self.app_name)
        builder = SparkSession.builder.appName(self.app_name)

        # 1. Initialize a master package registry list for external JARs
        # Start with your required PostgreSQL JDBC driver package coordinate
        jar_packages = ["org.postgresql:postgresql:42.7.3"]

        # 2. Automagic Execution Topology Evaluation
        if self.target_dir and self.target_dir.startswith("s3a://"):
            builder = self._apply_s3_configurations(builder)
            # Safe append: Merges cloud dependencies without causing key collision overrides
            jar_packages.extend([
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262"
            ])
        else:
            self.logger.info("Local file system target detected. Setting multithread master allocations...")
            builder.master("local[*]")

        # 3. Compile Core Spark Engine Properties (excluding raw jar package strings)
        compiled_configs = {
            # Turn on Adaptive Query Execution
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.shuffle.partitions": "100",
            # Delta Lake low-level catalog configurations
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            **self.extra_configs
        }

        for config_key, config_value in compiled_configs.items():
            self.logger.debug("Applying custom config rule: %s = %s", config_key, config_value)
            builder.config(config_key, config_value)

        try:
            # 4. FIX: Forward the collected jar list explicitly to the Delta wrapper ecosystem
            self.logger.info("Injecting unified dependency array into Delta configuration wrapper...")
            session = configure_spark_with_delta_pip(builder, extra_packages=jar_packages).getOrCreate()

            self.logger.info("SparkSession context successfully built (Spark version: %s).", session.version)
            return session
        except Exception as exc:
            self.logger.critical("Fatal initialization failure: Could not build active SparkContext. Details: %s", exc,
                                 exc_info=True)
            raise RuntimeError("Spark Context instantiation failed.") from exc
