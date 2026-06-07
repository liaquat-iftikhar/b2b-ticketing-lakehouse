import os
import logging
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from src.config.app_config import SparkConfig, StorageConfig, StorageType


class SparkSessionFactory:
    """
    Get or creates a SparkSession.
    """

    _POSTGRES_JAR = "org.postgresql:postgresql:42.7.3"
    _HADOOP_AWS_JARS = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]


    def __init__(self, spark_config: SparkConfig, storage_config: StorageConfig) -> None:
        self._spark_config = spark_config
        self._storage_config = storage_config
        self._logger = logging.getLogger(__name__)


    def get_or_create(self) -> SparkSession:
        """Build and return an active SparkSession."""
        self._logger.info("Initialising SparkSession for app '%s'", self._spark_config.app_name)
        builder = SparkSession.builder.appName(self._spark_config.app_name)
        jar_packages = [self._POSTGRES_JAR]

        if self._storage_config.type == StorageType.S3:
            builder = self._apply_s3_configs(builder)
            jar_packages.extend(self._HADOOP_AWS_JARS)
        else:
            self._logger.info("Local storage detected — setting master to local[*]")
            builder = builder.master("local[*]")

        for key, value in self._build_core_configs().items():
            builder = builder.config(key, value)

        try:
            session = configure_spark_with_delta_pip(
                builder, extra_packages=jar_packages
            ).getOrCreate()
            self._logger.info("SparkSession ready (Spark %s)", session.version)
            return session
        except Exception as exc:
            self._logger.critical("SparkSession initialisation failed: %s", exc, exc_info=True)
            raise RuntimeError("Failed to build SparkSession.") from exc

    def _apply_s3_configs(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        """Inject Hadoop-AWS credentials and filesystem settings."""
        self._logger.info("Applying S3/Hadoop-AWS configurations...")
        return (
            builder
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", ""))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )

    def _build_core_configs(self) -> dict:
        """Assemble the base Spark + Delta Lake configuration map."""
        return {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.shuffle.partitions": str(self._spark_config.shuffle_partitions),
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            **self._spark_config.extra_configs,
        }
