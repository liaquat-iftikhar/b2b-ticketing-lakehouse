import functools

from src.config.app_config import (
    AppConfig,
    AirflowConfig,
    DatabaseConfig,
    Environment,
    SparkConfig,
    StorageConfig,
    StorageType,
    _dynaconf,
)


class ConfigLoader:
    """Parses Dynaconf settings into a typed AppConfig."""

    @classmethod
    @functools.lru_cache(maxsize=1)
    def load(cls) -> AppConfig:
        """
        Return the application config, reading settings files at most once per process.

        The result is cached so repeated imports during Airflow DAG parsing do not
        re-read settings.toml on every scheduler cycle.
        """
        current_env_raw = _dynaconf.get("current_env", "local")

        return AppConfig(
            environment=Environment(current_env_raw.lower()),
            spark=cls._build_spark(),
            storage=cls._build_storage(),
            database=cls._build_database(),
            airflow=cls._build_airflow(),
        )

    @classmethod
    def _build_spark(cls) -> SparkConfig:
        s = _dynaconf.get("spark")
        if not s:
            raise KeyError(
                f"Missing '[spark]' block in settings "
                f"(active env: {_dynaconf.current_env})."
            )

        return SparkConfig(
            app_name=s.get("app_name", "lakehouse_etl_pipeline"),
            shuffle_partitions=int(s.get("shuffle_partitions", 100)),
            extra_configs=dict(s.get("extra_configs") or {}),
        )

    @classmethod
    def _build_storage(cls) -> StorageConfig:
        s = _dynaconf.get("storage")
        if not s:
            raise KeyError("Missing '[storage]' block in settings.")

        return StorageConfig(
            type=StorageType(s.get("type", "local").lower()),
            base_path=s.get("base_path"),
            inbound_path=s.get("inbound_path", ""),
        )

    @classmethod
    def _build_airflow(cls) -> AirflowConfig:
        a = _dynaconf.get("airflow") or {}
        return AirflowConfig(
            spark_conn_id=a.get("spark_conn_id", "spark_local"),
            postgres_conn_id=a.get("postgres_conn_id", "postgres_event_platform"),
        )

    @classmethod
    def _build_database(cls) -> DatabaseConfig:
        d = _dynaconf.get("database")
        if not d:
            raise KeyError("Missing '[database]' block in settings.")

        return DatabaseConfig(
            host=d.get("host"),
            port=int(d.get("port")),
            name=d.get("name"),
            user=d.get("user"),
            password=d.get("password"),
            driver=d.get("driver", "org.postgresql.Driver"),
        )
