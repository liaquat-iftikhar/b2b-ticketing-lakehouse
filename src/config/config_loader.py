from src.config.app_config import AppConfig, Environment, _dynaconf, SparkConfig, StorageConfig, StorageType, \
    DatabaseConfig


class ConfigLoader:
    """
    Reads the active Dynaconf settings and returns a fully typed AppConfig.
    """

    @classmethod
    def load(cls) -> AppConfig:

        current_env_raw = _dynaconf.get("current_env", "local")

        return AppConfig(
            environment=Environment(current_env_raw.lower()),
            spark=cls._build_spark(),
            storage=cls._build_storage(),
            database=cls._build_database(),
        )


    @classmethod
    def _build_spark(cls) -> SparkConfig:
        s = _dynaconf.get("spark")
        if not s:
            raise KeyError(
                f"Configuration Loading Error: The '[spark]' block is missing. "
                f"Active environment profile is [{_dynaconf.current_env}]."
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
            raise KeyError("Configuration Loading Error: The '[storage]' block is missing.")

        return StorageConfig(
            type=StorageType(s.get("type", "local").lower()),
            base_path=s.get("base_path"),
            inbound_path=s.get("inbound_path", ""),
        )

    @classmethod
    def _build_database(cls) -> DatabaseConfig:
        d = _dynaconf.get("database")
        if not d:
            raise KeyError("Configuration Loading Error: The '[database]' block is missing.")

        return DatabaseConfig(
            host=d.get("host"),
            port=int(d.get("port")),
            name=d.get("name"),
            user=d.get("user"),
            password=d.get("password"),
            driver=d.get("driver", "org.postgresql.Driver"),
        )
