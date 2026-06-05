from airflow.sdk.definitions.asset import Dataset

RESELLER_DATA_EXTRACTOR_DAG_SUCCESS_DATASET = Dataset(f"org.iftikhar.com/reseller_data_extractor_dag/success")
ORGANIZER_DATA_EXTRACTOR_DAG_SUCCESS_DATASET = Dataset(f"org.iftikhar.com/organizer_data_extractor_dag/success")
