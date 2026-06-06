from airflow.sdk.definitions.asset import Dataset

RESELLER_SALE_DATA_EXTRACTOR_DAG_SUCCESS_DATASET = Dataset(f"org.iftikhar.com/reseller_sale_data_extractor_dag/success")
ORGANIZER_DATA_EXTRACTOR_DAG_SUCCESS_DATASET = Dataset(f"org.iftikhar.com/organizer_data_extractor_dag/success")
SALE_DATA_EXTRACTOR_DAG_SUCCESS_DATASET = Dataset(f"org.iftikhar.com/sale_data_extractor_dag/success")
CUSTOMER_DATA_EXTRACTOR_DAG_SUCCESS_DATASET = Dataset(f"org.iftikhar.com/customer_data_extractor_dag/success")
RESELLER_DATA_EXTRACTOR_DAG_SUCCESS_DATASET = Dataset(f"org.iftikhar.com/reseller_data_extractor_dag/success")
COMMISSION_AGREEMENT_DATA_EXTRACTOR_DAG_SUCCESS_DATASET = Dataset(
    f"org.iftikhar.com/commission_agreement_data_extractor_dag/success")
EVENT_DATA_EXTRACTOR_DAG_SUCCESS_DATASET = Dataset(
    f"org.iftikhar.com/event_data_extractor_dag/success")
