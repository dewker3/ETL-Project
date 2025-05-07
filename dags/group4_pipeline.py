import os
import pandas as pd
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import (
    DataContextConfig,
    CheckpointConfig
)
from great_expectations.datasource import Datasource
from great_expectations.execution_engine import SqlAlchemyExecutionEngine

from google.cloud import storage
storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB

base_path = Path(__file__).parents[1]
data_file=os.path.join(
    base_path,
    "data",
    "FIFA-21 Complete.csv",
)

ge_root_dir = os.path.join(base_path, "config", "ge")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
gcp_bucket = os.environ.get("GCP_GCS_BUCKET")

bq_dataset = "great_expectations_bigquery_example"
bq_table = "FIFA"

gcp_data_dest = "FIFA-21 Complete.csv"

def delete_csv_files(csv_path):
    if csv_path.endswith(".csv"):
        file_path = os.path.join(csv_path)
        os.remove(file_path)

def delete_csv_file_from_GCS(gcp_bucket, file_name):
    client = storage.Client()
    bucket = client.get_bucket(gcp_bucket)
    blob = bucket.blob(file_name)
    blob.delete()

    print(f"{file_name} has been deleted from {gcp_bucket} bucket.")

sql_query = """
CREATE OR REPLACE TABLE `degroup4ue.great_expectations_bigquery_example.FIFA_clustered`
CLUSTER BY team
AS
SELECT team, player_id, name, nationality, position, overall, age, hits, potential
FROM `degroup4ue.great_expectations_bigquery_example.FIFA`
"""

with DAG(
    "FIFA_21_GROUP4",
    description="Example DAG showcasing loading and data quality checking with FIFA data.",
    doc_md=__doc__,
    
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        project_id=PROJECT_ID,
        dataset_id=bq_dataset,
        delete_contents=True,
        trigger_rule="all_done"
    )   
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=bq_dataset
    )
    
    upload_FIFA_data_to_GCS=LocalFilesystemToGCSOperator(
        task_id="upload_FIFA_data",
        src=data_file,
        dst=gcp_data_dest,
        bucket=gcp_bucket,
    )
    
    delete_csv_files_task = PythonOperator(
        task_id='delete_csv_files',
        python_callable=delete_csv_files,
        op_kwargs={
            "csv_path": "/opt/airflow/data/FIFA-21 Complete.csv",
        },
    )    
    transfer_FIFA_data_to_BQ = GCSToBigQueryOperator(
        task_id="FIFA_gcs_to_bigquery",
        bucket=gcp_bucket,
        source_objects=[gcp_data_dest],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}.{}".format(PROJECT_ID, bq_dataset, bq_table),
        schema_fields=[
            {"name": "player_id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "nationality", "type": "STRING", "mode": "NULLABLE"},
            {"name": "position", "type": "STRING", "mode": "NULLABLE"},
            {"name": "overall", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "age", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "hits", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "potential", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "team", "type": "STRING", "mode": "NULLABLE"},
        ],
        source_format="CSV",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        allow_jagged_rows=True,
    )       
    delete_csv_files_task_GCS = PythonOperator(
        task_id='delete_csv_GCS_files',
        python_callable=delete_csv_file_from_GCS,
        op_kwargs={
        "file_name" : 'FIFA-21 Complete.csv',
        "gcp_bucket" :  'demo_data_lake_degroup4ue',
        },
    ) 
    create_clustered_table = BigQueryExecuteQueryOperator(
        task_id='create_clustered_table',
        sql=sql_query,
        use_legacy_sql=False,
    ) 

    ge_bigquery_validation_pass = GreatExpectationsOperator(
        task_id="ge_bigquery_validation_pass",
        data_context_root_dir=ge_root_dir,
        checkpoint_name='demo_taxi_pass_chk',
        return_json_dict=True
    )

    ge_bigquery_validation_fail = GreatExpectationsOperator(
        task_id="ge_bigquery_validation_fail",
        data_context_root_dir=ge_root_dir,
        checkpoint_name='demo_taxi_fail_chk',
        return_json_dict=True,
    ) 
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end",trigger_rule="all_done")
    chain(
        begin,
        delete_dataset,
        create_dataset,
        upload_FIFA_data_to_GCS,
        delete_csv_files_task,
        transfer_FIFA_data_to_BQ,
        delete_csv_files_task_GCS,
        create_clustered_table,
        [ge_bigquery_validation_fail,ge_bigquery_validation_pass],
        end,
    )
    
