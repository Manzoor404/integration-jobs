from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Set the start date to 1 day ago (adjust if necessary)
    'email': ['symah1097@gmail.com'],  # Your email to receive notifications
    'email_on_failure': True,  # Enable email notifications on failure
    'email_on_retry': True,  # Enable email notifications on retry
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'dataproc_etl_dag',
    default_args=default_args,
    description='A DAG to run PySpark job on Dataproc',
    schedule_interval=timedelta(days=1),  # Set schedule to run daily (adjust as necessary)
    catchup=False,  # Only run DAG once on each schedule interval
) as dag:

    # Define the GCS Path where the PySpark script is located
    gcs_script_path = 'gs://syedmanzoor-bucket/dataproc_scripts/dataproc_airflow_etl_script.py'

    # Define the job configuration for the PySpark job
    pyspark_job = {
        "reference": {"project_id": "zomatoeda-435904"},
        "placement": {"cluster_name": "syedmanzoor-cluster"},
        "pyspark_job": {
            "main_python_file_uri": gcs_script_path,  # GCS Path to your PySpark script
            "args": [
                "gs://syedmanzoor-bucket/covid19/covid_data/",  # Input data
                "gs://syedmanzoor-bucket/transformed_data/"  # Output data
            ]
        }
    }

    # Task to submit the PySpark job to Dataproc
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='run_pyspark_etl',
        job=pyspark_job,
        region='us-central1',  # Region of the Dataproc Cluster
        project_id='zomatoeda-435904',  # GCP Project ID
        dag=dag
    )

    # Define task dependencies
    submit_pyspark_job