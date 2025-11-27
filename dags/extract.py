from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# Paths inside the container
DATASETS_DIR = "/opt/airflow/datasets"
ZIP_PATH = os.path.join(DATASETS_DIR, "weather-dataset.zip")
ORIG_CSV_PATH = os.path.join(DATASETS_DIR, "weatherHistory.csv")
NEW_CSV_PATH = os.path.join(DATASETS_DIR, "weatherHistory1.csv")


def download_and_unzip():
    # Importing at the start of dag file kept crashing the program and it would not be recognized by the UI
    # Importing here so DAG can parsed correctly
    from kaggle.api.kaggle_api_extended import KaggleApi
    import zipfile

    os.makedirs(DATASETS_DIR, exist_ok=True)

    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files(
        "muthuj7/weather-dataset",
        path=DATASETS_DIR,
        unzip=False,
    )

    with zipfile.ZipFile(ZIP_PATH, "r") as zip_ref:
        zip_ref.extractall(DATASETS_DIR)

    print("Download and unzip finished.")


def read_and_copy_csv():
     
    import pandas as pd

    df = pd.read_csv(ORIG_CSV_PATH)
    print("Columns in weatherHistory.csv:", list(df.columns))

    df.to_csv(NEW_CSV_PATH, index=False)
    print(f"Saved copy as: {NEW_CSV_PATH}")


with DAG(
    dag_id="extract_weather_kaggle",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # trigger manually from UI
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id="download_and_unzip",
        python_callable=download_and_unzip,
    )

    process_task = PythonOperator(
        task_id="read_and_copy_csv",
        python_callable=read_and_copy_csv,
    )

    download_task >> process_task
