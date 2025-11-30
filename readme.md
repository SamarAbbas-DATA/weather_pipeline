# Airflow weather ETL pipeline using kaggleAPI  

This project uses Apache Airflow running on WSL with Docker containers. Data is retrieved from the Kaggle API, extracted, transformed using Python, and finally loaded into a PostgreSQL database hosted on the local machine.

Two DAGs are implemented:

extract.py – downloads the dataset from Kaggle as a ZIP file and extracts it into the datasets/ directory.

project_file.py – reads the extracted data, performs all necessary transformations, and loads it into PostgreSQL.

Both DAGs are triggered manually in sequence (first extraction, then transformation/loading). All transformations are handled within a single task to maintain simplicity and efficiency.


```
ARFLOW-NEW/
├─ .kaggle/
├─ config/
├─ dags/
│  ├─ __pycache__/
│  ├─ extract.py
│  └─ project_file.py
├─ databases/
├─ datasets/
│  ├─ weather_daily.csv
│  ├─ weather_monthly.csv
│  ├─ weather-dataset.zip
│  ├─ weatherHistory_clean.csv
│  ├─ weatherHistory.csv
│  └─ weatherHistory1.csv
├─ logs/
├─ plugins/
├─ venv/
├─ .env
├─ .gitignore
├─ docker-compose.yaml
├─ LICENSE
└─ readme.md

```