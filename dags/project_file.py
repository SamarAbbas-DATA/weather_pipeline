from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule
from datetime import datetime, timedelta

import os
import pandas as pd


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def transform_weather(ti):
    base_dir = "datasets"
    os.makedirs(base_dir, exist_ok=True)
    csv_path = os.path.join(base_dir, "weatherHistory1.csv")

    # ============================
    # 1) READ ORIGINAL DATA
    # ============================
    df = pd.read_csv(csv_path)

    # ============================
    # 2) DATA CLEANING
    # ============================

    # 2.1 Convert "Formatted Date" string -> proper date (YYYY-MM-DD)
    df["Date"] = pd.to_datetime(
        df["Formatted Date"].str[:10],
        format="%Y-%m-%d",
        errors="coerce",
    )

    # 2.2 Handle missing / erroneous data in critical columns
    critical_cols = [
        "Temperature (C)",
        "Apparent Temperature (C)",
        "Humidity",
        "Wind Speed (km/h)",
        "Visibility (km)",
        "Pressure (millibars)",
    ]
    df = df.dropna(subset=critical_cols + ["Date"])

    # 2.3 Keep humidity in [0, 1]
    df = df[(df["Humidity"] >= 0) & (df["Humidity"] <= 1)]

    # 2.4 Remove duplicates
    df = df.drop_duplicates()

    # ============================
    # 3) WIND STRENGTH FEATURE
    # ============================

    # km/h -> m/s
    df["wind_speed_ms"] = df["Wind Speed (km/h)"] / 3.6

    def classify_speed(ws_ms: float) -> str:
        if ws_ms <= 1.5:
            return "Calm"
        elif ws_ms <= 3.3:
            return "Light Air"
        elif ws_ms <= 5.4:
            return "Light Breeze"
        elif ws_ms <= 7.9:
            return "Gentle Breeze"
        elif ws_ms <= 10.7:
            return "Moderate Breeze"
        elif ws_ms <= 13.8:
            return "Fresh Breeze"
        elif ws_ms <= 17.1:
            return "Strong Breeze"
        elif ws_ms <= 20.7:
            return "Near Gale"
        elif ws_ms <= 24.4:
            return "Gale"
        elif ws_ms <= 28.4:
            return "Strong Gale"
        elif ws_ms <= 32.6:
            return "Storm"
        else:
            return "Violent Storm"

    df["wind_strength"] = df["wind_speed_ms"].apply(classify_speed)

    # Optional: keep cleaned hourly file for debugging / validation
    base_dir = "datasets"
    clean_path = os.path.join(base_dir, "weatherHistory_clean.csv")
    df.to_csv(clean_path, index=False)

    # ============================
    # 4) DAILY AVERAGES
    # ============================

    daily_avg = df.groupby("Date", as_index=False).agg({
        "Temperature (C)": "mean",
        "Humidity": "mean",
        "Wind Speed (km/h)": "mean",
    })

    daily_avg = daily_avg.rename(columns={
        "Temperature (C)": "avg_temperature_c",
        "Humidity": "avg_humidity",
        "Wind Speed (km/h)": "avg_wind_speed_kmh",
    })

    # Merge daily averages back onto each row (by Date)
    df_daily = df.merge(daily_avg, on="Date", how="left")

    # Build daily_weather-style CSV with correct column names
    daily_weather = df_daily.rename(columns={
        "Formatted Date": "formatted_date",
        "Precip Type": "precip_type",
        "Temperature (C)": "temperature_c",
        "Apparent Temperature (C)": "apparent_temperature_c",
        "Humidity": "humidity",
        "Wind Speed (km/h)": "wind_speed_kmh",
        "Visibility (km)": "visibility_km",
        "Pressure (millibars)": "pressure_millibars",
        # wind_strength already has good name
    })

    daily_weather = daily_weather[
        [
            "formatted_date",
            "precip_type",
            "temperature_c",
            "apparent_temperature_c",
            "humidity",
            "wind_speed_kmh",
            "visibility_km",
            "pressure_millibars",
            "wind_strength",
            "avg_temperature_c",
            "avg_humidity",
            "avg_wind_speed_kmh",
        ]
    ]

    daily_path = os.path.join(base_dir, "weather_daily.csv")
    daily_weather.to_csv(daily_path, index=False)

    # ============================
    # 5) MONTHLY AGGREGATES + MODE PRECIP
    # ============================

    # Group by month using Date column
    df["Month"] = df["Date"].dt.to_period("M")

    # Monthly averages (includes wind speed)
    monthly_agg = df.groupby("Month").agg({
        "Temperature (C)": "mean",
        "Apparent Temperature (C)": "mean",
        "Humidity": "mean",
        "Visibility (km)": "mean",
        "Pressure (millibars)": "mean",
        "Wind Speed (km/h)": "mean",
    })

    # Custom mode for Precip Type: if multiple modes -> NaN
    def monthly_mode(series: pd.Series):
        counts = series.value_counts(dropna=True)
        if counts.empty:
            return pd.NA
        max_count = counts.max()
        modes = counts[counts == max_count].index
        if len(modes) == 1:
            return modes[0]
        return pd.NA

    monthly_precip = df.groupby("Month")["Precip Type"].agg(monthly_mode)

    # Combine averages + mode
    monthly = monthly_agg.join(monthly_precip)

    # Reset index and rename columns to match "monthly_weather" spec
    monthly = monthly.reset_index()

    monthly = monthly.rename(columns={
        "Month": "month",
        "Temperature (C)": "avg_temperature_c",
        "Apparent Temperature (C)": "avg_apparent_temperature_c",
        "Humidity": "avg_humidity",
        "Visibility (km)": "avg_visibility_km",
        "Pressure (millibars)": "avg_pressure_millibars",
        "Wind Speed (km/h)": "avg_wind_speed_kmh",
        "Precip Type": "mode_precip_type",
    })

    # Convert month from Period("2006-01") -> string "2006-01"
    monthly["month"] = monthly["month"].astype(str)

    monthly_path = os.path.join(base_dir, "weather_monthly.csv")
    monthly.to_csv(monthly_path, index=False)

    # ============================
    # 6) XCOM OUTPUT
    # ============================

    ti.xcom_push(key="clean_csv_path", value=clean_path)
    ti.xcom_push(key="daily_csv_path", value=daily_path)
    ti.xcom_push(key="monthly_csv_path", value=monthly_path)

    print("TRANSFORM (CLEAN + DAILY + MONTHLY) DONE")


def validate_weather(ti):
    """
    Validate transformed data:
    - Missing values in critical + new fields
    - Range checks
    - Outlier detection (logged, but does not stop load if within global range)
    """
    daily_path = ti.xcom_pull(task_ids="transform_weather", key="daily_csv_path")
    monthly_path = ti.xcom_pull(task_ids="transform_weather", key="monthly_csv_path")

    if daily_path is None or monthly_path is None:
        raise ValueError("Validation failed: missing daily or monthly CSV paths from XCom.")

    df_daily = pd.read_csv(daily_path)
    df_monthly = pd.read_csv(monthly_path)

    # ============================
    # 1) MISSING VALUES CHECK
    # ============================

    daily_critical = [
        "temperature_c",
        "apparent_temperature_c",
        "humidity",
        "wind_speed_kmh",
        "visibility_km",
        "pressure_millibars",
        "wind_strength",
        "avg_temperature_c",
        "avg_humidity",
        "avg_wind_speed_kmh",
    ]

    monthly_critical = [
        "avg_temperature_c",
        "avg_apparent_temperature_c",
        "avg_humidity",
        "avg_visibility_km",
        "avg_pressure_millibars",
        # mode_precip_type can be NaN
    ]

    missing_daily = df_daily[daily_critical].isna().sum()
    missing_monthly = df_monthly[monthly_critical].isna().sum()

    if (missing_daily > 0).any() or (missing_monthly > 0).any():
        print("Missing values in daily critical columns:")
        print(missing_daily[missing_daily > 0])
        print("Missing values in monthly critical columns:")
        print(missing_monthly[missing_monthly > 0])
        raise ValueError("Validation failed: missing values in critical fields.")

    # ============================
    # 2) RANGE CHECKS
    # ============================

    ok_temp = df_daily["temperature_c"].between(-50, 50).all() and df_daily["avg_temperature_c"].between(-50, 50).all()
    ok_hum = df_daily["humidity"].between(0, 1).all() and df_daily["avg_humidity"].between(0, 1).all()
    ok_wind = (df_daily["wind_speed_kmh"] >= 0).all() and (df_daily["avg_wind_speed_kmh"] >= 0).all()

    if not (ok_temp and ok_hum and ok_wind):
        raise ValueError("Validation failed: range checks failed (temperature, humidity, or wind speed).")

    # ============================
    # 3) OUTLIER DETECTION (LOG)
    # ============================

    outlier_mask = (
        (df_daily["temperature_c"] < -40) |
        (df_daily["temperature_c"] > 40) |
        (df_daily["avg_temperature_c"] < -40) |
        (df_daily["avg_temperature_c"] > 40)
    )

    outliers = df_daily.loc[outlier_mask, ["formatted_date", "temperature_c", "avg_temperature_c"]]

    if not outliers.empty:
        print(f"[VALIDATION] Found {len(outliers)} temperature outliers (extreme but within global range):")
        print(outliers.head(10))

    # ============================
    # 4) PUSH PATHS FOR LOAD STEP
    # ============================

    ti.xcom_push(key="validated_daily_csv_path", value=daily_path)
    ti.xcom_push(key="validated_monthly_csv_path", value=monthly_path)

    print("VALIDATION PASSED.")


def load_weather(ti):
    """
    Load validated daily and monthly CSVs into PostgreSQL:
    - daily_weather table
    - monthly_weather table
    """
    # Import inside the function to avoid DAG parse errors if package missing
    from sqlalchemy import create_engine

    # Pull validated paths from XCom (from validation step)
    daily_path = ti.xcom_pull(task_ids="validate_weather", key="validated_daily_csv_path")
    monthly_path = ti.xcom_pull(task_ids="validate_weather", key="validated_monthly_csv_path")

    if daily_path is None or monthly_path is None:
        raise ValueError("Load failed: missing validated CSV paths from XCom.")

    df_daily = pd.read_csv(daily_path)
    df_monthly = pd.read_csv(monthly_path)

    # Add 'id' columns (1, 2, 3, ...)
    df_daily.insert(0, "id", range(1, len(df_daily) + 1))
    df_monthly.insert(0, "id", range(1, len(df_monthly) + 1))

    # PostgreSQL connection (your config)
    user = "postgres"
    password = "yam%40110%23%23"
    host = "host.docker.internal"
    port = "5432"
    database = "new_db"

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )

    # Write to database tables
    df_daily.to_sql("daily_weather", engine, if_exists="replace", index=False)
    df_monthly.to_sql("monthly_weather", engine, if_exists="replace", index=False)

    print("LOAD TO POSTGRES DONE (daily_weather, monthly_weather).")


with DAG(
    dag_id="project_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    description="Project DAG: transform + validate + load weather data",
) as dag:

    transform_task = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
    )

    validate_task = PythonOperator(
        task_id="validate_weather",
        python_callable=validate_weather,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    load_task = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # only if validation passed
    )

    transform_task >> validate_task >> load_task
