from airflow import DAG
from datetime import datetime
import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta

'------------------------------------------------------------------------------------------------------------------------'

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

def extract_data():
    input_path = "/opt/airflow/dags/data/raw/shipments.csv"
    output_path = "/opt/airflow/dags/data/processed/extracted.csv"

    df = pd.read_csv(input_path)

    print("Number of rows:", len(df))

    # Save raw extracted data for downstream tasks
    df.to_csv(output_path, index=False)


'------------------------------------------------------------------------------------------------------------------------'


def validate_data():
    path = "/opt/airflow/dags/data/processed/extracted.csv"
    df = pd.read_csv(path)

    print("Starting data validation...")

    # Check missing values in critical columns
    critical_columns = [
        "shipment_id",
        "customer_id",
        "route_id",
        "planned_departure",
        "planned_arrival",
        "shipment_status"
    ]

    missing_issues = df[critical_columns].isnull().any(axis=1)
    df_missing = df[missing_issues]

    print("Rows with missing critical values:", len(df_missing))

    # Validate allowed shipment status values
    valid_status = ["Delivered", "In Transit", "Cancelled"]
    invalid_status_df = df[~df["shipment_status"].isin(valid_status)]

    print("Rows with invalid status:", len(invalid_status_df))

    # Convert numeric columns safely
    numeric_columns = ["distance_km", "fuel_cost", "shipment_revenue"]

    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    numeric_issues = df[numeric_columns].isnull().any(axis=1)
    df_numeric_issues = df[numeric_issues]

    print("Rows with numeric issues:", len(df_numeric_issues))

    # Convert date columns and handle invalid formats
    df["planned_departure"] = pd.to_datetime(df["planned_departure"], errors="coerce")
    df["actual_departure"] = pd.to_datetime(df["actual_departure"], errors="coerce")
    df["planned_arrival"] = pd.to_datetime(df["planned_arrival"], errors="coerce")
    df["actual_arrival"] = pd.to_datetime(df["actual_arrival"], errors="coerce")

    # Business and temporal consistency checks
    date_issues = df[
        (df["actual_arrival"] < df["actual_departure"]) |
        (df["planned_arrival"] < df["planned_departure"]) |
        ((df["shipment_status"] == "Delivered") & (df["actual_arrival"].isnull())) |
        ((df["shipment_status"] == "In Transit") & (df["actual_arrival"].notnull()))
    ]

    print("Rows with date/business inconsistencies:", len(date_issues))

    print("Validation completed")

    # Combine all detected issues into a single dataset
    output_path = "/opt/airflow/dags/data/processed/data_quality_issues.csv"

    df_issues = pd.concat([
        df_missing,
        invalid_status_df,
        df_numeric_issues,
        date_issues
    ]).drop_duplicates()

    df_issues.to_csv(output_path, index=False)

    print("Total problematic rows saved:", len(df_issues))

'------------------------------------------------------------------------------------------------------------------------'


def transform_data():
    input_path = "/opt/airflow/dags/data/processed/extracted.csv"
    output_path = "/opt/airflow/dags/data/processed/shipments_transformed.csv"

    df = pd.read_csv(input_path)

    print("Starting transformation...")

    # Convert date columns for time calculations
    df["planned_departure"] = pd.to_datetime(df["planned_departure"], errors="coerce")
    df["actual_departure"] = pd.to_datetime(df["actual_departure"], errors="coerce")
    df["planned_arrival"] = pd.to_datetime(df["planned_arrival"], errors="coerce")
    df["actual_arrival"] = pd.to_datetime(df["actual_arrival"], errors="coerce")

    # Calculate delays in minutes
    df["departure_delay_min"] = (
        (df["actual_departure"] - df["planned_departure"]).dt.total_seconds() / 60
    )

    df["arrival_delay_min"] = (
        (df["actual_arrival"] - df["planned_arrival"]).dt.total_seconds() / 60
    )

    # # Determine if the shipment was on time; return None if arrival data is missing
    df["on_time_delivery"] = df["arrival_delay_min"].apply(
    lambda x: None if pd.isnull(x) else x <= 0
)

    # Categorize delay
    def categorize_delay(x):
        if pd.isnull(x):
            return "In Transit"
        elif x <= 0:
            return "On Time"
        elif x <= 30:
            return "Minor Delay"
        else:
            return "Major Delay"

    df["delay_category"] = df["arrival_delay_min"].apply(categorize_delay)

    # Financial metrics
    df["profit"] = df["shipment_revenue"] - df["fuel_cost"]
    df["profit_margin_pct"] = (df["profit"] / df["shipment_revenue"]) * 100

    # Clean results (rounding)
    df["profit_margin_pct"] = df["profit_margin_pct"].round(2)

    df.to_csv(output_path, index=False)

    print("Transformation completed")



'------------------------------------------------------------------------------------------------------------------------'

def compute_kpis():
    import json

    input_path = "/opt/airflow/dags/data/processed/shipments_transformed.csv"
    kpi_output_path = "/opt/airflow/dags/data/processed/kpi_summary.json"
    route_output_path = "/opt/airflow/dags/data/processed/kpi_by_route.csv"

    df = pd.read_csv(input_path)

    print("Computing KPIs...")

    # Global KPIs
    total_shipments = len(df)
    delivered_df = df[df["shipment_status"] == "Delivered"]
    in_transit_df = df[df["shipment_status"] == "In Transit"]

    delivered_shipments = len(delivered_df)
    in_transit_shipments = len(in_transit_df)

    on_time_deliveries = len(delivered_df[delivered_df["on_time_delivery"] == True])
    late_deliveries = len(delivered_df[delivered_df["on_time_delivery"] == False])

    on_time_rate = on_time_deliveries / delivered_shipments if delivered_shipments > 0 else 0

    avg_arrival_delay = delivered_df["arrival_delay_min"].mean()

    total_revenue = df["shipment_revenue"].sum()
    total_fuel_cost = df["fuel_cost"].sum()
    total_profit = df["profit"].sum()

    # KPI dictionary
    kpis = {
        "total_shipments": int(total_shipments),
        "delivered_shipments": int(delivered_shipments),
        "in_transit_shipments": int(in_transit_shipments),
        "on_time_deliveries": int(on_time_deliveries),
        "late_deliveries": int(late_deliveries),
        "on_time_delivery_rate": round(on_time_rate, 2),
        "average_arrival_delay_min": round(avg_arrival_delay, 2) if pd.notnull(avg_arrival_delay) else None,
        "total_revenue": float(total_revenue),
        "total_fuel_cost": float(total_fuel_cost),
        "total_profit": float(total_profit)
    }

    # Save global KPIs
    with open(kpi_output_path, "w") as f:
        json.dump(kpis, f, indent=4)

    print("KPI summary saved")

    # Aggregation by route with business context (origin + destination)
    kpi_by_route = df.groupby(["route_id", "origin", "destination"]).agg({
        "shipment_id": "count",
        "arrival_delay_min": "mean",
        "profit": "sum"
    }).rename(columns={
        "shipment_id": "total_shipments",
        "arrival_delay_min": "avg_arrival_delay",
        "profit": "total_profit"
    }).reset_index()

    kpi_by_route["avg_arrival_delay"] = kpi_by_route["avg_arrival_delay"].round(2)

    kpi_by_route.to_csv(route_output_path, index=False)

    print("KPI by route saved")


    # Aggregate shipment data by customer to compute customer-level KPIs:
    # total shipments, average arrival delay, and total profit,
    # then save the results to a CSV file for business analysis
    customer_output_path = "/opt/airflow/dags/data/processed/kpi_by_customer.csv"

    kpi_by_customer = df.groupby("customer_id").agg({
        "shipment_id": "count",
        "arrival_delay_min": "mean",
        "profit": "sum"
    }).rename(columns={
        "shipment_id": "total_shipments",
        "arrival_delay_min": "avg_arrival_delay",
        "profit": "total_profit"
    }).reset_index()

    kpi_by_customer["avg_arrival_delay"] = kpi_by_customer["avg_arrival_delay"].round(2)

    kpi_by_customer.to_csv(customer_output_path, index=False)

    print("KPI by customer saved")

'------------------------------------------------------------------------------------------------------------------------'

def detect_alerts():
    from datetime import datetime

    input_path = "/opt/airflow/dags/data/processed/shipments_transformed.csv"
    output_path = "/opt/airflow/dags/data/processed/alerts.csv"

    df = pd.read_csv(input_path)

    print("Detecting alerts...")

    # Ensure numeric columns are clean
    df["arrival_delay_min"] = pd.to_numeric(df["arrival_delay_min"], errors="coerce")
    df["departure_delay_min"] = pd.to_numeric(df["departure_delay_min"], errors="coerce")
    df["profit"] = pd.to_numeric(df["profit"], errors="coerce")
    df["distance_km"] = pd.to_numeric(df["distance_km"], errors="coerce")

    alerts_list = []

    def add_alerts(filtered_df, alert_type, value_column):
        for _, row in filtered_df.iterrows():
            alerts_list.append({
                "shipment_id": row["shipment_id"],
                "alert_type": alert_type,
                "detected_value": row[value_column],
                "route_id": row["route_id"],
                "customer_id": row["customer_id"],
                "timestamp": datetime.now().isoformat()
            })

    # Existing alerts
    add_alerts(df[df["arrival_delay_min"] > 60], "HIGH_ARRIVAL_DELAY", "arrival_delay_min")
    add_alerts(df[df["departure_delay_min"] > 30], "HIGH_DEPARTURE_DELAY", "departure_delay_min")
    add_alerts(df[df["profit"] < 50], "LOW_PROFIT", "profit")

    add_alerts(
        df[(df["shipment_status"] == "Delivered") & (df["arrival_delay_min"] > 90)],
        "CRITICAL_DELAY",
        "arrival_delay_min"
    )

    # New (pro) alerts
    add_alerts(df[df["arrival_delay_min"] < -10], "NEGATIVE_DELAY", "arrival_delay_min")

    add_alerts(
        df[(df["distance_km"] > 300) & (df["profit"] < 100)],
        "LONG_DISTANCE_LOW_PROFIT",
        "profit"
    )

    alerts_df = pd.DataFrame(alerts_list)

    if alerts_df.empty:
        print("No alerts detected")
    else:
        alerts_df = alerts_df.drop_duplicates()
        alerts_df.to_csv(output_path, index=False)
        print("Alerts saved:", len(alerts_df))






"----------------------------------------------------------------------------------------------------------------------------------"
'''
GPS 
'''

def extract_gps_updates():
    input_path = "/opt/airflow/dags/data/raw/gps_updates.csv"
    output_path = "/opt/airflow/dags/data/processed/gps_extracted.csv"

    df = pd.read_csv(input_path)

    print("GPS rows:", len(df))
    print(df.head())

    df.to_csv(output_path, index=False)

def merge_with_gps():
    shipments_path = "/opt/airflow/dags/data/processed/extracted.csv"
    gps_path = "/opt/airflow/dags/data/processed/gps_extracted.csv"
    output_path = "/opt/airflow/dags/data/processed/shipments_gps_enriched.csv"

    df_ship = pd.read_csv(shipments_path)
    df_gps = pd.read_csv(gps_path)

    print("Merging shipments with GPS...")

    # Convert timestamp to datetime
    df_gps["gps_timestamp"] = pd.to_datetime(df_gps["gps_timestamp"])

    # Keep latest GPS per shipment
    df_gps_latest = df_gps.sort_values("gps_timestamp").groupby("shipment_id").tail(1)

    # Merge
    df_merged = df_ship.merge(df_gps_latest, on="shipment_id", how="left")

    df_merged.to_csv(output_path, index=False)

    print("Merge completed")
    print("Rows after merge:", len(df_merged))



def compute_eta():
    input_path = "/opt/airflow/dags/data/processed/shipments_gps_enriched.csv"
    output_path = "/opt/airflow/dags/data/processed/shipments_with_eta.csv"

    df = pd.read_csv(input_path)

    print("Computing ETA...")

    # Convert to datetime
    df["gps_timestamp"] = pd.to_datetime(df["gps_timestamp"], errors="coerce")
    df["planned_arrival"] = pd.to_datetime(df["planned_arrival"], errors="coerce")

    # Assumed average speed (km/h)
    AVG_SPEED_KMH = 60

    # Compute remaining time in hours
    df["remaining_hours"] = df["estimated_remaining_km"] / AVG_SPEED_KMH

    # Convert to timedelta
    df["remaining_time"] = pd.to_timedelta(df["remaining_hours"], unit="h")

    # Compute ETA
    df["estimated_arrival_time"] = df["gps_timestamp"] + df["remaining_time"]

    # Compute risk
    # Determine delay risk by comparing the estimated arrival time (ETA) 
    # with the planned arrival time.
    # If the ETA is later than the planned arrival, the shipment is at risk of delay.
    # Otherwise, it is considered on time.
    df["eta_risk"] = df.apply(
        lambda row: "RISK_DELAY"
        if pd.notnull(row["estimated_arrival_time"]) 
        and pd.notnull(row["planned_arrival"]) 
        and row["estimated_arrival_time"] > row["planned_arrival"]
        else "ON_TIME",
        axis=1
)
    df.to_csv(output_path, index=False)

    print("ETA computation completed")
    print("Rows:", len(df))



def quality_gate():
    import os

    data_path = "/opt/airflow/dags/data/processed/extracted.csv"
    issues_path = "/opt/airflow/dags/data/processed/data_quality_issues.csv"

    print("Starting quality gate...")

    df_data = pd.read_csv(data_path)
    total_rows = len(df_data)

    if os.path.exists(issues_path):
        df_issues = pd.read_csv(issues_path)
        invalid_rows = len(df_issues)
    else:
        invalid_rows = 0

    error_rate = invalid_rows / total_rows if total_rows > 0 else 0

    print(f"Total rows: {total_rows}")
    print(f"Invalid rows: {invalid_rows}")
    print(f"Error rate: {error_rate:.2%}")

    # Stop the pipeline if the percentage of invalid rows is above 20%
    if error_rate > 0.20:
        raise ValueError(
            f"Quality gate failed: {error_rate:.2%} of rows are invalid, which is above the 20% threshold."
        )

    print("Quality gate passed")


def send_summary():
        import os

        print("Generating execution summary...")

        # Paths
        data_path = "/opt/airflow/dags/data/processed/extracted.csv"
        issues_path = "/opt/airflow/dags/data/processed/data_quality_issues.csv"
        kpi_path = "/opt/airflow/dags/data/processed/kpi_summary.json"
        alerts_path = "/opt/airflow/dags/data/processed/alerts.csv"
        route_kpi_path = "/opt/airflow/dags/data/processed/kpi_by_route.csv"

        # Load data
        df_data = pd.read_csv(data_path)
        total_rows = len(df_data)

        # Issues
        if os.path.exists(issues_path):
            df_issues = pd.read_csv(issues_path)
            invalid_rows = len(df_issues)
        else:
            invalid_rows = 0

        quality_rate = 1 - (invalid_rows / total_rows) if total_rows > 0 else 0

        # KPIs
        if os.path.exists(kpi_path):
            import json
            with open(kpi_path) as f:
                kpis = json.load(f)
            on_time_rate = kpis.get("on_time_delivery_rate", 0)
        else:
            on_time_rate = 0

        # Alerts
        if os.path.exists(alerts_path):
            df_alerts = pd.read_csv(alerts_path)
            total_alerts = len(df_alerts)
        else:
            total_alerts = 0

        # Most risky route
        if os.path.exists(route_kpi_path):
            df_route = pd.read_csv(route_kpi_path)
            if not df_route.empty:
                worst_route = df_route.sort_values("avg_arrival_delay", ascending=False).iloc[0]
                route_info = f"{worst_route['route_id']} ({worst_route['origin']} → {worst_route['destination']})"
            else:
                route_info = "N/A"
        else:
            route_info = "N/A"

        # Print summary
        print("----- PIPELINE SUMMARY -----")
        print(f"Total rows: {total_rows}")
        print(f"Invalid rows: {invalid_rows}")
        print(f"Data quality: {quality_rate:.2%}")
        print(f"On-time delivery rate: {on_time_rate}")
        print(f"Total alerts: {total_alerts}")
        print(f"Most risky route: {route_info}")
        print("----------------------------")



def save_to_postgres(**context):
    import pandas as pd
    from sqlalchemy import create_engine

    # Load  final dataset
    df = pd.read_csv("/opt/airflow/dags/data/processed/shipments_with_eta.csv")

    # Connection to PostgreSQL (Docker Airflow default)
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")

    # Save to table
    df.to_sql("shipments_enriched", engine, if_exists="replace", index=False)

    print("Data saved to PostgreSQL successfully")







"------------------------------------------------------------------------------------------------------------------------"
with DAG(
    dag_id="daily_logistics_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    compute_kpis_task = PythonOperator(
        task_id="compute_kpis",
        python_callable=compute_kpis
    )

    detect_alerts_task = PythonOperator(
        task_id="detect_alerts",
        python_callable=detect_alerts
    )

    gps_extract_task = PythonOperator(
    task_id="extract_gps_updates",
    python_callable=extract_gps_updates
)

    merge_task = PythonOperator(
        task_id="merge_with_gps",
        python_callable=merge_with_gps
    )
    compute_eta_task = PythonOperator(
        task_id="compute_eta",
        python_callable=compute_eta
    
    )

    quality_task = PythonOperator(
    task_id="quality_gate",
    python_callable=quality_gate
)
    summary_task = PythonOperator(
    task_id="send_summary",
    python_callable=send_summary
    )

    from airflow.operators.python import PythonOperator

    save_postgres_task = PythonOperator(
        task_id="save_to_postgres",
        python_callable=save_to_postgres
    )



  # Define execution order (pipeline flow)
# Step 1: Extract shipments
    extract_task >> validate_task >> quality_task

    # GPS extraction (independent start)
    gps_extract_task

    # Merge requires BOTH inputs
    quality_task >> merge_task
    gps_extract_task >> merge_task

    # Continue pipeline
    merge_task >> compute_eta_task >> transform_task >> compute_kpis_task >> detect_alerts_task >> summary_task >> save_postgres_task