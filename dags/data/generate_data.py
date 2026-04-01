from pathlib import Path
import pandas as pd
import random
from datetime import datetime, timedelta

# -----------------------------
# CONFIGURATION
# -----------------------------
NUM_ROWS = 500
BASE_DATE = datetime(2026, 3, 30)

CITIES = ["Brussels", "Paris", "Liege", "Metz", "Reims", "Nancy"]
STATUS = ["Delivered", "In Transit"]
CARGO_TYPES = ["General", "Pharma", "Chemicals", "Food"]

# Reproducibility
random.seed(42)


# -----------------------------
# DATA GENERATION FUNCTION
# -----------------------------
def generate_shipments(n_rows: int) -> pd.DataFrame:
    rows = []

    for i in range(1, n_rows + 1):
        # Planned times
        planned_dep = BASE_DATE.replace(hour=random.randint(5, 15), minute=0)
        planned_arr = planned_dep + timedelta(hours=4)

        # Actual departure (always >= planned)
        actual_dep = planned_dep + timedelta(minutes=random.randint(0, 40))

        # Status selection
        status = random.choice(STATUS)

        # -----------------------------
        # LOGICAL CONSISTENCY RULES
        # -----------------------------
        if status == "Delivered":
            # Delivered shipments must have arrival time
            actual_arr = planned_arr + timedelta(minutes=random.randint(-10, 60))

            # Ensure arrival is always after departure
            if actual_arr < actual_dep:
                actual_arr = actual_dep + timedelta(minutes=random.randint(10, 60))

        else:
            # In Transit → no arrival yet
            actual_arr = None

        row = {
            "shipment_id": f"S{i:03}",
            "customer_id": f"C{random.randint(1, 20):03}",
            "route_id": f"R{random.randint(1, 10):03}",
            "vehicle_id": f"V{random.randint(1, 10):03}",
            "driver_id": f"D{random.randint(1, 10):03}",
            "planned_departure": planned_dep,
            "actual_departure": actual_dep,
            "planned_arrival": planned_arr,
            "actual_arrival": actual_arr,
            "origin": "Luxembourg",
            "destination": random.choice(CITIES),
            "distance_km": random.randint(50, 400),
            "fuel_cost": random.randint(20, 150),
            "shipment_revenue": random.randint(100, 700),
            "shipment_status": status,
            "cargo_type": random.choice(CARGO_TYPES),
        }

        rows.append(row)

    return pd.DataFrame(rows)


# -----------------------------
# SAVE FUNCTION
# -----------------------------
def save_to_csv(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


# -----------------------------
# MAIN EXECUTION
# -----------------------------
def main():
    base_dir = Path(__file__).resolve().parent
    output_file = base_dir / "raw" / "shipments.csv"

    df = generate_shipments(NUM_ROWS)
    save_to_csv(df, output_file)

    print(f"Dataset generated at: {output_file}")
    print(f"Total rows: {len(df)}")


if __name__ == "__main__":
    main()