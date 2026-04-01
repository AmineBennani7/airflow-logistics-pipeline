from pathlib import Path
import pandas as pd
import random
from datetime import datetime, timedelta
import math


# -----------------------------
# CONFIGURATION
# -----------------------------
NUM_SHIPMENTS = 500
POINTS_PER_SHIPMENT = 10
BASE_DATE = datetime(2026, 3, 30, 6, 0)

GPS_STATUS = ["On Route", "Delayed", "Stopped"]

LAT_RANGE = (49.5, 50.0)
LON_RANGE = (5.5, 6.5)

random.seed(42)


# -----------------------------
# HELPER FUNCTION
# -----------------------------
def move_towards(lat, lon, target_lat, target_lon, step_size):
    """
    Move from (lat, lon) towards (target_lat, target_lon)
    with a given step size.
    """
    direction_lat = target_lat - lat
    direction_lon = target_lon - lon

    distance = math.sqrt(direction_lat**2 + direction_lon**2)

    if distance == 0:
        return lat, lon

    # Normalize direction
    direction_lat /= distance
    direction_lon /= distance

    # Move in that direction
    lat += direction_lat * step_size
    lon += direction_lon * step_size

    return lat, lon


# -----------------------------
# DATA GENERATION FUNCTION
# -----------------------------
def generate_gps_data(num_shipments: int, points_per_shipment: int) -> pd.DataFrame:
    rows = []

    for i in range(1, num_shipments + 1):
        shipment_id = f"S{i:03}"
        vehicle_id = f"V{random.randint(1, 10):03}"

        # Start and destination points
        start_lat = random.uniform(*LAT_RANGE)
        start_lon = random.uniform(*LON_RANGE)

        target_lat = random.uniform(*LAT_RANGE)
        target_lon = random.uniform(*LON_RANGE)

        lat, lon = start_lat, start_lon

        timestamp = BASE_DATE

        # Initial remaining distance
        remaining_km = random.randint(150, 300)

        for _ in range(points_per_shipment):

            status = random.choice(GPS_STATUS)

            if status == "Stopped":
                # Very small movement (vehicle is almost stationary)
                lat += random.uniform(-0.0005, 0.0005)
                lon += random.uniform(-0.0005, 0.0005)

                distance_reduction = random.randint(0, 2)

            else:
                # Move towards destination
                step_size = random.uniform(0.002, 0.008)
                lat, lon = move_towards(lat, lon, target_lat, target_lon, step_size)

                # Reduce distance more when moving
                distance_reduction = random.randint(5, 20)

            # Update timestamp (consistent interval)
            timestamp += timedelta(minutes=random.randint(5, 15))

            # Update remaining distance
            remaining_km = max(0, remaining_km - distance_reduction)

            row = {
                "shipment_id": shipment_id,
                "vehicle_id": vehicle_id,
                "gps_timestamp": timestamp,
                "current_latitude": round(lat, 5),
                "current_longitude": round(lon, 5),
                "gps_status": status,
                "estimated_remaining_km": remaining_km,
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
# MAIN
# -----------------------------
def main():
    base_dir = Path(__file__).resolve().parent
    output_file = base_dir / "raw" / "gps_updates.csv"

    df = generate_gps_data(NUM_SHIPMENTS, POINTS_PER_SHIPMENT)
    save_to_csv(df, output_file)

    print(f"GPS dataset generated at: {output_file}")
    print(f"Total rows: {len(df)}")


if __name__ == "__main__":
    main()