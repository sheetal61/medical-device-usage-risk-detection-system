import pandas as pd
from ingestion.loader import load_device_logs


def compute_movement_score():
    """
    Computes movement frequency per device.
    """

    df = load_device_logs()

    # Sort events by time
    df = df.sort_values(["device_id", "timestamp"])

    # Detect location change
    df["prev_location"] = df.groupby("device_id")["location"].shift(1)
    df["moved"] = df["location"] != df["prev_location"]

    # First record per device is not a movement
    df["moved"] = df["moved"].fillna(False)

    # Count movements per device
    movement = (
        df.groupby("device_id")["moved"]
        .sum()
        .reset_index()
        .rename(columns={"moved": "movement_count"})
    )

    return movement


if __name__ == "__main__":
    result = compute_movement_score()
    print(result.head())
