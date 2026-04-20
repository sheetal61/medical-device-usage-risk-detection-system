import pandas as pd
from ingestion.loader import load_device_logs


def compute_usage_variability():
    """
    Computes usage variability per device.
    Higher variability = more unstable usage pattern.
    """

    # Load clean data
    df = load_device_logs()

    # ---- LOGIC STEP 1 ----
    # Convert timestamp to date (daily aggregation)
    df["date"] = df["timestamp"].dt.date

    # ---- LOGIC STEP 2 ----
    # Aggregate daily usage per device
    daily_usage = (
        df.groupby(["device_id", "date"])["usage_minutes"]
        .sum()
        .reset_index()
    )

    # ---- LOGIC STEP 3 ----
    # Compute standard deviation of daily usage per device
    variability = (
        daily_usage.groupby("device_id")["usage_minutes"]
        .std()
        .reset_index()
        .rename(columns={"usage_minutes": "usage_variability"})
    )

    # Replace NaN (single-day devices) with 0
    variability["usage_variability"] = variability["usage_variability"].fillna(0)

    return variability


if __name__ == "__main__":
    result = compute_usage_variability()
    print(result.head())
