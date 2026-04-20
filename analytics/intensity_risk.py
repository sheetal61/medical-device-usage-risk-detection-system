import pandas as pd
from ingestion.loader import load_device_logs


def compute_intensity_score():
    """
    Computes usage intensity score per device.
    """

    df = load_device_logs()

    # ---- LOGIC STEP 1 ----
    # Aggregate total usage & cycles per device
    device_usage = (
        df.groupby("device_id")
        .agg(
            total_usage=("usage_minutes", "sum"),
            total_cycles=("cycles", "sum")
        )
        .reset_index()
    )

    # ---- LOGIC STEP 2 ----
    # Normalize metrics
    device_usage["usage_norm"] = (
        device_usage["total_usage"] / device_usage["total_usage"].mean()
    )
    device_usage["cycles_norm"] = (
        device_usage["total_cycles"] / device_usage["total_cycles"].mean()
    )

    # ---- LOGIC STEP 3 ----
    # Combine into intensity score
    device_usage["intensity_score"] = (
        0.7 * device_usage["usage_norm"]
        + 0.3 * device_usage["cycles_norm"]
    )

    return device_usage[["device_id", "intensity_score"]]


if __name__ == "__main__":
    result = compute_intensity_score()
    print(result.head())
