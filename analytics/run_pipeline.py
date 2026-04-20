"""
Pipeline Orchestrator for Medical Device Risk Detection System.

Coordinates multiple analytics modules to generate a unified
risk score and actionable insights per device.
"""

from analytics.usage_variability import compute_usage_variability
from analytics.intensity_risk import compute_intensity_score
from analytics.movement_behavior import compute_movement_score

from sqlalchemy import create_engine


# -------------------------------
# Configuration (easy to modify)
# -------------------------------
RISK_WEIGHTS = {
    "usage_variability": 0.4,
    "intensity_score": 0.4,
    "movement_count": 0.2,
}

RISK_THRESHOLDS = {
    "HIGH": 90,
    "MEDIUM": 70,
}

RECOMMENDATIONS = {
    "HIGH": "Immediate inspection required",
    "MEDIUM": "Schedule maintenance soon",
    "LOW": "Normal monitoring",
}

# DB CONFIG 
DB_CONFIG = "mysql+pymysql://root:YOUR_PASSWORD@localhost:3306/medical_db"


# -------------------------------
# Helper Functions
# -------------------------------
def classify_risk(score: float) -> str:
    if score > RISK_THRESHOLDS["HIGH"]:
        return "HIGH"
    elif score > RISK_THRESHOLDS["MEDIUM"]:
        return "MEDIUM"
    return "LOW"


def get_recommendation(level: str) -> str:
    return RECOMMENDATIONS.get(level, "Normal monitoring")


# -------------------------------
# Main Pipeline
# -------------------------------
def run_pipeline():
    print("\n[INFO] Starting Medical Device Risk Pipeline...\n")

    # Step 1: Compute metrics
    usage_df = compute_usage_variability()
    intensity_df = compute_intensity_score()
    movement_df = compute_movement_score()

    # Step 2: Merge datasets
    final_df = (
        usage_df
        .merge(intensity_df, on="device_id")
        .merge(movement_df, on="device_id")
    )

    # Step 3: Compute risk score
    final_df["risk_score"] = (
        RISK_WEIGHTS["usage_variability"] * final_df["usage_variability"] +
        RISK_WEIGHTS["intensity_score"] * final_df["intensity_score"] +
        RISK_WEIGHTS["movement_count"] * final_df["movement_count"]
    )

    # Step 4: Add insights
    final_df["risk_level"] = final_df["risk_score"].apply(classify_risk)
    final_df["recommendation"] = final_df["risk_level"].apply(get_recommendation)

    # Step 5: Sort
    final_df = final_df.sort_values(by="risk_score", ascending=False)

    # -------------------------------
    # Step 6: Save to MySQL 
    # -------------------------------
    engine = create_engine(DB_CONFIG)

    final_df.to_sql(
        name="device_risk_scores",
        con=engine,
        if_exists="replace",   # overwrite each run
        index=False
    )

    # Step 7: Logging
    print("[INFO] Pipeline executed successfully.\n")
    print("[INFO] Top 10 High-Risk Devices:\n")
    print(final_df.head(10).to_string(index=False))

    print(f"\n[INFO] Total devices processed: {len(final_df)}")
    print("[INFO] Output saved to MySQL table: device_risk_scores\n")

    return final_df


# -------------------------------
# Entry Point
# -------------------------------
if __name__ == "__main__":
    run_pipeline()