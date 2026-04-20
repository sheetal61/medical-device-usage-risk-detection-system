import pandas as pd
import mysql.connector


def load_device_logs():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="YOUR_PASSWORD",
        database="medical_db"
    )

    query = "SELECT * FROM device_logs"
    df = pd.read_sql(query, conn)

    conn.close()

    # Convert timestamp properly
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    return df