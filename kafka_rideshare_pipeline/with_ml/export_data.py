import pandas as pd
from sqlalchemy import create_engine

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"


def export_training_data():
    """
    Export trips data from PostgreSQL
    """
    print("[Export] Connecting to PostgreSQL...")
    engine = create_engine(DATABASE_URL)

    query = "SELECT * FROM trips ORDER BY timestamp DESC LIMIT 1000"

    df = pd.read_sql_query(query, engine)
    print(f"[Export] Retrieved {len(df)} trips from database")

    # Save to CSV
    df.to_csv("training_data.csv", index=False)
    print(f"[Export] ✓ Saved to training_data.csv")

    # display summary
    print(f"\n[Export] Data Summary:")
    print(f"  - Total trips: {len(df)}")
    print(f"  - Completed: {len(df[df['status'] == 'Completed'])}")
    print(f"  - Cancelled: {len(df[df['status'] == 'Cancelled'])}")
    print(f"  - In Progress: {len(df[df['status'] == 'In Progress'])}")
    print(f"  - Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

    return df


if __name__ == "__main__":
    export_training_data()
