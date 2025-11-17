import json
import psycopg2
from kafka import KafkaConsumer


def run_consumer():
    """Consumes trip messages from Kafka and inserts them into PostgreSQL."""
    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "trips",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="trips-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")

        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        # Create table for ride-sharing trips
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trips (
                trip_id VARCHAR(50) PRIMARY KEY,
                driver_id VARCHAR(50),
                passenger_count INTEGER,
                city VARCHAR(100),
                pickup_location VARCHAR(200),
                dropoff_location VARCHAR(200),
                distance_km NUMERIC(10, 2),
                duration_minutes INTEGER,
                fare NUMERIC(10, 2),
                tip NUMERIC(10, 2),
                total_amount NUMERIC(10, 2),
                payment_method VARCHAR(50),
                vehicle_type VARCHAR(50),
                driver_rating NUMERIC(3, 2),
                status VARCHAR(50),
                timestamp TIMESTAMP
            );
            """
        )
        print("[Consumer] ✓ Table 'trips' ready.")
        print("[Consumer] 🎧 Listening for trip messages...\n")

        message_count = 0
        for message in consumer:
            try:
                trip_data = message.value

                insert_query = """
                    INSERT INTO trips (
                        trip_id, driver_id, passenger_count, city, 
                        pickup_location, dropoff_location, distance_km, 
                        duration_minutes, fare, tip, total_amount, 
                        payment_method, vehicle_type, driver_rating, 
                        status, timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trip_id) DO NOTHING;
                """
                cur.execute(
                    insert_query,
                    (
                        trip_data["trip_id"],
                        trip_data["driver_id"],
                        trip_data["passenger_count"],
                        trip_data["city"],
                        trip_data["pickup_location"],
                        trip_data["dropoff_location"],
                        trip_data["distance_km"],
                        trip_data["duration_minutes"],
                        trip_data["fare"],
                        trip_data["tip"],
                        trip_data["total_amount"],
                        trip_data["payment_method"],
                        trip_data["vehicle_type"],
                        trip_data["driver_rating"],
                        trip_data["status"],
                        trip_data["timestamp"],
                    ),
                )
                message_count += 1
                print(
                    f"[Consumer] ✓ #{message_count} Inserted trip {trip_data['trip_id']} | {trip_data['city']} | {trip_data['pickup_location']} → {trip_data['dropoff_location']} | ${trip_data['total_amount']}"
                )

            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_consumer()
