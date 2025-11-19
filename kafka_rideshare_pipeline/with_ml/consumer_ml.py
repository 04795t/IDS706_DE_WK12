import json
import psycopg2
import pickle
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime


def load_model():
    """
    Load the trained model and encoders
    """
    try:
        with open("cancellation_model.pkl", "rb") as f:
            model_artifacts = pickle.load(f)
        print("[Consumer] ✓ ML model loaded successfully")
        return model_artifacts
    except FileNotFoundError:
        print("[Consumer] No model found. Run train_model.py first.")
        return None


def predict_cancellation(trip_data, model_artifacts):
    """
    Predict cancellation probability for a trip
    """
    if model_artifacts is None:
        return None, 0.0

    model = model_artifacts["model"]
    feature_columns = model_artifacts["feature_columns"]
    le_city = model_artifacts["le_city"]
    le_vehicle = model_artifacts["le_vehicle"]
    le_payment = model_artifacts["le_payment"]

    # Parse timestamp
    timestamp = pd.to_datetime(trip_data["timestamp"])

    # Encode categorical variables
    try:
        city_encoded = le_city.transform([trip_data["city"]])[0]
    except:
        city_encoded = 0

    try:
        vehicle_encoded = le_vehicle.transform([trip_data["vehicle_type"]])[0]
    except:
        vehicle_encoded = 0

    try:
        payment_encoded = le_payment.transform([trip_data["payment_method"]])[0]
    except:
        payment_encoded = 0

    # Calculate derived features
    fare_per_km = trip_data["fare"] / max(trip_data["distance_km"], 1)
    tip_percentage = (trip_data["tip"] / max(trip_data["fare"], 1)) * 100

    # Time features
    hour = timestamp.hour
    day_of_week = timestamp.dayofweek
    is_weekend = 1 if day_of_week in [5, 6] else 0
    is_rush_hour = 1 if hour in [7, 8, 9, 17, 18, 19] else 0

    # Create feature vector
    features = {
        "distance_km": trip_data["distance_km"],
        "duration_minutes": trip_data["duration_minutes"],
        "fare": trip_data["fare"],
        "tip": trip_data["tip"],
        "passenger_count": trip_data["passenger_count"],
        "driver_rating": trip_data["driver_rating"],
        "fare_per_km": fare_per_km,
        "tip_percentage": tip_percentage,
        "city_encoded": city_encoded,
        "vehicle_type_encoded": vehicle_encoded,
        "payment_method_encoded": payment_encoded,
        "hour": hour,
        "day_of_week": day_of_week,
        "is_weekend": is_weekend,
        "is_rush_hour": is_rush_hour,
    }

    # Create DataFrame with correct column order
    X = pd.DataFrame([features])[feature_columns]

    # Predict
    prediction = model.predict(X)[0]
    probability = model.predict_proba(X)[0][1]

    return prediction, probability


def run_consumer_with_ml():
    """
    Consumes trip messages from Kafka and inserts them into PostgreSQL with ML predictions
    """

    # Load ML model
    model_artifacts = load_model()

    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "trips",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="trips-consumer-ml-group",
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

        # Create enhanced table with ML predictions
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trips_with_predictions (
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
                timestamp TIMESTAMP,
                cancellation_prediction INTEGER,
                cancellation_probability NUMERIC(5, 4)
            );
            """
        )
        print("[Consumer] ✓ Table 'trips_with_predictions' ready.")
        print("[Consumer] 🎧 Listening for trip messages with ML predictions...\n")

        message_count = 0
        for message in consumer:
            try:
                trip_data = message.value

                # Make prediction
                prediction, probability = predict_cancellation(
                    trip_data, model_artifacts
                )

                # Convert numpy types to Python native types for PostgreSQL
                if prediction is not None:
                    prediction = int(prediction)  # Convert numpy.int64 to Python int
                if probability is not None:
                    probability = float(
                        probability
                    )  # Convert numpy.float64 to Python float

                insert_query = """
                    INSERT INTO trips_with_predictions (
                        trip_id, driver_id, passenger_count, city, 
                        pickup_location, dropoff_location, distance_km, 
                        duration_minutes, fare, tip, total_amount, 
                        payment_method, vehicle_type, driver_rating, 
                        status, timestamp,
                        cancellation_prediction, cancellation_probability
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        prediction if prediction is not None else None,
                        probability if probability is not None else None,
                    ),
                )
                message_count += 1

                # Enhanced logging with prediction
                pred_str = (
                    f"| 🤖 Cancel Risk: {probability:.1%}"
                    if prediction is not None
                    else ""
                )
                actual_status = (
                    "× CANCELLED" if trip_data["status"] == "Cancelled" else "✓"
                )

                print(
                    f"[Consumer] #{message_count} {actual_status} {trip_data['trip_id']} | {trip_data['city']} | ${trip_data['total_amount']} {pred_str}"
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
    run_consumer_with_ml()
