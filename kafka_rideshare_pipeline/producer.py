import time
import json
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()


def generate_synthetic_trip():
    """
    Generates synthetic ride-sharing trip data
    """

    # ride-sharing specific data
    vehicle_types = ["Economy", "Comfort", "Premium", "SUV", "Electric"]
    statuses = ["Completed", "Cancelled", "In Progress"]

    # major US cities with neighborhoods
    locations = {
        "New York": [
            "Manhattan",
            "Brooklyn",
            "Queens",
            "Bronx",
            "Staten Island",
            "JFK Airport",
            "LaGuardia Airport",
        ],
        "Los Angeles": [
            "Downtown",
            "Hollywood",
            "Santa Monica",
            "Beverly Hills",
            "LAX Airport",
            "Venice Beach",
        ],
        "Chicago": [
            "Loop",
            "River North",
            "Lincoln Park",
            "Wicker Park",
            "O'Hare Airport",
            "Navy Pier",
        ],
        "San Francisco": [
            "Downtown",
            "Mission District",
            "Marina",
            "Financial District",
            "SFO Airport",
            "Fisherman's Wharf",
        ],
        "Miami": [
            "South Beach",
            "Downtown",
            "Brickell",
            "Wynwood",
            "MIA Airport",
            "Coral Gables",
        ],
    }

    payment_methods = [
        "Credit Card",
        "Debit Card",
        "Digital Wallet",
        "Cash",
        "Corporate Account",
    ]

    # city and locations
    city = random.choice(list(locations.keys()))
    pickup = random.choice(locations[city])
    dropoff = random.choice([loc for loc in locations[city] if loc != pickup])

    # Generate realistic trip metrics
    distance_km = round(random.uniform(2.0, 35.0), 2)
    duration_minutes = int(distance_km * random.uniform(2.5, 4.5))  # Variable speed

    # pricing logic
    base_fare = 5.0
    per_km_rate = random.uniform(1.5, 3.0)
    fare = round(base_fare + (distance_km * per_km_rate), 2)

    # surge pricing (20% chance)
    if random.random() < 0.2:
        surge_multiplier = random.uniform(1.3, 2.5)
        fare = round(fare * surge_multiplier, 2)

    # tip (70% of rides get tips)
    tip = round(random.uniform(2.0, 10.0), 2) if random.random() < 0.7 else 0.0

    status = random.choice(statuses)
    vehicle_type = random.choice(vehicle_types)
    payment_method = random.choice(payment_methods)
    passenger_count = random.randint(1, 4)

    # driver rating
    driver_rating = round(random.uniform(4.5, 5.0), 2)

    return {
        "trip_id": str(uuid.uuid4())[:8],
        "driver_id": f"DRV-{random.randint(1000, 9999)}",
        "passenger_count": passenger_count,
        "city": city,
        "pickup_location": pickup,
        "dropoff_location": dropoff,
        "distance_km": distance_km,
        "duration_minutes": duration_minutes,
        "fare": fare,
        "tip": tip,
        "total_amount": round(fare + tip, 2),
        "payment_method": payment_method,
        "vehicle_type": vehicle_type,
        "driver_rating": driver_rating,
        "status": status,
        "timestamp": datetime.now().isoformat(),
    }


def run_producer():
    """
    Kafka producer that sends synthetic ride-sharing trips to the 'trips' topic
    """
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")
        print("[Producer] 🚗 Starting to generate ride-sharing trips...\n")

        count = 0
        while True:
            trip = generate_synthetic_trip()
            print(
                f"[Producer] Sending trip #{count}: {trip['city']} | {trip['pickup_location']} → {trip['dropoff_location']} | ${trip['total_amount']}"
            )

            future = producer.send("trips", value=trip)
            record_metadata = future.get(timeout=10)
            print(
                f"[Producer] ✓ Sent to partition {record_metadata.partition} at offset {record_metadata.offset}"
            )

            producer.flush()
            count += 1

            # Random interval between trips (0.5-2 seconds)
            sleep_time = random.uniform(0.5, 2.0)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_producer()
