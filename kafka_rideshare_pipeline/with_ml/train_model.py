import pandas as pd
import numpy as np
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score


def train_cancellation_model():
    """
    Train a model to predict trip cancellations
    """

    print("[Training] Loading data from training_data.csv...")
    df = pd.read_csv("training_data.csv")

    print(f"[Training] Loaded {len(df)} trips")

    # create binary target: 1 = Cancelled, 0 = Not Cancelled
    df["is_cancelled"] = (df["status"] == "Cancelled").astype(int)

    print(f"[Training] Cancellation rate: {df['is_cancelled'].mean():.2%}")

    # feature engineering
    print("[Training] Engineering features...")

    # encode categorical variables
    le_city = LabelEncoder()
    le_vehicle = LabelEncoder()
    le_payment = LabelEncoder()

    df["city_encoded"] = le_city.fit_transform(df["city"])
    df["vehicle_type_encoded"] = le_vehicle.fit_transform(df["vehicle_type"])
    df["payment_method_encoded"] = le_payment.fit_transform(df["payment_method"])

    # time-based features
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    df["is_rush_hour"] = df["hour"].isin([7, 8, 9, 17, 18, 19]).astype(int)

    # additional features
    df["fare_per_km"] = df["fare"] / df["distance_km"].replace(0, 1)
    df["tip_percentage"] = (df["tip"] / df["fare"].replace(0, 1)) * 100

    # select features for model
    feature_columns = [
        "distance_km",
        "duration_minutes",
        "fare",
        "tip",
        "passenger_count",
        "driver_rating",
        "fare_per_km",
        "tip_percentage",
        "city_encoded",
        "vehicle_type_encoded",
        "payment_method_encoded",
        "hour",
        "day_of_week",
        "is_weekend",
        "is_rush_hour",
    ]

    X = df[feature_columns]
    y = df["is_cancelled"]

    # handle any NaN values
    X = X.fillna(0)

    # split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print(f"[Training] Training set: {len(X_train)} samples")
    print(f"[Training] Test set: {len(X_test)} samples")

    # train Random Forest model
    print("[Training] Training Random Forest model...")
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        class_weight="balanced",
    )

    model.fit(X_train, y_train)

    # evaluate
    print("\n[Training] Model Evaluation:")
    y_pred = model.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)
    print(f"Accuracy: {accuracy:.2%}")

    print("\nClassification Report:")
    print(
        classification_report(
            y_test, y_pred, target_names=["Not Cancelled", "Cancelled"]
        )
    )

    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, y_pred))

    # feature importance
    print("\n[Training] Top 10 Most Important Features:")
    feature_importance = pd.DataFrame(
        {"feature": feature_columns, "importance": model.feature_importances_}
    ).sort_values("importance", ascending=False)

    print(feature_importance.head(10).to_string(index=False))

    # save model and encoders
    print("\n[Training] Saving model and encoders...")

    model_artifacts = {
        "model": model,
        "feature_columns": feature_columns,
        "le_city": le_city,
        "le_vehicle": le_vehicle,
        "le_payment": le_payment,
    }

    with open("cancellation_model.pkl", "wb") as f:
        pickle.dump(model_artifacts, f)

    print("[Training] ✓ Model saved to cancellation_model.pkl")
    print(f"[Training] ✓ Model accuracy: {accuracy:.2%}")

    return model, model_artifacts


if __name__ == "__main__":
    train_cancellation_model()
