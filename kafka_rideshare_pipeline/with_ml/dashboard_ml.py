import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(
    page_title="🤖 ML-Powered Ride-Sharing Dashboard", layout="wide", page_icon="🚗"
)
st.title("🚗 Real-Time Ride-Sharing Dashboard with ML Predictions")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"


@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)


engine = get_engine(DATABASE_URL)


def load_data(
    status_filter: str | None = None, city_filter: str | None = None, limit: int = 500
) -> pd.DataFrame:
    base_query = "SELECT * FROM trips_with_predictions WHERE 1=1"
    params = {}

    if status_filter and status_filter != "All":
        base_query += " AND status = :status"
        params["status"] = status_filter

    if city_filter and city_filter != "All":
        base_query += " AND city = :city"
        params["city"] = city_filter

    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = limit

    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()


# Sidebar controls
st.sidebar.header("⚙️ Dashboard Controls")

status_options = ["All", "Completed", "Cancelled", "In Progress"]
selected_status = st.sidebar.selectbox("Filter by Status", status_options)

city_options = ["All", "New York", "Los Angeles", "Chicago", "San Francisco", "Miami"]
selected_city = st.sidebar.selectbox("Filter by City", city_options)

update_interval = st.sidebar.slider(
    "Update Interval (seconds)", min_value=2, max_value=20, value=5
)
limit_records = st.sidebar.number_input(
    "Number of records to load", min_value=100, max_value=2000, value=500, step=100
)

if st.sidebar.button("🔄 Refresh Now"):
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("### 🤖 ML Model Info")
st.sidebar.info("Predicts trip cancellation probability using Random Forest classifier")

placeholder = st.empty()

while True:
    df_trips = load_data(selected_status, selected_city, limit=int(limit_records))

    with placeholder.container():
        if df_trips.empty:
            st.warning("⏳ No records found. Waiting for data...")
            time.sleep(update_interval)
            continue

        if "timestamp" in df_trips.columns:
            df_trips["timestamp"] = pd.to_datetime(df_trips["timestamp"])

        # Calculate KPIs
        total_trips = len(df_trips)
        total_revenue = df_trips["total_amount"].sum()
        total_distance = df_trips["distance_km"].sum()
        avg_fare = df_trips["fare"].mean()
        avg_tip = df_trips["tip"].mean()
        avg_rating = df_trips["driver_rating"].mean()
        completed_trips = len(df_trips[df_trips["status"] == "Completed"])
        cancelled_trips = len(df_trips[df_trips["status"] == "Cancelled"])
        completion_rate = (
            (completed_trips / total_trips * 100) if total_trips > 0 else 0.0
        )

        # ML Model Performance Metrics
        df_with_predictions = df_trips.dropna(subset=["cancellation_prediction"])
        if len(df_with_predictions) > 0:
            df_with_predictions["actual_cancelled"] = (
                df_with_predictions["status"] == "Cancelled"
            ).astype(int)
            correct_predictions = (
                df_with_predictions["cancellation_prediction"]
                == df_with_predictions["actual_cancelled"]
            ).sum()
            model_accuracy = (correct_predictions / len(df_with_predictions)) * 100
            avg_cancel_prob = (
                df_with_predictions["cancellation_probability"].mean() * 100
            )
        else:
            model_accuracy = 0
            avg_cancel_prob = 0

        # Display filters
        filter_text = f"Status: {selected_status} | City: {selected_city}"
        st.subheader(f"📊 Displaying {total_trips:,} trips ({filter_text})")

        # KPI Cards - Row 1
        col1, col2, col3, col4, col5, col6 = st.columns(6)

        col1.metric("🚗 Total Trips", f"{total_trips:,}")
        col2.metric("💰 Total Revenue", f"${total_revenue:,.2f}")
        col3.metric("📏 Total Distance", f"{total_distance:,.1f} km")
        col4.metric("🎫 Avg Fare", f"${avg_fare:,.2f}")
        col5.metric("⭐ Avg Rating", f"{avg_rating:.2f}")
        col6.metric("✅ Completion Rate", f"{completion_rate:.1f}%")

        # ML Metrics - Row 2
        st.markdown("### 🤖 ML Model Performance")
        ml_col1, ml_col2, ml_col3, ml_col4 = st.columns(4)

        ml_col1.metric("🎯 Model Accuracy", f"{model_accuracy:.1f}%")
        ml_col2.metric("📊 Predictions Made", len(df_with_predictions))
        ml_col3.metric("⚠️ Avg Cancel Risk", f"{avg_cancel_prob:.1f}%")
        ml_col4.metric("❌ Actual Cancellations", cancelled_trips)

        st.markdown("---")

        # Recent Trips with Predictions
        st.markdown("### 📋 Recent Trips with ML Predictions (Top 10)")
        display_cols = [
            "trip_id",
            "city",
            "pickup_location",
            "dropoff_location",
            "distance_km",
            "fare",
            "total_amount",
            "vehicle_type",
            "status",
            "cancellation_probability",
            "timestamp",
        ]

        display_df = df_trips[display_cols].head(10).copy()
        if "cancellation_probability" in display_df.columns:
            display_df["cancellation_probability"] = display_df[
                "cancellation_probability"
            ].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")

        st.dataframe(display_df, width="stretch")

        st.markdown("---")

        # Charts Row 1
        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            # Revenue by City
            city_revenue = df_trips.groupby("city")["total_amount"].sum().reset_index()
            city_revenue = city_revenue.sort_values("total_amount", ascending=False)
            fig_city = px.bar(
                city_revenue,
                x="city",
                y="total_amount",
                title="💰 Revenue by City",
                labels={"total_amount": "Revenue ($)", "city": "City"},
                color="total_amount",
                color_continuous_scale="Blues",
            )
            fig_city.update_layout(showlegend=False)
            st.plotly_chart(
                fig_city,
                width="stretch",
                key=f"city_revenue_{int(time.time())}",
            )

        with chart_col2:
            # ML: Prediction vs Actual
            if len(df_with_predictions) > 0:
                confusion_data = pd.DataFrame(
                    {
                        "Predicted": df_with_predictions["cancellation_prediction"].map(
                            {0: "Not Cancelled", 1: "Cancelled"}
                        ),
                        "Actual": df_with_predictions["actual_cancelled"].map(
                            {0: "Not Cancelled", 1: "Cancelled"}
                        ),
                    }
                )

                confusion_counts = (
                    confusion_data.groupby(["Actual", "Predicted"])
                    .size()
                    .reset_index(name="Count")
                )

                fig_confusion = px.bar(
                    confusion_counts,
                    x="Actual",
                    y="Count",
                    color="Predicted",
                    title="🤖 ML Predictions vs Actual Outcomes",
                    barmode="group",
                    color_discrete_map={
                        "Not Cancelled": "#90EE90",
                        "Cancelled": "#FFB6C6",
                    },
                )
                st.plotly_chart(
                    fig_confusion,
                    width="stretch",
                    key=f"confusion_{int(time.time())}",
                )
            else:
                st.info("Waiting for ML predictions...")

        # Charts Row 2
        chart_col3, chart_col4 = st.columns(2)

        with chart_col3:
            # Cancellation Probability Distribution
            if len(df_with_predictions) > 0:
                fig_prob_dist = px.histogram(
                    df_with_predictions,
                    x="cancellation_probability",
                    nbins=20,
                    title="📊 Cancellation Probability Distribution",
                    labels={"cancellation_probability": "Cancellation Probability"},
                    color_discrete_sequence=["#FF6B6B"],
                )
                fig_prob_dist.update_layout(showlegend=False)
                st.plotly_chart(
                    fig_prob_dist,
                    width="stretch",
                    key=f"prob_dist_{int(time.time())}",
                )
            else:
                st.info("Waiting for probability data...")

        with chart_col4:
            # High Risk Trips
            if len(df_with_predictions) > 0:
                high_risk = df_with_predictions[
                    df_with_predictions["cancellation_probability"] > 0.5
                ]
                risk_by_city = (
                    high_risk.groupby("city").size().reset_index(name="count")
                )

                if len(risk_by_city) > 0:
                    fig_risk = px.pie(
                        risk_by_city,
                        values="count",
                        names="city",
                        title="⚠️ High-Risk Trips by City (>50% cancel prob)",
                        color_discrete_sequence=px.colors.sequential.Reds_r,
                    )
                    st.plotly_chart(
                        fig_risk,
                        width="stretch",
                        key=f"risk_{int(time.time())}",
                    )
                else:
                    st.info("No high-risk trips detected")
            else:
                st.info("Waiting for risk assessment data...")

        # Charts Row 3
        chart_col5, chart_col6 = st.columns(2)

        with chart_col5:
            # Trips by Vehicle Type
            vehicle_counts = (
                df_trips.groupby("vehicle_type").size().reset_index(name="count")
            )
            fig_vehicle = px.pie(
                vehicle_counts,
                values="count",
                names="vehicle_type",
                title="🚙 Trips by Vehicle Type",
                hole=0.4,
            )
            st.plotly_chart(
                fig_vehicle, width="stretch", key=f"vehicle_{int(time.time())}"
            )

        with chart_col6:
            # Payment Methods Distribution
            payment_dist = (
                df_trips.groupby("payment_method")["total_amount"].sum().reset_index()
            )
            payment_dist = payment_dist.sort_values("total_amount", ascending=False)
            fig_payment = px.bar(
                payment_dist,
                x="payment_method",
                y="total_amount",
                title="💳 Revenue by Payment Method",
                labels={
                    "total_amount": "Revenue ($)",
                    "payment_method": "Payment Method",
                },
                color="total_amount",
                color_continuous_scale="Greens",
            )
            fig_payment.update_layout(showlegend=False)
            st.plotly_chart(
                fig_payment, width="stretch", key=f"payment_{int(time.time())}"
            )

        # Model Performance Over Time
        if len(df_with_predictions) > 10:
            st.markdown("---")
            st.markdown("### 📈 Model Performance Over Time")

            df_time = df_with_predictions.sort_values("timestamp").copy()
            df_time["correct"] = (
                df_time["cancellation_prediction"] == df_time["actual_cancelled"]
            ).astype(int)
            df_time["rolling_accuracy"] = (
                df_time["correct"].rolling(window=20, min_periods=1).mean() * 100
            )

            fig_accuracy = px.line(
                df_time,
                x="timestamp",
                y="rolling_accuracy",
                title="🎯 Rolling Model Accuracy (20-trip window)",
                labels={"rolling_accuracy": "Accuracy (%)", "timestamp": "Time"},
            )
            fig_accuracy.add_hline(
                y=50,
                line_dash="dash",
                line_color="red",
                annotation_text="Random Guess (50%)",
            )
            st.plotly_chart(
                fig_accuracy,
                width="stretch",
                key=f"accuracy_{int(time.time())}",
            )

        st.markdown("---")
        st.caption(
            f"⏰ Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} • Auto-refresh: {update_interval}s"
        )
        st.caption("🤖 Powered by Random Forest ML Model")

    time.sleep(update_interval)
