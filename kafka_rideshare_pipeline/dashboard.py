import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(
    page_title="Real-Time Ride-Sharing Dashboard", layout="wide", page_icon="🚗"
)
st.title("🚗 Real-Time Ride-Sharing Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"


@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)


engine = get_engine(DATABASE_URL)


def load_data(
    status_filter: str | None = None, city_filter: str | None = None, limit: int = 500
) -> pd.DataFrame:
    base_query = "SELECT * FROM trips WHERE 1=1"
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
st.sidebar.markdown("### About")
st.sidebar.info(
    "Real-time streaming pipeline using Apache Kafka, PostgreSQL, and Streamlit"
)

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

        # Display filters
        filter_text = f"Status: {selected_status} | City: {selected_city}"
        st.subheader(f"📊 Displaying {total_trips:,} trips ({filter_text})")

        # KPI Cards
        col1, col2, col3, col4, col5, col6 = st.columns(6)

        col1.metric("🚗 Total Trips", f"{total_trips:,}")
        col2.metric("💰 Total Revenue", f"${total_revenue:,.2f}")
        col3.metric("📏 Total Distance", f"{total_distance:,.1f} km")
        col4.metric("🎫 Avg Fare", f"${avg_fare:,.2f}")
        col5.metric("⭐ Avg Rating", f"{avg_rating:.2f}")
        col6.metric("✅ Completion Rate", f"{completion_rate:.1f}%")

        st.markdown("---")

        # Second row of metrics
        col7, col8, col9, col10 = st.columns(4)
        col7.metric("💵 Avg Tip", f"${avg_tip:,.2f}")
        col8.metric("✅ Completed", completed_trips)
        col9.metric("❌ Cancelled", cancelled_trips)
        col10.metric("⏱️ Avg Duration", f"{df_trips['duration_minutes'].mean():.0f} min")

        st.markdown("---")

        # Recent Trips Table
        st.markdown("### 📋 Recent Trips (Top 10)")
        display_cols = [
            "trip_id",
            "city",
            "pickup_location",
            "dropoff_location",
            "distance_km",
            "fare",
            "tip",
            "total_amount",
            "vehicle_type",
            "status",
            "timestamp",
        ]
        st.dataframe(df_trips[display_cols].head(10), width="stretch")

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
            st.plotly_chart(fig_city, width="stretch", key=f"city_{int(time.time())}")

        with chart_col2:
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

        # Charts Row 2
        chart_col3, chart_col4 = st.columns(2)

        with chart_col3:
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

        with chart_col4:
            # Trip Status Distribution
            status_counts = df_trips.groupby("status").size().reset_index(name="count")
            fig_status = px.pie(
                status_counts,
                values="count",
                names="status",
                title="📊 Trip Status Distribution",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            st.plotly_chart(
                fig_status, width="stretch", key=f"status_{int(time.time())}"
            )

        # Charts Row 3
        chart_col5, chart_col6 = st.columns(2)

        with chart_col5:
            # Distance Distribution
            fig_distance = px.histogram(
                df_trips,
                x="distance_km",
                nbins=30,
                title="📏 Distance Distribution",
                labels={"distance_km": "Distance (km)", "count": "Number of Trips"},
                color_discrete_sequence=["#636EFA"],
            )
            st.plotly_chart(
                fig_distance,
                width="stretch",
                key=f"distance_{int(time.time())}",
            )

        with chart_col6:
            # Top Routes
            df_trips["route"] = (
                df_trips["pickup_location"] + " → " + df_trips["dropoff_location"]
            )
            top_routes = df_trips["route"].value_counts().head(10).reset_index()
            top_routes.columns = ["route", "count"]
            fig_routes = px.bar(
                top_routes,
                y="route",
                x="count",
                orientation="h",
                title="🗺️ Top 10 Routes",
                labels={"count": "Number of Trips", "route": "Route"},
                color="count",
                color_continuous_scale="Oranges",
            )
            fig_routes.update_layout(
                showlegend=False, yaxis={"categoryorder": "total ascending"}
            )
            st.plotly_chart(
                fig_routes, width="stretch", key=f"routes_{int(time.time())}"
            )

        # Time Series Chart (if enough data)
        if len(df_trips) > 10 and "timestamp" in df_trips.columns:
            st.markdown("---")
            df_trips_sorted = df_trips.sort_values("timestamp")
            df_trips_sorted["cumulative_revenue"] = df_trips_sorted[
                "total_amount"
            ].cumsum()

            fig_timeseries = px.line(
                df_trips_sorted,
                x="timestamp",
                y="cumulative_revenue",
                title="📈 Cumulative Revenue Over Time",
                labels={
                    "cumulative_revenue": "Cumulative Revenue ($)",
                    "timestamp": "Time",
                },
            )
            st.plotly_chart(
                fig_timeseries,
                width="stretch",
                key=f"timeseries_{int(time.time())}",
            )

        st.markdown("---")
        st.caption(
            f"⏰ Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} • Auto-refresh: {update_interval}s"
        )

    time.sleep(update_interval)
