import streamlit as st
# import duckdb
import pandas as pd
import plotly.express as px

# DB_PATH = "/Users/bliu/LearningProjects/polymarket-pipeline/data/polymarket.duckdb"

st.set_page_config(page_title="Polymarket Analytics", layout="wide")
st.title("Polymarket Prediction Market Analytics")
st.markdown("Built with Python, PySpark, dbt, DuckDB, and Streamlit")

@st.cache_data
def load_data():
    '''
    conn = duckdb.connect(DB_PATH, read_only=True)
    categories = conn.execute("SELECT * FROM fct_category_volume").df()
    top_markets = conn.execute("SELECT * FROM fct_top_markets").df()
    timing = conn.execute("SELECT * FROM fct_market_timing").df()
    conn.close()
    '''
    categories = pd.read_csv("data/exports/fct_category_volume.csv")
    top_markets = pd.read_csv("data/exports/fct_top_markets.csv")
    timing = pd.read_csv("data/exports/fct_market_timing.csv")
    return categories, top_markets, timing


categories, top_markets, timing = load_data()

# KPI Cards
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Markets", f"{top_markets['market_id'].count():,}")
col2.metric("Total Volume", f"${categories['total_volume'].sum():,.0f}")
col3.metric("24hr Volume", f"${categories['total_volume_24hr'].sum():,.0f}")
col4.metric("Avg Liquidity", f"${categories['avg_liquidity'].mean():,.0f}")

st.divider()

# Volume by Category
st.subheader("Total Volume by Category")
fig1 = px.bar(
    categories.sort_values("total_volume", ascending=True),
    x="total_volume", y="category",
    orientation="h",
    color="category",
    text="total_markets"
)
st.plotly_chart(fig1, use_container_width=True)

# 24hr Volume vs Total Volume
st.subheader("24hr Volume vs Total Volume by Category")
fig2 = px.scatter(
    categories,
    x="total_volume",
    y="total_volume_24hr",
    size="avg_liquidity",
    color="category",
    text="category",
    title="Bubble size = avg liquidity"
)
st.plotly_chart(fig2, use_container_width=True)

# Top 20 markets
st.subheader("Top 20 Markets by Volume")
top20 = top_markets.head(20)[["question", "category", "volume", "volume_24hr", "days_until_close"]]
top20["volume"] = top20["volume"].apply(lambda x: f"${x:,.0f}")
top20["volume_24hr"] = top20["volume_24hr"].apply(lambda x: f"${x:,.0f}")
st.dataframe(top20, use_container_width=True)

# Market timing
st.subheader("Markets Closing Soon by Category")
fig3 = px.bar(
    timing,
    x="category",
    y=["closing_this_week", "closing_this_month", "closing_later"],
    barmode="stack",
    title="Market Closing Timeline"
)
st.plotly_chart(fig3, use_container_width=True)

# Volume to liquidity ratio
st.subheader("Volume to Liquidity Ratio by Category")
st.markdown("Higher ratio = more trading activity relative to available liquidity")
fig4 = px.bar(
    categories.sort_values("avg_vol_liq_ratio", ascending=False),
    x="category",
    y="avg_vol_liq_ratio",
    color="category"
)
st.plotly_chart(fig4, use_container_width=True)
