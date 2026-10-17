from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = ROOT / "data" / "Global_Cybersecurity_Threats_2015-2024.csv"
ANALYTICS_DIR = ROOT / "output" / "analytics"

st.set_page_config(
    page_title="Global Cybersecurity Threat Analysis",
    page_icon="🔐",
    layout="wide",
)

st.title("GLOBAL CYBERSECURITY THREAT ANALYSIS")
st.caption("Trends, Targets and Impact Analysis | 2015–2024")
st.warning(
    "Research demonstration: this dashboard uses a simulated cybersecurity "
    "dataset and simulated streaming ingestion. It is not a live threat feed."
)


@st.cache_data
def load_data():
    df = pd.read_csv(DATA_FILE)
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    for column in [
        "Financial Loss (in Million $)",
        "Number of Affected Users",
        "Incident Resolution Time (in Hours)",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    return df.dropna(subset=["Year"])


@st.cache_data
def load_pyspark_yearly():
    folder = ANALYTICS_DIR / "attacks_by_year"
    files = list(folder.glob("*.csv"))
    if not files:
        return None
    result = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    return result


df = load_data()

st.sidebar.header("Filter analysis")
years = sorted(df["Year"].dropna().astype(int).unique().tolist())
selected_years = st.sidebar.multiselect(
    "Year", years, default=years
)
countries = sorted(df["Country"].dropna().unique().tolist())
selected_countries = st.sidebar.multiselect(
    "Country", countries, default=countries
)
attack_types = sorted(df["Attack Type"].dropna().unique().tolist())
selected_attacks = st.sidebar.multiselect(
    "Attack type", attack_types, default=attack_types
)

filtered = df[
    df["Year"].isin(selected_years)
    & df["Country"].isin(selected_countries)
    & df["Attack Type"].isin(selected_attacks)
].copy()

if filtered.empty:
    st.info("No records match the selected filters.")
    st.stop()

total = len(filtered)
loss_billion = filtered["Financial Loss (in Million $)"].sum() / 1000
affected = filtered["Number of Affected Users"].sum()
avg_resolution = filtered["Incident Resolution Time (in Hours)"].mean()
country_count = filtered["Country"].nunique()

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Incidents", f"{total:,}")
k2.metric("Financial Loss", f"${loss_billion:,.2f}B")
k3.metric("Affected Users", f"{affected:,.0f}")
k4.metric("Avg Resolution", f"{avg_resolution:,.2f} hrs")
k5.metric("Countries", f"{country_count:,}")

st.divider()
st.subheader("Incident Trends and Attack Distribution")

left, right = st.columns(2)

with left:
    yearly = (
        filtered.groupby("Year", as_index=False)
        .size()
        .rename(columns={"size": "Incident Count"})
        .sort_values("Year")
    )
    fig = px.line(
        yearly,
        x="Year",
        y="Incident Count",
        markers=True,
        title="Cybersecurity Incidents by Year",
    )
    fig.update_layout(xaxis=dict(dtick=1))
    st.plotly_chart(fig, use_container_width=True)

with right:
    types = (
        filtered.groupby("Attack Type", as_index=False)
        .size()
        .rename(columns={"size": "Incident Count"})
        .sort_values("Incident Count", ascending=False)
    )
    fig = px.bar(
        types,
        x="Attack Type",
        y="Incident Count",
        color="Attack Type",
        title="Incidents by Attack Type",
    )
    st.plotly_chart(fig, use_container_width=True)

left, right = st.columns(2)

with left:
    industries = (
        filtered.groupby("Target Industry", as_index=False)
        .size()
        .rename(columns={"size": "Incident Count"})
        .sort_values("Incident Count", ascending=False)
    )
    fig = px.bar(
        industries,
        x="Target Industry",
        y="Incident Count",
        color="Target Industry",
        title="Target Industries",
    )
    st.plotly_chart(fig, use_container_width=True)

with right:
    country_summary = (
        filtered.groupby("Country", as_index=False)
        .agg(
            **{
                "Incident Count": ("Country", "size"),
                "Financial Loss (Million USD)": (
                    "Financial Loss (in Million $)", "sum"
                ),
                "Affected Users": ("Number of Affected Users", "sum"),
            }
        )
        .sort_values("Incident Count", ascending=False)
    )
    fig = px.bar(
        country_summary,
        x="Country",
        y="Incident Count",
        title="Incidents by Country",
        hover_data=[
            "Financial Loss (Million USD)",
            "Affected Users",
        ],
    )
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Financial Impact")
fig = px.scatter(
    filtered,
    x="Financial Loss (in Million $)",
    y="Number of Affected Users",
    color="Attack Type",
    hover_data=["Country", "Year", "Target Industry"],
    title="Financial Loss vs. Affected Users",
)
st.plotly_chart(fig, use_container_width=True)


with st.expander("View filtered source records"):
    st.dataframe(filtered, use_container_width=True)

st.caption(
    "Source: Global_Cybersecurity_Threats_2015-2024.csv. "
    "All reported values describe this dataset only."
)
