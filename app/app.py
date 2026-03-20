# app.py — Stock Market Intelligence App
# Databricks Apps + Streamlit + Claude API

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import anthropic
from databricks import sql
import os
import warnings
warnings.filterwarnings("ignore")

# ── Page Config ─────────────────────────────────
st.set_page_config(
    page_title = "Stock Market Intelligence",
    page_icon  = "📈",
    layout     = "wide"
)

# ── Custom CSS ───────────────────────────────────
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stMetric { background-color: #1e2130; padding: 10px; border-radius: 8px; }
    .stTabs [data-baseweb="tab"] { font-size: 16px; font-weight: bold; }
    </style>
""", unsafe_allow_html=True)

# ── Spark Session ────────────────────────────────
# ── Databricks SQL Connection ────────────────────
# @st.cache_resource
# def get_connection():
#     return sql.connect(
#         server_hostname = os.getenv("DATABRICKS_HOST"),
#         http_path       = os.getenv("DATABRICKS_WAREHOUSE_PATH"),
#         credentials_provider = lambda: oauth_machine_to_machine(
#             host          = os.getenv("DATABRICKS_HOST"),
#             client_id     = os.getenv("DATABRICKS_CLIENT_ID"),
#             client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
#         )
#     )


# @st.cache_resource
# def get_claude():
#     api_key = dbutils.secrets.get(         # ← secure, never hardcoded
#         scope = "app_secrets",
#         key   = "claude_api_key"
#     )
#     return anthropic.Anthropic(api_key=api_key)

# ── Claude Client ────────────────────────────────

@st.cache_resource
def get_claude():
    api_key = os.environ["CLAUDE_API_KEY"]
    return anthropic.Anthropic(api_key=api_key)
    
client = get_claude()

# ── Data Loading Functions ───────────────────────
# @st.cache_data(ttl=3600)
# def load_daily_prices(ticker, start_date, end_date):
#     return (
#         spark.table("stock_analytics.gold.daily_prices")
#             .filter(F.col("ticker")     == ticker)
#             .filter(F.col("trade_date") >= start_date)
#             .filter(F.col("trade_date") <= end_date)
#             .orderBy("trade_date")
#             .toPandas()
#     )

# @st.cache_data(ttl=3600)
# def load_all_prices(start_date, end_date):
#     return (
#         spark.table("stock_analytics.gold.daily_prices")
#             .filter(F.col("trade_date") >= start_date)
#             .filter(F.col("trade_date") <= end_date)
#             .orderBy("ticker", "trade_date")
#             .toPandas()
#     )

# @st.cache_data(ttl=3600)
# def load_volatility():
#     return (
#         spark.table("stock_analytics.gold.stock_volatility")
#             .orderBy("volatility_stddev", ascending=False)
#             .toPandas()
#     )

# @st.cache_data(ttl=3600)
# def load_monthly(start_month):
#     return (
#         spark.table("stock_analytics.gold.monthly_performance")
#             .filter(F.col("month") >= start_month)
#             .orderBy("month", "rank")
#             .toPandas()
#     )

# @st.cache_data(ttl=3600)
# def build_ai_context():
#     vol_summary = (
#         spark.table("stock_analytics.gold.stock_volatility")
#             .toPandas()
#             .to_string(index=False)
#     )
#     monthly_summary = (
#         spark.table("stock_analytics.gold.monthly_performance")
#             .filter(F.col("month") >= "2024-01")
#             .filter(F.col("rank")  <= 3)
#             .orderBy("month", "rank")
#             .toPandas()
#             .to_string(index=False)
#     )
#     latest_prices = (
#         spark.table("stock_analytics.gold.daily_prices")
#             .groupBy("ticker")
#             .agg(
#                 F.max("trade_date")  .alias("latest_date"),
#                 F.last("close")      .alias("latest_close"),
#                 F.last("daily_return_pct").alias("latest_return")
#             )
#             .orderBy("ticker")
#             .toPandas()
#             .to_string(index=False)
#     )
#     return f"""

@st.cache_resource
def get_conn():
    # These are auto-injected by Databricks Apps — no manual setup needed!
    host          = os.environ["DATABRICKS_HOST"]
    client_id     = os.environ["DATABRICKS_CLIENT_ID"]
    client_secret = os.environ["DATABRICKS_CLIENT_SECRET"]

    # Get warehouse HTTP path
    http_path = "/sql/1.0/warehouses/c5119fd62b692e9b"  # ← fill this in

    credentials = ClientCredentials(
        host          = f"https://{host}",
        client_id     = client_id,
        client_secret = client_secret,
        scopes        = ["all-apis"]
    )

    return sql.connect(
        server_hostname      = host,
        http_path            = http_path,
        credentials_provider = lambda: credentials
    )

def run_query(query: str) -> pd.DataFrame:
    conn   = get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=cols)

@st.cache_data(ttl=3600)
def load_daily_prices(ticker, start_date, end_date):
    return run_query(f"""
        SELECT ticker, trade_date, open, high, low, close,
               volume, daily_return_pct, price_range,
               moving_avg_7d, moving_avg_30d
        FROM stock_analytics.gold.daily_prices
        WHERE ticker     = '{ticker}'
          AND trade_date >= '{start_date}'
          AND trade_date <= '{end_date}'
        ORDER BY trade_date
    """)

@st.cache_data(ttl=3600)
def load_all_prices(start_date, end_date):
    return run_query(f"""
        SELECT ticker, trade_date, close, moving_avg_7d
        FROM stock_analytics.gold.daily_prices
        WHERE trade_date >= '{start_date}'
          AND trade_date <= '{end_date}'
        ORDER BY ticker, trade_date
    """)

@st.cache_data(ttl=3600)
def load_volatility():
    return run_query("""
        SELECT ticker, volatility_stddev, avg_daily_return,
               best_day_return, worst_day_return,
               total_trading_days, risk_tier,
               data_from, data_to
        FROM stock_analytics.gold.stock_volatility
        ORDER BY volatility_stddev DESC
    """)

@st.cache_data(ttl=3600)
def load_monthly(start_month):
    return run_query(f"""
        SELECT ticker, month, monthly_return_pct,
               best_single_day, worst_single_day,
               avg_close, trading_days, rank
        FROM stock_analytics.gold.monthly_performance
        WHERE month >= '{start_month}'
        ORDER BY month, rank
    """)

@st.cache_data(ttl=3600)
def build_ai_context():
    vol     = run_query("SELECT * FROM stock_analytics.gold.stock_volatility").to_string(index=False)
    monthly = run_query("""
        SELECT * FROM stock_analytics.gold.monthly_performance
        WHERE month >= '2024-01' AND rank <= 3
        ORDER BY month, rank
    """).to_string(index=False)
    latest  = run_query("""
        SELECT ticker, MAX(trade_date) as latest_date,
               LAST_VALUE(close) OVER (
                   PARTITION BY ticker ORDER BY trade_date
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
               ) as latest_close
        FROM stock_analytics.gold.daily_prices
        GROUP BY ticker, close, trade_date
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) = 1
    """).to_string(index=False)

    return f"""
You are a financial data analyst assistant with access to 
S&P 500 stock market data from January 2023 to March 2026.

Stocks covered: AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, BRK-B, JPM, V

VOLATILITY & RISK DATA:
{vol_summary}

MONTHLY PERFORMANCE (Top 3 per month, 2024 onwards):
{monthly_summary}

LATEST PRICES:
{latest_prices}

Rules:
- Answer accurately based only on the data provided
- Be concise and use bullet points where helpful  
- Format numbers clearly ($182.50, +3.2%)
- If something is outside the data range say so clearly
- Always mention the time period when discussing performance
    """

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# APP HEADER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
st.title("📈 Stock Market Intelligence")
st.caption("Powered by Databricks Delta Lake + Claude AI | S&P 500 Top 10 Stocks")
st.divider()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SIDEBAR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with st.sidebar:
    st.header("⚙️ Filters")
    st.divider()

    selected_ticker = st.selectbox(
        "🏢 Select Stock",
        options = ["AAPL","MSFT","GOOGL","AMZN",
                   "NVDA","META","TSLA","BRK-B","JPM","V"],
        index   = 0
    )

    st.markdown("**📅 Date Range**")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "From",
            value = pd.to_datetime("2024-01-01")
        )
    with col2:
        end_date = st.date_input(
            "To",
            value = pd.to_datetime("2026-03-18")
        )

    start_date  = str(start_date)
    end_date    = str(end_date)
    start_month = start_date[:7]

    st.divider()

    # Quick date range buttons
    st.markdown("**⚡ Quick Select**")
    c1, c2 = st.columns(2)
    if c1.button("1Y"):
        start_date = "2025-03-18"
    if c2.button("2Y"):
        start_date = "2024-03-18"
    if c1.button("YTD"):
        start_date = "2026-01-01"
    if c2.button("All"):
        start_date = "2023-01-01"

    st.divider()
    st.caption("🔄 Cache refreshes every hour")
    st.caption("⚡ Databricks Free Edition")
    st.caption("🤖 AI powered by Claude")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
tab1, tab2 = st.tabs(["📊 Dashboard", "🤖 Ask AI"])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1 — DASHBOARD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab1:

    # ── Load data ───────────────────────────────
    with st.spinner("Loading data from Delta tables..."):
        df       = load_daily_prices(selected_ticker, start_date, end_date)
        df_all   = load_all_prices(start_date, end_date)
        vol_df   = load_volatility()
        monthly  = load_monthly(start_month)

    if df.empty:
        st.warning(f"⚠️ No data for {selected_ticker} in selected range.")
        st.stop()

    # ── KPI Cards ───────────────────────────────
    latest     = df.iloc[-1]
    prev       = df.iloc[-2] if len(df) > 1 else latest
    price_diff = round(float(latest["close"]) - float(prev["close"]), 2)
    pct_diff   = round(float(latest["daily_return_pct"]), 2)

    ticker_vol = vol_df[vol_df["ticker"] == selected_ticker]
    risk       = ticker_vol["risk_tier"].values[0] \
                 if not ticker_vol.empty else "N/A"
    vol_val    = ticker_vol["volatility_stddev"].values[0] \
                 if not ticker_vol.empty else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric(
        "💰 Current Price",
        f"${float(latest['close']):,.2f}",
        f"${price_diff:+.2f}"
    )
    k2.metric(
        "📈 Day Return",
        f"{pct_diff:+.2f}%",
        f"{pct_diff:+.2f}%"
    )
    k3.metric(
        "📊 7d Moving Avg",
        f"${float(latest['moving_avg_7d']):,.2f}"
    )
    k4.metric(
        "📊 30d Moving Avg",
        f"${float(latest['moving_avg_30d']):,.2f}"
    )
    k5.metric(
        "⚠️ Risk Tier",
        risk,
        f"σ={vol_val:.2f}%"
    )

    st.divider()

    # ── Price Chart + Volume ─────────────────────
    st.subheader(f"📈 {selected_ticker} — Price & Volume History")

    fig = make_subplots(
        rows            = 2,
        cols            = 1,
        shared_xaxes    = True,
        row_heights     = [0.72, 0.28],
        vertical_spacing = 0.05,
        subplot_titles  = (
            f"{selected_ticker} Close Price + Moving Averages",
            "Daily Volume"
        )
    )

    fig.add_trace(go.Scatter(
        x    = df["trade_date"],
        y    = df["close"],
        name = "Close Price",
        line = dict(color="#1f77b4", width=2)
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x    = df["trade_date"],
        y    = df["moving_avg_7d"],
        name = "7d MA",
        line = dict(color="#ff7f0e", width=1.5, dash="dash")
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x    = df["trade_date"],
        y    = df["moving_avg_30d"],
        name = "30d MA",
        line = dict(color="#2ca02c", width=1.5, dash="dot")
    ), row=1, col=1)

    # Colour volume bars green/red based on daily return
    colors = [
        "#2ca02c" if r >= 0 else "#d62728"
        for r in df["daily_return_pct"]
    ]
    fig.add_trace(go.Bar(
        x              = df["trade_date"],
        y              = df["volume"],
        name           = "Volume",
        marker_color   = colors,
        opacity        = 0.7
    ), row=2, col=1)

    fig.update_layout(
        height       = 550,
        showlegend   = True,
        hovermode    = "x unified",
        plot_bgcolor = "#0e1117",
        paper_bgcolor = "#0e1117",
        font         = dict(color="white")
    )
    fig.update_xaxes(gridcolor="#1e2130")
    fig.update_yaxes(gridcolor="#1e2130")

    st.plotly_chart(fig, use_container_width=True)
    st.divider()

    # ── All Stocks + Volatility ──────────────────
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("🔀 All Stocks Comparison")
        fig2 = px.line(
            df_all,
            x       = "trade_date",
            y       = "close",
            color   = "ticker",
            title   = "Close Price — All 10 Stocks"
        )
        fig2.update_layout(
            height        = 400,
            plot_bgcolor  = "#0e1117",
            paper_bgcolor = "#0e1117",
            font          = dict(color="white")
        )
        st.plotly_chart(fig2, use_container_width=True)

    with col_r:
        st.subheader("⚠️ Volatility & Risk")
        fig3 = px.bar(
            vol_df,
            x         = "ticker",
            y         = "volatility_stddev",
            color     = "risk_tier",
            color_discrete_map = {
                "🔴 High"  : "#d62728",
                "🟡 Medium": "#ff7f0e",
                "🟢 Low"   : "#2ca02c"
            },
            title     = "Volatility (Std Dev of Daily Returns %)"
        )
        fig3.update_layout(
            height        = 400,
            plot_bgcolor  = "#0e1117",
            paper_bgcolor = "#0e1117",
            font          = dict(color="white")
        )
        st.plotly_chart(fig3, use_container_width=True)

    st.divider()

    # ── Monthly Performance ──────────────────────
    st.subheader("🏆 Monthly Performance")
    col_a, col_b = st.columns(2)

    with col_a:
        winners = monthly[monthly["rank"] == 1]
        fig4 = px.bar(
            winners,
            x     = "month",
            y     = "monthly_return_pct",
            color = "ticker",
            title = "Best Performing Stock Each Month"
        )
        fig4.update_layout(
            height        = 380,
            plot_bgcolor  = "#0e1117",
            paper_bgcolor = "#0e1117",
            font          = dict(color="white")
        )
        st.plotly_chart(fig4, use_container_width=True)

    with col_b:
        try:
            pivot = monthly.pivot_table(
                index   = "ticker",
                columns = "month",
                values  = "monthly_return_pct",
                aggfunc = "mean"
            ).fillna(0)

            fig5 = px.imshow(
                pivot,
                color_continuous_scale = "RdYlGn",
                title  = "Monthly Returns Heatmap",
                aspect = "auto"
            )
            fig5.update_layout(
                height        = 380,
                plot_bgcolor  = "#0e1117",
                paper_bgcolor = "#0e1117",
                font          = dict(color="white")
            )
            st.plotly_chart(fig5, use_container_width=True)
        except Exception as e:
            st.warning(f"Heatmap unavailable: {str(e)}")

    st.divider()

    # ── Risk Summary Table ───────────────────────
    st.subheader("📋 Risk Summary Table")
    st.dataframe(
        vol_df[[
            "ticker", "risk_tier",
            "volatility_stddev", "avg_daily_return",
            "best_day_return", "worst_day_return",
            "total_trading_days", "data_from", "data_to"
        ]].rename(columns={
            "ticker"            : "Stock",
            "risk_tier"         : "Risk Tier",
            "volatility_stddev" : "Volatility σ",
            "avg_daily_return"  : "Avg Daily Return %",
            "best_day_return"   : "Best Day %",
            "worst_day_return"  : "Worst Day %",
            "total_trading_days": "Trading Days",
            "data_from"         : "From",
            "data_to"           : "To"
        }),
        use_container_width = True,
        hide_index          = True
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2 — ASK AI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab2:
    st.subheader("🤖 Ask AI About Your Stock Data")
    st.caption("Claude has access to all Gold table data — ask anything!")

    # ── Build context ────────────────────────────
    with st.spinner("Loading data context for AI..."):
        context = build_ai_context()

    # ── Suggested questions ──────────────────────
    if "messages" not in st.session_state:
        st.session_state.messages = []

    if len(st.session_state.messages) == 0:
        st.markdown("**💡 Try asking:**")
        suggestions = [
            "Which stock had best return in 2025?",
            "Which stock is most risky?",
            "Compare AAPL vs NVDA",
            "Best single day return ever?",
            "Which stocks are safest?"
        ]
        cols = st.columns(5)
        for i, s in enumerate(suggestions):
            if cols[i].button(s, key=f"s{i}"):
                st.session_state.messages.append({
                    "role": "user", "content": s
                })
                st.rerun()

    st.divider()

    # ── Chat history ─────────────────────────────
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # ── Chat input ───────────────────────────────
    if prompt := st.chat_input("Ask anything about the stocks..."):
        st.session_state.messages.append({
            "role": "user", "content": prompt
        })
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Claude is analysing the data..."):
                try:
                    response = client.messages.create(
                        model      = "claude-sonnet-4-20250514",
                        max_tokens = 1024,
                        system     = context,
                        messages   = [
                            {
                                "role"   : m["role"],
                                "content": m["content"]
                            }
                            for m in st.session_state.messages
                        ]
                    )
                    answer = response.content[0].text
                    st.markdown(answer)
                    st.session_state.messages.append({
                        "role": "assistant", "content": answer
                    })

                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")

    # ── Clear chat ───────────────────────────────
    if len(st.session_state.messages) > 0:
        st.divider()
        if st.button("🗑️ Clear conversation"):
            st.session_state.messages = []
            st.rerun()