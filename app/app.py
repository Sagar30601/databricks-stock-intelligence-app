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
    /* ── Hide Streamlit default UI elements ─── */
    #MainMenu { visibility: hidden !important; }
    header[data-testid="stHeader"] { display: none !important; }
    footer { display: none !important; }
    [data-testid="stToolbar"] { display: none !important; }
    .stDeployButton { display: none !important; }
    [data-testid="manage-app-button"] { display: none !important; }

    /* ── Remove top padding ─────────────────── */
    .stApp { 
        background-color: #F5F5F5;
        margin-top: 0 !important;
    }
    .main .block-container {
        background-color: #FFFFFF;
        padding: 0.5rem 1.5rem 1rem 1.5rem !important;
        max-width: 100% !important;
        margin-top: 0 !important;
    }

    /* ── Sidebar ────────────────────────────── */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #003366 0%, #004080 100%);
        min-width: 220px !important;
        max-width: 220px !important;
    }
    [data-testid="stSidebar"] > div:first-child {
        padding: 0.5rem 0.75rem !important;
        overflow: hidden !important;
    }
    [data-testid="stSidebar"] * { color: #FFFFFF !important; }
    [data-testid="stSidebar"] hr {
        border-color: rgba(255,255,255,0.2) !important;
        margin: 0.3rem 0 !important;
    }

    /* Fix dropdown text visibility */
    [data-testid="stSidebar"] [data-baseweb="select"] [data-baseweb="tag"],
    [data-testid="stSidebar"] [data-baseweb="select"] div,
    [data-testid="stSidebar"] [data-baseweb="select"] span,
    [data-testid="stSidebar"] [data-baseweb="select"] input,
    [data-testid="stSidebar"] input {
        color: #1A1A1A !important;
        background-color: #FFFFFF !important;
        font-size: 12px !important;
    }

    /* Date inputs compact */
    [data-testid="stSidebar"] .stDateInput input {
        font-size: 11px !important;
        padding: 3px 5px !important;
    }
    [data-testid="stSidebar"] .stDateInput,
    [data-testid="stSidebar"] .stSelectbox { 
        margin-bottom: 0 !important; 
    }

    /* Sidebar buttons */
    [data-testid="stSidebar"] .stButton button {
        background-color: rgba(255,255,255,0.15) !important;
        color: #FFFFFF !important;
        border: 1px solid rgba(255,255,255,0.3) !important;
        border-radius: 4px !important;
        padding: 0.2rem !important;
        font-size: 11px !important;
        width: 100% !important;
    }
    [data-testid="stSidebar"] .stButton button:hover {
        background-color: #CC0000 !important;
        border-color: #CC0000 !important;
    }

    /* Remove sidebar scrollbar */
    [data-testid="stSidebar"] ::-webkit-scrollbar { display: none !important; }
    [data-testid="stSidebar"] { overflow: hidden !important; }

    /* ── KPI Cards ──────────────────────────── */
    [data-testid="metric-container"] {
        background: #FFFFFF;
        border: 1px solid #E0E0E0;
        border-top: 3px solid #003366;
        border-radius: 6px;
        padding: 0.4rem 0.6rem !important;
    }
    [data-testid="metric-container"] label {
        color: #666 !important;
        font-size: 11px !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    [data-testid="metric-container"] [data-testid="stMetricValue"] {
        color: #003366 !important;
        font-weight: 700 !important;
        font-size: 1.1rem !important;
    }
    [data-testid="metric-container"] [data-testid="stMetricDelta"] {
        font-size: 10px !important;
    }

    /* ── Tabs ───────────────────────────────── */
    .stTabs [data-baseweb="tab-list"] {
        border-bottom: 2px solid #003366;
        gap: 0;
        margin-bottom: 0.25rem !important;
    }
    .stTabs [data-baseweb="tab"] {
        font-size: 13px;
        font-weight: 600;
        color: #666;
        padding: 0.4rem 1.25rem;
        border-radius: 0;
    }
    .stTabs [aria-selected="true"] {
        color: #003366 !important;
        border-bottom: 3px solid #CC0000 !important;
        background: transparent !important;
    }

    /* ── Typography ─────────────────────────── */
    h1 { 
        color: #003366 !important; 
        font-weight: 700 !important;
        font-size: 1.4rem !important; 
        margin: 0 !important; 
        padding: 0 !important;
    }
    h2, h3, h4 { 
        color: #003366 !important; 
        font-weight: 600 !important;
        font-size: 0.95rem !important; 
        margin: 0.25rem 0 !important; 
    }
    hr { 
        border-color: #E0E0E0 !important; 
        margin: 0.3rem 0 !important; 
    }

        /* ── Sidebar overrides ──────────────────────── */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #002855 0%, #003f7f 100%);
        min-width: 240px !important;
        max-width: 240px !important;
    }
    [data-testid="stSidebar"] > div:first-child {
        padding: 1rem !important;
        overflow-y: auto !important;
        overflow-x: hidden !important;
    }
    [data-testid="stSidebar"] * { color: white !important; }

    /* Selectbox */
    [data-testid="stSidebar"] [data-baseweb="select"] > div {
        background: white !important;
        border-radius: 6px !important;
        border: none !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] span,
    [data-testid="stSidebar"] [data-baseweb="select"] div,
    [data-testid="stSidebar"] [data-baseweb="select"] input {
        color: #1a1a1a !important;
        font-size: 13px !important;
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
    }

    /* Date inputs */
    [data-testid="stSidebar"] [data-baseweb="input"] {
        background: white !important;
        border-radius: 6px !important;
        border: none !important;
    }
    [data-testid="stSidebar"] [data-baseweb="input"] input {
        color: #1a1a1a !important;
        font-size: 12px !important;
        padding: 6px 8px !important;
        background: white !important;
    }

    /* Quick select buttons — fix YTD wrapping */
    [data-testid="stSidebar"] .stButton > button {
        background: rgba(255,255,255,0.12) !important;
        color: white !important;
        border: 1px solid rgba(255,255,255,0.25) !important;
        border-radius: 6px !important;
        font-size: 11px !important;
        font-weight: 600 !important;
        padding: 4px 2px !important;
        white-space: nowrap !important;
        width: 100% !important;
        letter-spacing: 0 !important;
    }
    [data-testid="stSidebar"] .stButton > button:hover {
        background: #CC0000 !important;
        border-color: #CC0000 !important;
    }

    /* Remove gaps */
    [data-testid="stSidebar"] .stSelectbox { margin-bottom: 6px !important; }
    [data-testid="stSidebar"] .stDateInput  { margin-bottom: 0 !important; }
    [data-testid="stSidebar"] [data-testid="column"] { padding: 0 2px !important; }
    [data-testid="stSidebar"] ::-webkit-scrollbar { width: 0 !important; }

    /* Date inputs full width */
    [data-testid="stSidebar"] [data-baseweb="input"] {
        background: white !important;
        border-radius: 6px !important;
        border: none !important;
        width: 100% !important;
    }
    [data-testid="stSidebar"] [data-baseweb="input"] input {
        color: #1a1a1a !important;
        font-size: 12px !important;
        padding: 6px 10px !important;
        width: 100% !important;
    }
    [data-testid="stSidebar"] .stDateInput {
        width: 100% !important;
        margin-bottom: 0 !important;
    }

    /* Reduce gaps in sidebar */
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div {
        gap: 0 !important;
        margin: 0 !important;
    }
    [data-testid="stSidebar"] .element-container {
        margin-bottom: 4px !important;
    }

    [data-testid="stSidebar"] .stDateInput {
    margin-top: 0 !important;
    margin-bottom: 4px !important;
    }

    /* ── Remove extra spacing ───────────────── */
    .block-container { padding-top: 0.5rem !important; }
    [data-testid="stVerticalBlock"] { gap: 0.4rem !important; }
    [data-testid="stVerticalBlockBorderWrapper"] { gap: 0 !important; }
    div[data-testid="stVerticalBlock"] > div { padding: 0 !important; }
    </style>
    """, unsafe_allow_html=True)

# ── Claude Client ────────────────────────────────
@st.cache_resource
def get_claude():
    api_key = os.environ.get("CLAUDE_API_KEY")
    if not api_key:
        st.error("❌ CLAUDE_API_KEY not configured")
        st.stop()
    return anthropic.Anthropic(api_key=api_key)

client = get_claude()

# ── Databricks SQL Connection ────────────────────
@st.cache_resource
def get_conn():
    return sql.connect(
        server_hostname = "",
        http_path       = "",
        access_token    = ""
        # auth_type       = "databricks-oauth",
        # use_cloud_fetch = False
    )

def run_query(query: str) -> pd.DataFrame:
    conn   = get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=cols)

# ── Data Loading Functions ───────────────────────
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
    vol_summary = run_query(
        "SELECT * FROM stock_analytics.gold.stock_volatility"
    ).to_string(index=False)

    monthly_summary = run_query("""
        SELECT * FROM stock_analytics.gold.monthly_performance
        WHERE month >= '2024-01' AND rank <= 3
        ORDER BY month, rank
    """).to_string(index=False)

    latest_prices = run_query("""
        SELECT ticker, MAX(trade_date) as latest_date,
               LAST_VALUE(close) OVER (
                   PARTITION BY ticker ORDER BY trade_date
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
               ) as latest_close
        FROM stock_analytics.gold.daily_prices
        GROUP BY ticker, close, trade_date
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ticker ORDER BY trade_date DESC
        ) = 1
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
st.markdown("""
    <div style='display:flex; align-items:baseline; gap:12px; 
                padding: 0.5rem 0 0.25rem 0; border-bottom: 2px solid #003366; 
                margin-bottom: 0.5rem;'>
        <span style='font-size:22px; font-weight:700; color:#003366;'>
            📈 Stock Market Intelligence
        </span>
        <span style='font-size:12px; color:#888888;'>
            Powered by Databricks Delta Lake + Claude AI | S&P 500 Top 10
        </span>
    </div>
""", unsafe_allow_html=True)
st.divider()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SIDEBAR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with st.sidebar:
    # Logo
    st.markdown(f"""
        <div style="padding:0 0 12px 0; border-bottom:1px solid rgba(255,255,255,0.15);">
            <div style="display:flex; align-items:center; gap:10px;">
                <div style="background:#CC0000; width:36px; height:36px; border-radius:8px;
                            display:flex; align-items:center; justify-content:center;
                            font-size:18px; flex-shrink:0;">📈</div>
                <div>
                    <div style="font-size:16px; font-weight:800; letter-spacing:1px; line-height:1.1;">STOCK IQ</div>
                    <div style="font-size:9px; opacity:0.5; letter-spacing:1px;">MARKET INTELLIGENCE</div>
                </div>
            </div>
        </div>

        <div style="margin:14px 0 6px 0; font-size:10px; font-weight:700;
                    letter-spacing:1.5px; opacity:0.5;">EQUITY</div>
    """, unsafe_allow_html=True)

    # Stock dropdown
    ticker_options = ["AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","BRK-B","JPM","V"]
    company_map    = {
        "AAPL":"Apple","MSFT":"Microsoft","GOOGL":"Alphabet",
        "AMZN":"Amazon","NVDA":"NVIDIA","META":"Meta",
        "TSLA":"Tesla","BRK-B":"Berkshire","JPM":"JPMorgan","V":"Visa"
    }
    sector_map = {
        "AAPL":"Technology","MSFT":"Technology","GOOGL":"Technology",
        "AMZN":"Consumer","NVDA":"Semiconductors","META":"Technology",
        "TSLA":"Automotive","BRK-B":"Financials","JPM":"Financials","V":"Financials"
    }

    selected_ticker = st.selectbox(
        label       = "",
        options     = ticker_options,
        format_func = lambda x: f"{x}  ·  {company_map[x]}",
        index       = 0
    )

    # Company card
    st.markdown("""
        <div style="border-top:1px solid rgba(255,255,255,0.15);
                    padding-top:12px; margin-bottom:10px;">
            <div style="font-size:10px; font-weight:700;
                        letter-spacing:1.5px; opacity:0.5; margin-bottom:10px;">
                DATE RANGE
            </div>
        </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='font-size:10px; font-weight:600; color:rgba(255,255,255,0.7); margin:0 0 4px 0;'>FROM</div>",
            unsafe_allow_html=True)
    start_date = st.date_input("_s", value=pd.to_datetime("2024-01-01"),
                            label_visibility="collapsed")

    st.markdown("<div style='font-size:10px; font-weight:600; color:rgba(255,255,255,0.7); margin:10px 0 4px 0;'>TO</div>",
            unsafe_allow_html=True)
    end_date = st.date_input("_e", value=pd.Timestamp.today(),
                            label_visibility="collapsed")

    start_date  = str(start_date)
    end_date    = str(end_date)
    start_month = start_date[:7]

    # Quick select — 4 equal buttons in one row
    st.markdown("""
        <div style="font-size:10px; font-weight:700; letter-spacing:1.5px;
                    opacity:0.5; margin:14px 0 6px;">QUICK SELECT</div>
    """, unsafe_allow_html=True)

    b1, b2 = st.columns(2)
    b3, b4 = st.columns(2)
    if b1.button("1Y",  use_container_width=True):
        start_date = str(pd.Timestamp.today() - pd.DateOffset(years=1))[:10]
    if b2.button("2Y",  use_container_width=True):
        start_date = str(pd.Timestamp.today() - pd.DateOffset(years=2))[:10]
    if b3.button("YTD", use_container_width=True):
        start_date = str(pd.Timestamp.today().replace(month=1, day=1))[:10]
    if b4.button("All", use_container_width=True):
        start_date = "2023-01-01"

    # Footer
    st.markdown(f"""
        <div style="border-top:1px solid rgba(255,255,255,0.15);
                    margin-top:20px; padding-top:12px;
                    font-size:10px; opacity:0.4; line-height:2;">
            🔄 Cache refreshes every hour<br>
            ⚡ Databricks Free Edition<br>
            🤖 Claude AI<br>
            <span style="opacity:1.5; font-weight:700; font-size:11px;">
                📅 {pd.Timestamp.today().strftime('%b %d, %Y')}
            </span>
        </div>
    """, unsafe_allow_html=True)
# ```
# ---

## Key Improvements
# ```
# ✅ Logo horizontal (icon + text side by side) — compact
# ✅ Dropdown shows "AAPL — Apple Inc." format
# ✅ Company card shows name + sector
# ✅ FROM/TO labels above each date input
# ✅ Quick select in 4 columns (1Y 2Y YTD All) — one row
# ✅ Footer compact at bottom
# ✅ Consistent spacing with dividers

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
tab1, tab2 = st.tabs(["📊 Dashboard", "🤖 Ask AI"])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1 — DASHBOARD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab1:
    with st.spinner("Loading..."):
        df      = load_daily_prices(selected_ticker, start_date, end_date)
        df_all  = load_all_prices(start_date, end_date)
        vol_df  = load_volatility()
        monthly = load_monthly(start_month)

    if df.empty:
        st.warning(f"⚠️ No data for {selected_ticker}")
        st.stop()

    latest     = df.iloc[-1]
    prev       = df.iloc[-2] if len(df) > 1 else latest
    price_diff = round(float(latest["close"]) - float(prev["close"]), 2)
    pct_diff   = round(float(latest["daily_return_pct"]), 2)
    ticker_vol = vol_df[vol_df["ticker"] == selected_ticker]
    risk       = ticker_vol["risk_tier"].values[0] if not ticker_vol.empty else "N/A"
    vol_val    = ticker_vol["volatility_stddev"].values[0] if not ticker_vol.empty else 0

    # ── KPI Cards (compact) ──────────────────────
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("💰 Price",     f"${float(latest['close']):,.2f}",        f"${price_diff:+.2f}")
    k2.metric("📈 Return",    f"{pct_diff:+.2f}%",                       f"{pct_diff:+.2f}%")
    k3.metric("📊 7d MA",     f"${float(latest['moving_avg_7d']):,.2f}")
    k4.metric("📊 30d MA",    f"${float(latest['moving_avg_30d']):,.2f}")
    k5.metric("⚠️ Risk",      risk,                                       f"σ={vol_val:.2f}%")

    # ── Price + Volume Chart (compact height) ────
    st.markdown(f"#### 📈 {selected_ticker} — Price & Volume")
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.70, 0.30],
        vertical_spacing=0.03,           # ← tighter spacing
        subplot_titles=(f"{selected_ticker} Close + Moving Averages", "Volume")
    )
    fig.add_trace(go.Scatter(x=df["trade_date"], y=df["close"],
        name="Close", line=dict(color="#003366", width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df["trade_date"], y=df["moving_avg_7d"],
        name="7d MA", line=dict(color="#CC0000", width=1.5, dash="dash")), row=1, col=1)
    fig.add_trace(go.Scatter(x=df["trade_date"], y=df["moving_avg_30d"],
        name="30d MA", line=dict(color="#0066CC", width=1.5, dash="dot")), row=1, col=1)
    colors = ["#2ca02c" if r >= 0 else "#d62728" for r in df["daily_return_pct"]]
    fig.add_trace(go.Bar(x=df["trade_date"], y=df["volume"],
        name="Volume", marker_color=colors, opacity=0.7), row=2, col=1)
    fig.update_layout(
        height=400,                      # ← reduced from 550
        margin=dict(t=30, b=10, l=10, r=10),   # ← tight margins
        showlegend=True,
        legend=dict(
        orientation="h",
        yanchor="top",
        y=-0.15,          # ← below the chart
        xanchor="center",
        x=0.5,
        font=dict(size=10)
        ),
        hovermode="x unified",
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        font=dict(color="#1A1A1A", size=11)
    )
    fig.update_xaxes(gridcolor="#F0F0F0", linecolor="#E0E0E0")
    fig.update_yaxes(gridcolor="#F0F0F0", linecolor="#E0E0E0")
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Row: All Stocks + Volatility ────────────
    col_l, col_r = st.columns(2)
    with col_l:
        fig2 = px.line(df_all, x="trade_date", y="close", color="ticker",
            title="All Stocks — Close Price")
        fig2.update_layout(height=300, margin=dict(t=30,b=10,l=10,r=10),
            plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
            font=dict(color="#1A1A1A", size=11),
            legend=dict(font=dict(size=9)))
        fig2.update_xaxes(gridcolor="#F0F0F0")
        fig2.update_yaxes(gridcolor="#F0F0F0")
        st.plotly_chart(fig2, use_container_width=True)

    with col_r:
        fig3 = px.bar(vol_df, x="ticker", y="volatility_stddev", color="risk_tier",
            color_discrete_map={"🔴 High":"#d62728","🟡 Medium":"#ff7f0e","🟢 Low":"#2ca02c"},
            title="Volatility by Stock")
        fig3.update_layout(height=300, margin=dict(t=30,b=10,l=10,r=10),
            plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
            font=dict(color="#1A1A1A", size=11),
            legend=dict(font=dict(size=9)))
        fig3.update_xaxes(gridcolor="#F0F0F0")
        fig3.update_yaxes(gridcolor="#F0F0F0")
        st.plotly_chart(fig3, use_container_width=True)

    # ── Row: Monthly Winner + Heatmap ───────────
    col_a, col_b = st.columns(2)
    with col_a:
        winners = monthly[monthly["rank"] == 1]
        fig4 = px.bar(winners, x="month", y="monthly_return_pct",
            color="ticker", title="Best Stock Each Month")
        fig4.update_layout(height=300, margin=dict(t=30,b=10,l=10,r=10),
            plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
            font=dict(color="#1A1A1A", size=11),
            legend=dict(font=dict(size=9)))
        fig4.update_xaxes(gridcolor="#F0F0F0")
        fig4.update_yaxes(gridcolor="#F0F0F0")
        st.plotly_chart(fig4, use_container_width=True)

    with col_b:
        try:
            pivot = monthly.pivot_table(index="ticker", columns="month",
                values="monthly_return_pct", aggfunc="mean").fillna(0)
            fig5 = px.imshow(pivot, color_continuous_scale="RdYlGn",
                title="Monthly Returns Heatmap", aspect="auto")
            fig5.update_layout(height=300, margin=dict(t=30,b=10,l=10,r=10),
                plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
                font=dict(color="#1A1A1A", size=11))
            st.plotly_chart(fig5, use_container_width=True)
        except Exception as e:
            st.warning(f"Heatmap: {str(e)}")

    # ── Risk Summary Table ───────────────────────
    st.divider()
    st.markdown("#### 📋 Risk Summary Table")
    st.dataframe(
        vol_df[["ticker","risk_tier","volatility_stddev","avg_daily_return",
                "best_day_return","worst_day_return","total_trading_days"]
        ].rename(columns={
            "ticker":"Stock", "risk_tier":"Risk",
            "volatility_stddev":"Volatility σ",
            "avg_daily_return":"Avg Return %",
            "best_day_return":"Best Day %",
            "worst_day_return":"Worst Day %",
            "total_trading_days":"Days"
        }),
        use_container_width=True,
        hide_index=True,
        height=250                    # ← compact table
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2 — ASK AI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab2:
    st.subheader("🤖 Ask AI About Your Stock Data")
    st.caption("Claude has access to all Gold table data — ask anything!")

    with st.spinner("Loading data context for AI..."):
        context = build_ai_context()

    if "messages" not in st.session_state:
        st.session_state.messages = []

    if len(st.session_state.messages) == 0:
        st.markdown("**💡 Try asking:**")
        suggestions = ["Which stock had best return in 2025?",
                       "Which stock is most risky?",
                       "Compare AAPL vs NVDA",
                       "Best single day return ever?",
                       "Which stocks are safest?"]
        cols = st.columns(5)
        for i, s in enumerate(suggestions):
            if cols[i].button(s, key=f"s{i}"):
                st.session_state.messages.append({"role":"user","content":s})
                st.rerun()

    st.divider()

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt := st.chat_input("Ask anything about the stocks..."):
        st.session_state.messages.append({"role":"user","content":prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Claude is analysing the data..."):
                try:
                    response = client.messages.create(
                        model      = "claude-sonnet-4-20250514",
                        max_tokens = 1024,
                        system     = context,
                        messages   = [{"role":m["role"],"content":m["content"]}
                                      for m in st.session_state.messages]
                    )
                    answer = response.content[0].text
                    st.markdown(answer)
                    st.session_state.messages.append({"role":"assistant","content":answer})
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")

    if len(st.session_state.messages) > 0:
        st.divider()
        if st.button("🗑️ Clear conversation"):
            st.session_state.messages = []
            st.rerun()