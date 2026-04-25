"""
Fraud Detection Dashboard — Streamlit
Reads from PostgreSQL written by Spark Structured Streaming
"""

import os
import time
from decimal import Decimal

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import psycopg2.extras
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ── Dark theme ────────────────────────────────────────────────────────────────
DARK_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Inter:wght@300;400;500;600;700&display=swap');
:root{--bg:#0d1117;--bg2:#161b22;--border:#21262d;--green:#23c45e;--red:#f85149;--yellow:#e3b341;--blue:#388bfd;--text:#e6edf3;--muted:#8b949e;}
html,body{background-color:var(--bg)!important;color:var(--text)!important;}
.stApp,[data-testid="stAppViewContainer"],[data-testid="stAppViewBlockContainer"],[data-testid="stVerticalBlock"],.main,.main>div{background-color:var(--bg)!important;color:var(--text)!important;}
.main .block-container{background-color:var(--bg)!important;padding:1.5rem 2rem!important;max-width:100%!important;}
.main p,.main span,.main div,.main label{color:var(--text)!important;}
#MainMenu,footer,header,.stDeployButton{visibility:hidden!important;display:none!important;}
section[data-testid="stSidebar"],section[data-testid="stSidebar"]>div{background-color:var(--bg)!important;border-right:1px solid var(--border)!important;}
section[data-testid="stSidebar"] *{color:var(--text)!important;}
h1{font-size:1.9rem!important;font-weight:700!important;color:var(--text)!important;letter-spacing:-.04em!important;}
h2,h3{color:var(--text)!important;font-weight:600!important;}
h2{font-size:1.05rem!important;}
[data-testid="metric-container"]{background:var(--bg2)!important;border:1px solid var(--border)!important;border-radius:8px!important;padding:1rem 1.2rem!important;}
[data-testid="stMetricLabel"]{font-size:.72rem!important;color:var(--muted)!important;text-transform:uppercase;letter-spacing:.06em;}
[data-testid="stMetricValue"]{font-size:1.5rem!important;font-weight:700!important;font-family:'JetBrains Mono',monospace!important;color:var(--text)!important;}
.stTabs [data-baseweb="tab-list"]{background:transparent!important;border-bottom:1px solid var(--border)!important;gap:0!important;}
.stTabs [data-baseweb="tab"]{background:transparent!important;color:var(--muted)!important;border:none!important;border-bottom:2px solid transparent!important;padding:.55rem 1rem!important;font-size:.84rem!important;font-weight:500!important;}
.stTabs [aria-selected="true"]{color:var(--green)!important;border-bottom-color:var(--green)!important;background:transparent!important;}
.stTabs [data-baseweb="tab-panel"]{padding-top:1.2rem!important;}
[data-testid="stDataFrame"]{border:1px solid var(--border)!important;border-radius:8px!important;overflow:hidden;}
.stButton>button{background:transparent!important;border:1px solid var(--border)!important;color:var(--text)!important;border-radius:6px!important;}
.stButton>button:hover{border-color:var(--green)!important;background:rgba(35,196,94,.08)!important;}
hr{border-color:var(--border)!important;margin:1rem 0!important;}
::-webkit-scrollbar{width:5px;height:5px;}
::-webkit-scrollbar-track{background:var(--bg);}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px;}
</style>
"""

PLOTLY = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#161b22",
    font_color="#e6edf3",
    font_family="Inter",
    margin=dict(l=0, r=0, t=36, b=0),
    title_font_size=13,
)

GREEN  = "#23c45e"
RED    = "#f85149"
YELLOW = "#e3b341"
BLUE   = "#388bfd"

st.set_page_config(page_title="Fraud Detection", page_icon="🛡️",
                   layout="wide", initial_sidebar_state="expanded")
st.markdown(DARK_CSS, unsafe_allow_html=True)

# ── DB ────────────────────────────────────────────────────────────────────────
DB_DSN = os.getenv("DB_DSN", "host=postgres port=5432 dbname=fraud user=fraud password=fraud")


def get_conn():
    return psycopg2.connect(DB_DSN)


def _cast_floats(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        sample = df[col].dropna()
        if not sample.empty and isinstance(sample.iloc[0], Decimal):
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(ttl=10, show_spinner=False)
def run_query(sql: str, params=None) -> pd.DataFrame:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                df = pd.DataFrame(cur.fetchall())
        return _cast_floats(df) if not df.empty else df
    except Exception as e:
        st.error(f"DB error: {e}")
        return pd.DataFrame()


def test_db() -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True
    except Exception:
        return False


# ── SQL ───────────────────────────────────────────────────────────────────────
SQL_OVERVIEW = """
SELECT
    (SELECT COUNT(*) FROM transactions
     WHERE ingested_at >= now() - interval '1 hour')        AS tx_last_hour,
    (SELECT COUNT(*) FROM fraud_alerts
     WHERE ingested_at >= now() - interval '1 hour')        AS fraud_last_hour,
    (SELECT COUNT(*) FROM fraud_alerts WHERE acknowledged=false) AS unacked,
    (SELECT COUNT(*) FROM dlq_messages
     WHERE ingested_at >= now() - interval '1 hour')        AS dlq_last_hour,
    (SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE fa.id IS NOT NULL)
            / NULLIF(COUNT(*),0), 2)
     FROM transactions t
     LEFT JOIN fraud_alerts fa USING (transaction_id)
     WHERE t.ingested_at >= now() - interval '1 hour')      AS fraud_rate_pct
"""

SQL_FRAUD_BY_REASON = """
    SELECT fraud_reason, COUNT(*) as n
    FROM fraud_alerts
    WHERE ingested_at >= now() - interval '24 hours'
    GROUP BY fraud_reason ORDER BY n DESC
"""

SQL_FRAUD_BY_COUNTRY = """
    SELECT country, COUNT(*) as n
    FROM fraud_alerts
    WHERE ingested_at >= now() - interval '24 hours'
    GROUP BY country ORDER BY n DESC LIMIT 15
"""

SQL_TX_RATE = """
    SELECT date_trunc('minute', ingested_at) as minute,
           COUNT(*) as total,
           COUNT(*) FILTER (WHERE transaction_id IN (
               SELECT transaction_id FROM fraud_alerts)) as fraud
    FROM transactions
    WHERE ingested_at >= now() - interval '30 minutes'
    GROUP BY 1 ORDER BY 1
"""

SQL_FRAUD_ALERTS = """
    SELECT id, transaction_id, user_id, amount, currency, merchant,
           country, card_type, fraud_reason, ingested_at, acknowledged
    FROM fraud_alerts
    ORDER BY ingested_at DESC LIMIT 200
"""

SQL_RECENT_TX = """
    SELECT transaction_id, user_id, amount, currency,
           merchant, country, card_type, timestamp, ingested_at
    FROM transactions
    ORDER BY ingested_at DESC LIMIT 100
"""

SQL_TOP_USERS = """
    SELECT user_id, COUNT(*) as fraud_count,
           SUM(amount) as total_amount
    FROM fraud_alerts
    WHERE ingested_at >= now() - interval '24 hours'
    GROUP BY user_id ORDER BY fraud_count DESC LIMIT 10
"""

# ── Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar():
    with st.sidebar:
        st.markdown('<div style="font-family:JetBrains Mono,monospace;font-size:1rem;font-weight:600;padding:0 0 .8rem">🛡️ Fraud <span style="color:#23c45e">Detection</span></div>',
                    unsafe_allow_html=True)
        connected = test_db()
        color = "#23c45e" if connected else "#f85149"
        bg    = "#0F6E56" if connected else "#791F1F"
        label = "● Connected" if connected else "✕ DB unreachable"
        st.markdown(f'<div style="background:{bg};color:{color};font-weight:600;font-size:.8rem;border-radius:6px;padding:.4rem 1rem;margin:.5rem 0;text-align:center">{label}</div>',
                    unsafe_allow_html=True)
        st.markdown('<div style="font-family:JetBrains Mono,monospace;font-size:.7rem;color:#23c45e;background:rgba(35,196,94,.08);border:1px solid rgba(35,196,94,.2);border-radius:4px;padding:.25rem .5rem;margin-top:.25rem">postgres:5432/fraud</div>',
                    unsafe_allow_html=True)
        st.divider()
        if st.button("↺  Refresh now", use_container_width=True):
            st.cache_data.clear(); st.rerun()
        auto = st.checkbox("Auto-refresh every 15s")
        if auto:
            time.sleep(15)
            st.cache_data.clear()
            st.rerun()
        st.divider()
        st.markdown('<p style="font-size:.72rem;color:#6e7681">Pipeline</p>', unsafe_allow_html=True)
        st.markdown("""
        <div style="font-size:.7rem;color:#8b949e;line-height:2">
        Transaction Generator<br>
        ↓ <span style="color:#23c45e">Kafka</span> transactions<br>
        ↓ <span style="color:#388bfd">Spark</span> Structured Streaming<br>
        ↓ <span style="color:#f85149">Fraud Detection</span><br>
        ↓ <span style="color:#e3b341">PostgreSQL</span><br>
        ↓ <span style="color:#23c45e">Streamlit</span> dashboard
        </div>""", unsafe_allow_html=True)
        st.divider()
        st.markdown('<p style="font-size:.72rem;color:#6e7681">Fraud Rules</p>', unsafe_allow_html=True)
        st.markdown("""
        <div style="font-size:.69rem;color:#8b949e;line-height:1.8">
        🔴 amount &gt; 9,000<br>
        🟡 high-risk country + amount &gt; 500<br>
        🔵 IP starts with 185.*<br>
        🟠 Amex + amount &gt; 5,000<br>
        🟣 velocity flag
        </div>""", unsafe_allow_html=True)
    return connected


# ── Overview metrics ──────────────────────────────────────────────────────────
def render_overview():
    df = run_query(SQL_OVERVIEW)
    if df.empty:
        st.info("Waiting for data… make sure all services are running.")
        return

    r = df.iloc[0]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("📊 Transactions (1h)", int(r["tx_last_hour"] or 0))
    c2.metric("🚨 Fraud Alerts (1h)", int(r["fraud_last_hour"] or 0))
    c3.metric("⚠️ Unacknowledged",    int(r["unacked"] or 0))
    c4.metric("💀 DLQ (1h)",          int(r["dlq_last_hour"] or 0))
    c5.metric("📈 Fraud Rate",         f"{float(r['fraud_rate_pct'] or 0):.2f}%")

    unacked = int(r["unacked"] or 0)
    if unacked > 0:
        st.warning(f"⚠️ **{unacked}** unacknowledged fraud alert(s) require review.")


# ── Tab: Activity ─────────────────────────────────────────────────────────────
def tab_activity():
    col_l, col_r = st.columns([1.5, 1])

    with col_l:
        st.markdown("#### Transaction vs Fraud rate (last 30 min)")
        df = run_query(SQL_TX_RATE)
        if not df.empty:
            df["total"] = pd.to_numeric(df["total"], errors="coerce")
            df["fraud"] = pd.to_numeric(df["fraud"], errors="coerce")
            fig = go.Figure()
            fig.add_trace(go.Bar(name="Total", x=df["minute"], y=df["total"],
                                 marker_color=BLUE, marker_line_width=0))
            fig.add_trace(go.Bar(name="Fraud", x=df["minute"], y=df["fraud"],
                                 marker_color=RED, marker_line_width=0))
            fig.update_layout(**PLOTLY, barmode="overlay", height=280,
                              legend=dict(orientation="h", y=1.1))
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown("#### Fraud by rule")
        df = run_query(SQL_FRAUD_BY_REASON)
        if not df.empty:
            color_map = {
                "HIGH_AMOUNT": RED, "HIGH_RISK_COUNTRY": YELLOW,
                "SUSPICIOUS_IP": BLUE, "AMEX_LARGE": "#fb923c",
                "VELOCITY_FRAUD": "#a78bfa",
            }
            colors = [color_map.get(r, "#8b949e") for r in df["fraud_reason"]]
            fig = go.Figure(go.Pie(
                labels=df["fraud_reason"], values=df["n"],
                marker_colors=colors, hole=0.45, textinfo="label+percent",
            ))
            fig.update_layout(**{**PLOTLY, "margin": dict(l=0, r=0, t=10, b=0)}, height=280)
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.markdown("#### Fraud by country (last 24h)")
    df = run_query(SQL_FRAUD_BY_COUNTRY)
    if not df.empty:
        fig = px.bar(df, x="n", y="country", orientation="h",
                     color="n", color_continuous_scale="Reds",
                     title="Fraud alerts by country")
        fig.update_layout(**PLOTLY, coloraxis_showscale=False,
                          yaxis={"categoryorder": "total ascending"}, height=320)
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(fig, use_container_width=True)


# ── Tab: Fraud Alerts ─────────────────────────────────────────────────────────
def tab_fraud_alerts():
    st.markdown("#### 🚨 Fraud Alert Log")

    status = st.radio("Filter", ["All", "Unacknowledged", "Acknowledged"],
                      horizontal=True, label_visibility="collapsed")

    df = run_query(SQL_FRAUD_ALERTS)
    if df.empty:
        st.success("✅ No fraud alerts yet."); return

    if status == "Unacknowledged":
        df = df[df["acknowledged"] == False]  # noqa
    elif status == "Acknowledged":
        df = df[df["acknowledged"] == True]   # noqa

    for col in ["amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    reason_colors = {
        "HIGH_AMOUNT": "🔴", "HIGH_RISK_COUNTRY": "🟡",
        "SUSPICIOUS_IP": "🔵", "AMEX_LARGE": "🟠", "VELOCITY_FRAUD": "🟣",
    }
    df["rule"] = df["fraud_reason"].map(lambda x: f"{reason_colors.get(x,'⚪')} {x}")

    st.dataframe(
        df[["id","transaction_id","user_id","amount","currency",
            "merchant","country","card_type","rule","ingested_at","acknowledged"]],
        use_container_width=True, hide_index=True,
        column_config={
            "id":             "ID",
            "transaction_id": st.column_config.TextColumn("TX ID", width="medium"),
            "amount":         st.column_config.NumberColumn("Amount", format="$%.2f"),
            "rule":           "Fraud Rule",
            "acknowledged":   st.column_config.CheckboxColumn("Acked"),
        }
    )

    st.divider()
    st.markdown("#### Top suspicious users (last 24h)")
    top = run_query(SQL_TOP_USERS)
    if not top.empty:
        top["total_amount"] = pd.to_numeric(top["total_amount"], errors="coerce")
        fig = px.bar(top, x="user_id", y="fraud_count",
                     color="fraud_count", color_continuous_scale="Reds",
                     title="Fraud count per user")
        fig.update_layout(**PLOTLY, coloraxis_showscale=False, height=260)
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(fig, use_container_width=True)


# ── Tab: Transactions ─────────────────────────────────────────────────────────
def tab_transactions():
    st.markdown("#### Recent Transactions (last 100)")
    df = run_query(SQL_RECENT_TX)
    if df.empty:
        st.info("No transactions yet."); return

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    col_l, col_r = st.columns(2)
    with col_l:
        by_country = df.groupby("country")["amount"].sum().reset_index().sort_values("amount", ascending=False)
        fig = px.bar(by_country.head(10), x="country", y="amount",
                     color="amount", color_continuous_scale="Blues",
                     title="Volume by country (last 100 tx)")
        fig.update_layout(**PLOTLY, coloraxis_showscale=False, height=260)
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        by_merchant = df.groupby("merchant")["amount"].sum().reset_index().sort_values("amount", ascending=False)
        fig = px.pie(by_merchant, names="merchant", values="amount",
                     title="Volume by merchant", hole=0.4,
                     color_discrete_sequence=[GREEN,BLUE,YELLOW,RED,"#a78bfa","#fb923c"])
        fig.update_layout(**{**PLOTLY, "margin": dict(l=0, r=0, t=30, b=0)}, height=260)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.dataframe(df, use_container_width=True, hide_index=True,
                 column_config={
                     "amount": st.column_config.NumberColumn("Amount", format="$%.2f"),
                 })


# ── Tab: Spark Pipeline ───────────────────────────────────────────────────────
def tab_spark():
    st.markdown("#### Pipeline status")
    col1, col2, col3, col4 = st.columns(4)

    total_tx    = run_query("SELECT COUNT(*) as n FROM transactions")
    total_fraud = run_query("SELECT COUNT(*) as n FROM fraud_alerts")
    total_dlq   = run_query("SELECT COUNT(*) as n FROM dlq_messages")
    last        = run_query("SELECT MAX(ingested_at) as last FROM transactions")

    col1.metric("Total transactions", int(total_tx.iloc[0]["n"])    if not total_tx.empty    else 0)
    col2.metric("Total fraud alerts", int(total_fraud.iloc[0]["n"]) if not total_fraud.empty else 0)
    col3.metric("Total DLQ",          int(total_dlq.iloc[0]["n"])   if not total_dlq.empty   else 0)
    col4.metric("Last ingestion",     str(last.iloc[0]["last"])[:19] if not last.empty and last.iloc[0]["last"] else "—")

    st.divider()
    st.markdown("#### Ingestion rate (tx/minute)")
    rate = run_query("""
        SELECT date_trunc('minute', ingested_at) as minute, COUNT(*) as ticks
        FROM transactions
        WHERE ingested_at >= now() - interval '30 minutes'
        GROUP BY 1 ORDER BY 1
    """)
    if not rate.empty:
        fig = px.area(rate, x="minute", y="ticks",
                      color_discrete_sequence=[GREEN])
        fig.update_layout(**PLOTLY, height=260,
                          title="Transactions ingested per minute")
        fig.update_traces(line_color=GREEN, fillcolor="rgba(35,196,94,0.1)")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.markdown("#### Last 20 raw transactions")
    raw = run_query("SELECT * FROM transactions ORDER BY ingested_at DESC LIMIT 20")
    st.dataframe(raw, use_container_width=True, hide_index=True)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    connected = render_sidebar()
    st.title("🛡️ Fraud Detection Dashboard")
    st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

    if not connected:
        st.error("Cannot reach PostgreSQL. Make sure all services are running.")
        st.stop()

    render_overview()
    st.markdown("<div style='height:.8rem'></div>", unsafe_allow_html=True)

    t1, t2, t3, t4 = st.tabs([
        "📡 Activity", "🚨 Fraud Alerts", "💳 Transactions", "⚡ Spark Pipeline"
    ])

    with t1: tab_activity()
    with t2: tab_fraud_alerts()
    with t3: tab_transactions()
    with t4: tab_spark()


if __name__ == "__main__":
    main()
