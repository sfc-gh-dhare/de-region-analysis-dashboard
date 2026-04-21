import re
import streamlit as st
import pandas as pd
from datetime import date, timedelta

st.set_page_config(
    page_title="DE Territory Deep Dive",
    page_icon="🏥",
    layout="wide",
)

conn = st.connection("snowflake")
SFDC = "https://snowforce.lightning.force.com/lightning/r/Account/{}/view"


def n_months_ago(n: int) -> date:
    t = date.today()
    m, y = t.month - n, t.year
    while m <= 0:
        m += 12
        y -= 1
    return date(y, m, 1)


M1, M2, M3 = n_months_ago(3), n_months_ago(2), n_months_ago(1)
M1_LBL = M1.strftime("%b %Y")
M2_LBL = M2.strftime("%b %Y")
M3_LBL = M3.strftime("%b %Y")
_m3_next = date(M3.year + (1 if M3.month == 12 else 0), M3.month % 12 + 1, 1)
M3_END = _m3_next - timedelta(days=1)

FEATURE_DEFS = {
    "Dynamic Tables":            "f.primary_feature = 'DT refresh'",
    "Snowpark DE":               "f.primary_feature IN ('Snowpark DE','Iceberg Snowpark DE')",
    "dbt Projects in Snowflake": "f.primary_feature = 'dbt projects in Snowflake'",
    "Openflow":                  "f.primary_feature IN ('Openflow','Openflow Connector','Iceberg Openflow')",
    "Snowpipe Streaming":        "f.primary_feature IN ('Snowpipe Streaming','Snowpipe Streaming v2')",
    "Iceberg Storage":           "f.primary_feature ILIKE 'Iceberg%'",
    "Lakehouse Analytics":       "f.product_category='Analytics' AND f.primary_feature='Lakehouse Analytics'",
}
ALL_FEATURES = list(FEATURE_DEFS.keys())
ALL_REGIONS = "__ALL__"


# ─── Sidebar ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=86400, show_spinner=False)
def _territory_options():
    return conn.query(
        "SELECT DISTINCT GEO, SALES_AREA "
        "FROM sales.raven.d_salesforce_account_customers "
        "WHERE IS_CAPACITY_CUSTOMER=TRUE AND IS_REVENUE_ACCOUNT=TRUE "
        "ORDER BY 1, 2",
        ttl=0,
    )


with st.sidebar:
    st.markdown("## DE Territory Deep Dive")
    st.caption(f"{M1_LBL} – {M3_LBL}  ·  ~3-day lag")
    st.divider()

    terr_opts = _territory_options()
    theaters = sorted(terr_opts["GEO"].dropna().unique().tolist())
    theater = st.selectbox(
        "Theater",
        theaters,
        index=theaters.index("USMajors") if "USMajors" in theaters else 0,
    )
    regions = sorted(
        terr_opts[terr_opts["GEO"] == theater]["SALES_AREA"].dropna().unique().tolist()
    )
    region_options = ["All Regions (Theater-Wide)"] + regions
    region_selection = st.selectbox(
        "Region",
        region_options,
        index=region_options.index("HCLS") if "HCLS" in region_options else 0,
    )
    region = ALL_REGIONS if region_selection == "All Regions (Theater-Wide)" else region_selection
    region_sql = "" if region == ALL_REGIONS else f"AND SALES_AREA='{region}'"

    st.divider()
    selected_features = st.multiselect(
        "Prioritized Features",
        options=ALL_FEATURES,
        default=ALL_FEATURES,
        help="Filter consumption totals (Sections 1–4) to selected features only.",
    )

    if not selected_features or set(selected_features) == set(ALL_FEATURES):
        feature_filter = "1=1"
    else:
        conds = [FEATURE_DEFS[f] for f in selected_features]
        feature_filter = " OR ".join(f"({c})" for c in conds)

    st.divider()
    st.caption("**Use Case Date Window** (for Recent Wins & Losses)")
    uc_date_from = st.date_input("From", value=M1, key="uc_from")
    uc_date_to = st.date_input("To", value=M3_END, key="uc_to")

    st.divider()
    st.caption(
        "**Scope:** DE + Lakehouse Analytics  \n"
        "**Accounts:** Capacity customers only  \n"
        "**Feature threshold:** > 0 credits, 3-mo window  \n"
        "**Adoption target:** < 500 credits last month"
    )
    st.divider()
    st.caption("Questions/Contact: David Hare")


# ─── Data Loaders ───────────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def q_meta(theater, region_sql):
    try:
        return conn.query("""
            SELECT COUNT(*) AS ACCOUNT_COUNT, MAX(RVP) AS RVP, MAX(SE_DIRECTOR_NAME) AS SE_RVP
            FROM sales.raven.d_salesforce_account_customers
            WHERE GEO='{theater}' {region_sql}
              AND IS_CAPACITY_CUSTOMER=TRUE AND IS_REVENUE_ACCOUNT=TRUE
        """.format(theater=theater, region_sql=region_sql), ttl=0)
    except Exception:
        try:
            return conn.query("""
                SELECT COUNT(*) AS ACCOUNT_COUNT, NULL AS RVP, NULL AS SE_RVP
                FROM sales.raven.d_salesforce_account_customers
                WHERE GEO='{theater}' {region_sql}
                  AND IS_CAPACITY_CUSTOMER=TRUE AND IS_REVENUE_ACCOUNT=TRUE
            """.format(theater=theater, region_sql=region_sql), ttl=0)
        except Exception:
            return None


@st.cache_data(ttl=3600, show_spinner=False)
def q_summary(theater, region_sql, feature_filter):
    return conn.query("""
        WITH al AS (
            SELECT SALESFORCE_ACCOUNT_ID
            FROM sales.raven.d_salesforce_account_customers
            WHERE GEO='{theater}' {region_sql}
              AND IS_CAPACITY_CUSTOMER=TRUE AND IS_REVENUE_ACCOUNT=TRUE
        ), mf AS (
            SELECT DATE_TRUNC('month', f.general_date)::DATE AS month,
                   f.salesforce_account_id,
                   CASE WHEN f.product_category='Analytics' THEN 'Lakehouse'
                        WHEN f.primary_feature ILIKE 'Iceberg%' THEN 'Interop'
                        WHEN f.primary_feature IN (
                            'DML','DT refresh','Snowpark DE','Stream access','Task',
                            'dbt projects in Snowflake',
                            'Data Engineering tools - Transformation'
                        ) THEN 'Transform'
                        ELSE 'Ingest'
                   END AS cat,
                   f.product_category,
                   SUM(f.total_credits) AS cr
            FROM sales.raven.A360_PRODUCT_CATEGORY_DAILY_VIEW f
            JOIN al ON al.SALESFORCE_ACCOUNT_ID = f.salesforce_account_id
            WHERE f.general_date >= DATEADD('month',-3,DATE_TRUNC('month',CURRENT_DATE()))
              AND f.general_date < DATE_TRUNC('month',CURRENT_DATE())
              AND (f.product_category='Data Engineering'
                   OR (f.product_category='Analytics' AND f.primary_feature='Lakehouse Analytics'))
              AND ({feature_filter})
            GROUP BY 1,2,3,4
        )
        SELECT month,
            ROUND(SUM(cr),0) AS TOTAL,
            ROUND(SUM(CASE WHEN product_category='Data Engineering' THEN cr ELSE 0 END),0) AS DE,
            ROUND(SUM(CASE WHEN cat='Ingest' THEN cr ELSE 0 END),0) AS INGESTION,
            ROUND(SUM(CASE WHEN cat='Transform' THEN cr ELSE 0 END),0) AS TRANSFORMATION,
            ROUND(SUM(CASE WHEN cat='Interop' THEN cr ELSE 0 END),0) AS INTEROP_STORAGE,
            ROUND(SUM(CASE WHEN cat='Lakehouse' THEN cr ELSE 0 END),0) AS LAKEHOUSE_ANALYTICS,
            COUNT(DISTINCT salesforce_account_id) AS ACTIVE_ACCOUNTS
        FROM mf GROUP BY 1 ORDER BY 1
    """.format(theater=theater, region_sql=region_sql, feature_filter=feature_filter), ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_pivoted(theater, region_sql, feature_filter):
    return conn.query("""
        WITH al AS (
            SELECT SALESFORCE_ACCOUNT_ID, SALESFORCE_ACCOUNT_NAME,
                   SALESFORCE_OWNER_NAME, LEAD_SALES_ENGINEER_NAME, SALES_AREA
            FROM sales.raven.d_salesforce_account_customers
            WHERE GEO='{theater}' {region_sql}
              AND IS_CAPACITY_CUSTOMER=TRUE AND IS_REVENUE_ACCOUNT=TRUE
        ), m AS (
            SELECT a.SALESFORCE_ACCOUNT_ID, a.SALESFORCE_ACCOUNT_NAME,
                   a.SALESFORCE_OWNER_NAME, a.LEAD_SALES_ENGINEER_NAME, a.SALES_AREA,
                   DATE_TRUNC('month', f.general_date)::DATE AS month,
                   ROUND(SUM(f.total_credits),0) AS cr
            FROM sales.raven.A360_PRODUCT_CATEGORY_DAILY_VIEW f
            JOIN al a ON a.SALESFORCE_ACCOUNT_ID = f.salesforce_account_id
            WHERE f.general_date >= DATEADD('month',-3,DATE_TRUNC('month',CURRENT_DATE()))
              AND f.general_date < DATE_TRUNC('month',CURRENT_DATE())
              AND (f.product_category='Data Engineering'
                   OR (f.product_category='Analytics' AND f.primary_feature='Lakehouse Analytics'))
              AND ({feature_filter})
        GROUP BY 1,2,3,4,5,6
        )
        SELECT SALESFORCE_ACCOUNT_ID,
               MAX(SALESFORCE_ACCOUNT_NAME) AS ACCOUNT,
               MAX(SALESFORCE_OWNER_NAME) AS AE,
               MAX(LEAD_SALES_ENGINEER_NAME) AS SE,
               MAX(SALES_AREA) AS REGION,
               COALESCE(MAX(CASE WHEN month=DATE_TRUNC('month',DATEADD('month',-3,CURRENT_DATE()))::DATE THEN cr END),0) AS M1,
               COALESCE(MAX(CASE WHEN month=DATE_TRUNC('month',DATEADD('month',-2,CURRENT_DATE()))::DATE THEN cr END),0) AS M2,
               COALESCE(MAX(CASE WHEN month=DATE_TRUNC('month',DATEADD('month',-1,CURRENT_DATE()))::DATE THEN cr END),0) AS M3,
               CASE
                   WHEN COALESCE(MAX(CASE WHEN month=DATE_TRUNC('month',DATEADD('month',-3,CURRENT_DATE()))::DATE THEN cr END),0) > 0
                   THEN ROUND(
                       (COALESCE(MAX(CASE WHEN month=DATE_TRUNC('month',DATEADD('month',-1,CURRENT_DATE()))::DATE THEN cr END),0) -
                        COALESCE(MAX(CASE WHEN month=DATE_TRUNC('month',DATEADD('month',-3,CURRENT_DATE()))::DATE THEN cr END),0)) /
                       COALESCE(MAX(CASE WHEN month=DATE_TRUNC('month',DATEADD('month',-3,CURRENT_DATE()))::DATE THEN cr END),0) * 100.0, 1)
                   ELSE NULL
               END AS PCT_CHANGE
        FROM m GROUP BY 1
    """.format(theater=theater, region_sql=region_sql, feature_filter=feature_filter), ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_cross_feature(theater, region_sql):
    return conn.query("""
        WITH al AS (
            SELECT SALESFORCE_ACCOUNT_ID
            FROM sales.raven.d_salesforce_account_customers
            WHERE GEO='{theater}' {region_sql}
              AND IS_CAPACITY_CUSTOMER=TRUE AND IS_REVENUE_ACCOUNT=TRUE
        ), m AS (
            SELECT a.SALESFORCE_ACCOUNT_ID,
                   DATE_TRUNC('month', f.general_date)::DATE AS month,
                   ROUND(SUM(f.total_credits),0) AS cr
            FROM sales.raven.A360_PRODUCT_CATEGORY_DAILY_VIEW f
            JOIN al a ON a.SALESFORCE_ACCOUNT_ID = f.salesforce_account_id
            WHERE f.general_date >= DATEADD('month',-3,DATE_TRUNC('month',CURRENT_DATE()))
              AND f.general_date < DATE_TRUNC('month',CURRENT_DATE())
              AND (f.product_category='Data Engineering'
                   OR (f.product_category='Analytics' AND f.primary_feature='Lakehouse Analytics'))
            GROUP BY 1,2
        ), p AS (
            SELECT SALESFORCE_ACCOUNT_ID,
                   COALESCE(MAX(CASE WHEN month=DATE_TRUNC('month',DATEADD('month',-3,CURRENT_DATE()))::DATE THEN cr END),0) AS M1,
                   COALESCE(MAX(CASE WHEN month=DATE_TRUNC('month',DATEADD('month',-1,CURRENT_DATE()))::DATE THEN cr END),0) AS M3,
                   CASE WHEN COALESCE(MAX(CASE WHEN month=DATE_TRUNC('month',DATEADD('month',-3,CURRENT_DATE()))::DATE THEN cr END),0) > 0
                        THEN ROUND(
                            (COALESCE(MAX(CASE WHEN month=DATE_TRUNC('month',DATEADD('month',-1,CURRENT_DATE()))::DATE THEN cr END),0) -
                             COALESCE(MAX(CASE WHEN month=DATE_TRUNC('month',DATEADD('month',-3,CURRENT_DATE()))::DATE THEN cr END),0)) /
                            COALESCE(MAX(CASE WHEN month=DATE_TRUNC('month',DATEADD('month',-3,CURRENT_DATE()))::DATE THEN cr END),0)*100.0, 1)
                        ELSE NULL END AS PCT_CHANGE
            FROM m GROUP BY 1
        ), fu AS (
            SELECT a.SALESFORCE_ACCOUNT_ID,
                   SUM(CASE WHEN f.primary_feature IN ('Openflow','Openflow Connector','Iceberg Openflow') THEN f.total_credits ELSE 0 END) AS openflow,
                   SUM(CASE WHEN f.primary_feature IN ('Snowpipe Streaming','Snowpipe Streaming v2') THEN f.total_credits ELSE 0 END) AS streaming,
                   SUM(CASE WHEN f.primary_feature ILIKE 'Iceberg%' THEN f.total_credits ELSE 0 END) AS iceberg,
                   SUM(CASE WHEN f.primary_feature='DT refresh' THEN f.total_credits ELSE 0 END) AS dt,
                   SUM(CASE WHEN f.primary_feature IN ('Snowpark DE','Iceberg Snowpark DE') THEN f.total_credits ELSE 0 END) AS snowpark,
                   SUM(CASE WHEN f.primary_feature='dbt projects in Snowflake' THEN f.total_credits ELSE 0 END) AS dbt_projects,
                   SUM(CASE WHEN f.product_category='Analytics' AND f.primary_feature='Lakehouse Analytics' THEN f.total_credits ELSE 0 END) AS lakehouse
            FROM sales.raven.A360_PRODUCT_CATEGORY_DAILY_VIEW f
            JOIN al a ON a.SALESFORCE_ACCOUNT_ID = f.salesforce_account_id
            WHERE f.general_date >= DATEADD('month',-3,DATE_TRUNC('month',CURRENT_DATE()))
              AND f.general_date < DATE_TRUNC('month',CURRENT_DATE())
            GROUP BY 1
        ), c AS (
            SELECT p.*,
                   COALESCE(fu.openflow,0) AS openflow, COALESCE(fu.streaming,0) AS streaming,
                   COALESCE(fu.iceberg,0) AS iceberg, COALESCE(fu.dt,0) AS dt,
                   COALESCE(fu.snowpark,0) AS snowpark, COALESCE(fu.dbt_projects,0) AS dbt_projects,
                   COALESCE(fu.lakehouse,0) AS lakehouse
            FROM p LEFT JOIN fu ON p.SALESFORCE_ACCOUNT_ID = fu.SALESFORCE_ACCOUNT_ID
            WHERE p.M1 > 0 AND p.M3 > 0
        )
        SELECT FEATURE,
               ROUND(AVG(CASE WHEN HAS_FEATURE THEN PCT_CHANGE END),1) AS AVG_WITH,
               ROUND(AVG(CASE WHEN NOT HAS_FEATURE THEN PCT_CHANGE END),1) AS AVG_WITHOUT,
               COUNT(CASE WHEN HAS_FEATURE THEN 1 END) AS ACCTS_WITH,
               COUNT(CASE WHEN NOT HAS_FEATURE THEN 1 END) AS ACCTS_WITHOUT
        FROM (
            SELECT 'dbt Projects in Snowflake' AS FEATURE, dbt_projects>0 AS HAS_FEATURE, PCT_CHANGE FROM c
            UNION ALL SELECT 'Snowpark DE', snowpark>0, PCT_CHANGE FROM c
            UNION ALL SELECT 'Dynamic Tables', dt>0, PCT_CHANGE FROM c
            UNION ALL SELECT 'Iceberg (Interop Storage)', iceberg>0, PCT_CHANGE FROM c
            UNION ALL SELECT 'Openflow', openflow>0, PCT_CHANGE FROM c
            UNION ALL SELECT 'Lakehouse Analytics', lakehouse>0, PCT_CHANGE FROM c
            UNION ALL SELECT 'Snowpipe Streaming', streaming>0, PCT_CHANGE FROM c
        ) GROUP BY FEATURE
        ORDER BY (AVG(CASE WHEN HAS_FEATURE THEN PCT_CHANGE END) - AVG(CASE WHEN NOT HAS_FEATURE THEN PCT_CHANGE END)) DESC NULLS LAST
    """.format(theater=theater, region_sql=region_sql), ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_features(theater, region_sql):
    return conn.query("""
        SELECT * FROM (
            SELECT a.SALESFORCE_ACCOUNT_ID,
                   a.SALESFORCE_ACCOUNT_NAME AS ACCOUNT,
                   a.SALESFORCE_OWNER_NAME AS AE,
                   a.LEAD_SALES_ENGINEER_NAME AS SE,
                   a.SALES_AREA AS REGION,
                   ROUND(SUM(CASE WHEN f.product_category='Data Engineering'
                       OR (f.product_category='Analytics' AND f.primary_feature='Lakehouse Analytics')
                       THEN f.total_credits ELSE 0 END),0) AS TOTAL_CREDITS,
                   ROUND(SUM(CASE WHEN f.primary_feature IN ('Openflow','Openflow Connector','Iceberg Openflow')
                       THEN f.total_credits ELSE 0 END),0) AS OPENFLOW,
                   ROUND(SUM(CASE WHEN f.primary_feature IN ('Snowpipe Streaming','Snowpipe Streaming v2')
                       THEN f.total_credits ELSE 0 END),0) AS STREAMING,
                   ROUND(SUM(CASE WHEN f.primary_feature ILIKE 'Iceberg%'
                       THEN f.total_credits ELSE 0 END),0) AS ICEBERG,
                   ROUND(SUM(CASE WHEN f.primary_feature='DT refresh'
                       THEN f.total_credits ELSE 0 END),0) AS DT,
                   ROUND(SUM(CASE WHEN f.primary_feature IN ('Snowpark DE','Iceberg Snowpark DE')
                       THEN f.total_credits ELSE 0 END),0) AS SNOWPARK,
                   ROUND(SUM(CASE WHEN f.product_category='Analytics' AND f.primary_feature='Lakehouse Analytics'
                       THEN f.total_credits ELSE 0 END),0) AS LAKEHOUSE
            FROM sales.raven.A360_PRODUCT_CATEGORY_DAILY_VIEW f
            JOIN (
                SELECT SALESFORCE_ACCOUNT_ID, SALESFORCE_ACCOUNT_NAME,
                       SALESFORCE_OWNER_NAME, LEAD_SALES_ENGINEER_NAME, SALES_AREA
                FROM sales.raven.d_salesforce_account_customers
                WHERE GEO='{theater}' {region_sql}
                  AND IS_CAPACITY_CUSTOMER=TRUE AND IS_REVENUE_ACCOUNT=TRUE
            ) a ON a.SALESFORCE_ACCOUNT_ID = f.salesforce_account_id
            WHERE f.general_date >= DATE_TRUNC('month',DATEADD('month',-1,CURRENT_DATE()))
              AND f.general_date < DATE_TRUNC('month',CURRENT_DATE())
            GROUP BY 1,2,3,4,5
        ) WHERE TOTAL_CREDITS > 0
        ORDER BY TOTAL_CREDITS DESC
    """.format(theater=theater, region_sql=region_sql), ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_maturity(theater, region_sql):
    return conn.query("""
        SELECT * FROM (
            WITH al AS (
                SELECT SALESFORCE_ACCOUNT_ID, SALESFORCE_ACCOUNT_NAME,
                       SALESFORCE_OWNER_NAME, LEAD_SALES_ENGINEER_NAME, SALES_AREA
                FROM sales.raven.d_salesforce_account_customers
                WHERE GEO='{theater}' {region_sql}
                  AND IS_CAPACITY_CUSTOMER=TRUE AND IS_REVENUE_ACCOUNT=TRUE
            ), fu AS (
                SELECT a.SALESFORCE_ACCOUNT_ID, a.SALESFORCE_ACCOUNT_NAME AS ACCOUNT,
                       a.SALESFORCE_OWNER_NAME AS AE, a.LEAD_SALES_ENGINEER_NAME AS SE,
                       a.SALES_AREA AS REGION,
                       SUM(CASE WHEN f.primary_feature IN ('Openflow','Openflow Connector','Iceberg Openflow') THEN f.total_credits ELSE 0 END) AS openflow,
                       SUM(CASE WHEN f.primary_feature IN ('Snowpipe Streaming','Snowpipe Streaming v2') THEN f.total_credits ELSE 0 END) AS streaming,
                       SUM(CASE WHEN f.primary_feature ILIKE 'Iceberg%' THEN f.total_credits ELSE 0 END) AS iceberg,
                       SUM(CASE WHEN f.primary_feature='DT refresh' THEN f.total_credits ELSE 0 END) AS dt,
                       SUM(CASE WHEN f.primary_feature IN ('Snowpark DE','Iceberg Snowpark DE') THEN f.total_credits ELSE 0 END) AS snowpark,
                       SUM(CASE WHEN f.primary_feature='dbt projects in Snowflake' THEN f.total_credits ELSE 0 END) AS dbt_projects,
                       SUM(CASE WHEN f.product_category='Analytics' AND f.primary_feature='Lakehouse Analytics' THEN f.total_credits ELSE 0 END) AS lakehouse,
                       ROUND(SUM(CASE WHEN f.product_category='Data Engineering'
                           OR (f.product_category='Analytics' AND f.primary_feature='Lakehouse Analytics')
                           THEN f.total_credits ELSE 0 END),0) AS TOTAL_CREDITS
                FROM sales.raven.A360_PRODUCT_CATEGORY_DAILY_VIEW f
                JOIN al a ON a.SALESFORCE_ACCOUNT_ID = f.salesforce_account_id
                WHERE f.general_date >= DATE_TRUNC('month',DATEADD('month',-1,CURRENT_DATE()))
                  AND f.general_date < DATE_TRUNC('month',CURRENT_DATE())
                GROUP BY 1,2,3,4,5
            )
            SELECT SALESFORCE_ACCOUNT_ID, ACCOUNT, AE, SE, REGION, TOTAL_CREDITS,
                   CASE WHEN dt>0 THEN '✓' ELSE '' END AS DT,
                   CASE WHEN iceberg>0 THEN '✓' ELSE '' END AS ICEBERG,
                   CASE WHEN snowpark>0 THEN '✓' ELSE '' END AS SNOWPARK,
                   CASE WHEN streaming>0 THEN '✓' ELSE '' END AS STREAMING,
                   CASE WHEN dbt_projects>0 THEN '✓' ELSE '' END AS DBT,
                   CASE WHEN lakehouse>0 THEN '✓' ELSE '' END AS LAKEHOUSE,
                   CASE WHEN openflow>0 THEN '✓' ELSE '' END AS OPENFLOW,
                   CASE
                       WHEN (CASE WHEN dt>0 THEN 1 ELSE 0 END + CASE WHEN iceberg>0 THEN 1 ELSE 0 END +
                             CASE WHEN snowpark>0 THEN 1 ELSE 0 END + CASE WHEN streaming>0 THEN 1 ELSE 0 END +
                             CASE WHEN dbt_projects>0 THEN 1 ELSE 0 END + CASE WHEN lakehouse>0 THEN 1 ELSE 0 END) >= 4
                       THEN 'Tier 1 — Full Stack'
                       WHEN (CASE WHEN dt>0 THEN 1 ELSE 0 END + CASE WHEN snowpark>0 THEN 1 ELSE 0 END +
                             CASE WHEN dbt_projects>0 THEN 1 ELSE 0 END) >= 2
                       THEN 'Tier 2 — DE Heavy, No Lakehouse'
                       ELSE 'Tier 3 — Growing, Low Feature Depth'
                   END AS TIER
            FROM fu
        ) WHERE TOTAL_CREDITS > 0
        ORDER BY TOTAL_CREDITS DESC
    """.format(theater=theater, region_sql=region_sql), ttl=0)


# ─── Helpers ────────────────────────────────────────────────────────────────

def add_sfdc(df, id_col="SALESFORCE_ACCOUNT_ID"):
    df = df.copy()
    df["SFDC_LINK"] = df[id_col].apply(lambda x: SFDC.format(x) if pd.notna(x) and x else "")
    return df


def pct_color(val):
    if pd.isna(val):
        return ""
    return f"color: {'#16a34a' if val >= 0 else '#dc2626'}; font-weight: 600"


def fmt_cr(v):
    if pd.isna(v):
        return "—"
    return f"{int(v):,}"


def fmt_pct(v):
    if pd.isna(v):
        return "—"
    return f"{v:+.1f}%"


# ─── Load core data ──────────────────────────────────────────────────────────

with st.spinner("Loading territory data…"):
    df_meta = q_meta(theater, region_sql)
    df_summary = q_summary(theater, region_sql, feature_filter)
    df_pivoted = q_pivoted(theater, region_sql, feature_filter)

# ─── Territory Metadata Header ───────────────────────────────────────────────

acct_count = int(df_meta["ACCOUNT_COUNT"].iloc[0]) if df_meta is not None and not df_meta.empty else "—"
rvp = str(df_meta["RVP"].iloc[0]) if df_meta is not None and not df_meta.empty and pd.notna(df_meta["RVP"].iloc[0]) else "—"
se_rvp = str(df_meta["SE_RVP"].iloc[0]) if df_meta is not None and not df_meta.empty and "SE_RVP" in df_meta.columns and pd.notna(df_meta["SE_RVP"].iloc[0]) else ""

region_label = "All Regions" if region == ALL_REGIONS else region
st.title(f"{theater} · {region_label} — DE + Lakehouse Territory Deep Dive")

_meta_parts = [
    f"<b>Theater:</b> {theater}",
    f"<b>Region:</b> {region_label}",
]
if region != ALL_REGIONS:
    _meta_parts.append(f"<b>RVP:</b> {rvp}")
    if se_rvp:
        _meta_parts.append(f"<b>SE RVP:</b> {se_rvp}")
_meta_parts += [
    f"<b>Data Range:</b> {M1_LBL} – {M3_LBL}",
    f"<b>Report Date:</b> {date.today().strftime('%b %d, %Y')}",
    f"<b>Accounts:</b> {acct_count} capacity customers",
]
st.markdown(
    '<p style="font-size:13px; color:#555; margin-top:-10px; margin-bottom:4px">' +
    " &nbsp;|&nbsp; ".join(_meta_parts) +
    "</p>",
    unsafe_allow_html=True,
)

st.divider()

# ─── KPI Cards ───────────────────────────────────────────────────────────────

if not df_summary.empty:
    months_sorted = sorted(df_summary["MONTH"].tolist())
    r_map = {row["MONTH"]: row for _, row in df_summary.iterrows()}
    r1 = r_map[months_sorted[0]]
    r3 = r_map[months_sorted[-1]]

    def delta_pct(v3, v1):
        return f"{(v3 - v1) / v1 * 100:+.1f}% vs {M1_LBL}" if v1 and v1 > 0 else None

    with st.container(horizontal=True):
        st.metric(
            f"Grand Total ({M3_LBL})", f"{int(r3['TOTAL']):,}",
            delta_pct(r3["TOTAL"], r1["TOTAL"]), border=True
        )
        st.metric(
            f"DE Credits ({M3_LBL})", f"{int(r3['DE']):,}",
            delta_pct(r3["DE"], r1["DE"]), border=True
        )
        st.metric(
            "Lakehouse Analytics", f"{int(r3['LAKEHOUSE_ANALYTICS']):,}",
            delta_pct(r3["LAKEHOUSE_ANALYTICS"], r1["LAKEHOUSE_ANALYTICS"]) if r1["LAKEHOUSE_ANALYTICS"] > 0 else None,
            border=True
        )
        st.metric(
            "Interop Storage", f"{int(r3['INTEROP_STORAGE']):,}",
            delta_pct(r3["INTEROP_STORAGE"], r1["INTEROP_STORAGE"]) if r1["INTEROP_STORAGE"] > 0 else None,
            border=True
        )
        st.metric("Active Accounts", f"{int(r3['ACTIVE_ACCOUNTS'])}", border=True)
        if not df_pivoted.empty:
            top = df_pivoted.sort_values("M3", ascending=False).iloc[0]
            st.metric(
                f"Top Account ({M3_LBL})",
                f"{int(top['M3']):,}",
                (top["ACCOUNT"][:28] + "…") if len(top["ACCOUNT"]) > 28 else top["ACCOUNT"],
                border=True,
            )

if selected_features != ALL_FEATURES:
    st.info(f"📌 **Feature filter active:** showing consumption for {', '.join(selected_features)} only.")

st.divider()

# ─── 7 Section Tabs ─────────────────────────────────────────────────────────
tabs = st.tabs([
    "📊 Consumption Summary",
    "🔬 Cross-Feature",
    "🎯 Adoption Targets",
    "🏗️ Maturity Tiers",
    "💡 Recommendations",
    "🎙️ Gong Insights",
    "🏆 Use Case Win/Loss",
    "🐛 Product Gaps",
    "💼 Business Problems",
])

# ── Tab 1: Territory Overview (sub-tabs) ─────────────────────────────────────
with tabs[0]:
    _ov_tabs = st.tabs(["📊 Territory Summary", "🥇 Top 30", "🚀 Growers", "📉 Decliners"])

with _ov_tabs[0]:
    st.subheader(f"Territory-Level Consumption — {M1_LBL} to {M3_LBL}")

    if df_summary.empty:
        st.warning("No data found for this territory.")
    else:
        # Build display table with trend row
        display = df_summary.copy()
        display["MONTH"] = pd.to_datetime(display["MONTH"]).dt.strftime("%b %Y")

        credit_cols = ["TOTAL", "DE", "INGESTION", "TRANSFORMATION", "INTEROP_STORAGE", "LAKEHOUSE_ANALYTICS"]

        def pct_or_dash(v3, v1):
            if v1 and v1 > 0:
                return f"{(v3 - v1) / v1 * 100:+.1f}%"
            return "—"

        trend_row = {
            "MONTH": f"Trend ({M1_LBL}→{M3_LBL})",
            "TOTAL": pct_or_dash(r3["TOTAL"], r1["TOTAL"]),
            "DE": pct_or_dash(r3["DE"], r1["DE"]),
            "INGESTION": pct_or_dash(r3["INGESTION"], r1["INGESTION"]),
            "TRANSFORMATION": pct_or_dash(r3["TRANSFORMATION"], r1["TRANSFORMATION"]),
            "INTEROP_STORAGE": pct_or_dash(r3["INTEROP_STORAGE"], r1["INTEROP_STORAGE"]),
            "LAKEHOUSE_ANALYTICS": pct_or_dash(r3["LAKEHOUSE_ANALYTICS"], r1["LAKEHOUSE_ANALYTICS"]),
            "ACTIVE_ACCOUNTS": "—",
        }

        for col in credit_cols:
            display[col] = display[col].apply(lambda v: f"{int(v):,}" if pd.notna(v) else "—")

        trend_df = pd.DataFrame([trend_row])
        combined = pd.concat([display, trend_df], ignore_index=True)
        combined = combined.rename(columns={
            "MONTH": "Month", "TOTAL": "Grand Total", "DE": "DE Credits",
            "INGESTION": "Ingestion", "TRANSFORMATION": "Transformation",
            "INTEROP_STORAGE": "Interop Storage", "LAKEHOUSE_ANALYTICS": "Lakehouse Analytics",
            "ACTIVE_ACCOUNTS": "Active Accts",
        })

        def style_last_row(df):
            styles = pd.DataFrame("", index=df.index, columns=df.columns)
            last = df.index[-1]
            styles.iloc[-1, 0] = "font-weight:700; background:#f0f8ff"
            for col in df.columns[1:]:
                val = str(df.loc[last, col])
                if val.startswith("+"):
                    styles.loc[last, col] = "color:#16a34a; font-weight:700; background:#f0f8ff"
                elif val.startswith("-"):
                    styles.loc[last, col] = "color:#dc2626; font-weight:700; background:#f0f8ff"
                else:
                    styles.loc[last, col] = "background:#f0f8ff"
            return styles

        st.dataframe(
            combined.style.apply(style_last_row, axis=None),
            hide_index=True,
            use_container_width=True,
        )
        st.caption(
            "**Ingestion**: COPY, Snowpipe, Openflow, Streaming  ·  "
            "**Transformation**: Tasks, DML, DT Refresh, Snowpark DE, dbt  ·  "
            "**Interop Storage**: All Iceberg features  ·  "
            "**Lakehouse Analytics**: Analytics workloads on Lakehouse pattern"
        )

        # Charts — split by scale to keep each readable
        st.markdown("#### Monthly Trends")
        chart_base = df_summary.copy().sort_values("MONTH")
        chart_base["MONTH"] = pd.to_datetime(chart_base["MONTH"])

        col1, col2, col3 = st.columns(3)

        with col1:
            with st.container(border=True):
                st.markdown("**Ingestion & Transformation** _(large scale)_")
                c1 = chart_base.set_index("MONTH")[["INGESTION", "TRANSFORMATION"]].rename(
                    columns={"INGESTION": "Ingestion", "TRANSFORMATION": "Transformation"}
                )
                st.line_chart(c1, height=220)

        with col2:
            with st.container(border=True):
                st.markdown("**Interop Storage** _(mid scale)_")
                c2 = chart_base.set_index("MONTH")[["INTEROP_STORAGE"]].rename(
                    columns={"INTEROP_STORAGE": "Interop Storage"}
                )
                st.line_chart(c2, height=220)

        with col3:
            with st.container(border=True):
                st.markdown("**Lakehouse Analytics** _(growth scale)_")
                c3 = chart_base.set_index("MONTH")[["LAKEHOUSE_ANALYTICS"]].rename(
                    columns={"LAKEHOUSE_ANALYTICS": "Lakehouse Analytics"}
                )
                st.line_chart(c3, height=220)

with _ov_tabs[1]:
    st.subheader(f"Top 30 Accounts — DE + Lakehouse Credits ({M3_LBL})")
    if df_pivoted.empty:
        st.warning("No data found.")
    else:
        top30 = df_pivoted.sort_values("M3", ascending=False).head(30).reset_index(drop=True)
        top30 = add_sfdc(top30)
        top30.index = top30.index + 1
        display = top30[["ACCOUNT", "REGION", "AE", "SE", "M1", "M2", "M3", "PCT_CHANGE", "SFDC_LINK"]].rename(
            columns={"M1": M1_LBL, "M2": M2_LBL, "M3": M3_LBL, "PCT_CHANGE": "Δ%"}
        )
        styled = (
            display.style
            .applymap(pct_color, subset=["Δ%"])
            .format({
                M1_LBL: lambda v: fmt_cr(v),
                M2_LBL: lambda v: fmt_cr(v),
                M3_LBL: lambda v: fmt_cr(v),
                "Δ%": lambda v: fmt_pct(v),
            })
        )
        st.dataframe(
            styled,
            column_config={"SFDC_LINK": st.column_config.LinkColumn("SFDC", display_text="↗", width="small")},
            use_container_width=True,
        )

with _ov_tabs[2]:
    st.subheader(f"Top 15 Growers — {M1_LBL} → {M3_LBL}")
    growers = df_pivoted[df_pivoted["M1"] > 0].copy()
    growers["DELTA"] = growers["M3"] - growers["M1"]
    growers = growers.sort_values("DELTA", ascending=False).head(15).reset_index(drop=True)
    growers = add_sfdc(growers)
    growers.index = growers.index + 1
    display = growers[["ACCOUNT", "REGION", "AE", "SE", "M1", "M2", "M3", "DELTA", "PCT_CHANGE", "SFDC_LINK"]].rename(
        columns={"M1": M1_LBL, "M2": M2_LBL, "M3": M3_LBL, "DELTA": "Δ Credits", "PCT_CHANGE": "Δ%"}
    )
    styled = (
        display.style
        .applymap(pct_color, subset=["Δ%"])
        .applymap(pct_color, subset=["Δ Credits"])
        .format({M1_LBL: lambda v: fmt_cr(v), M2_LBL: lambda v: fmt_cr(v),
                 M3_LBL: lambda v: fmt_cr(v), "Δ Credits": lambda v: fmt_cr(v), "Δ%": lambda v: fmt_pct(v)})
    )
    st.dataframe(
        styled,
        column_config={"SFDC_LINK": st.column_config.LinkColumn("SFDC", display_text="↗", width="small")},
        use_container_width=True,
    )
    low_base = growers[growers["M1"] < 100]
    if not low_base.empty:
        st.warning(
            f"⚠️ **Near-zero baseline:** {', '.join(low_base['ACCOUNT'].tolist())} — "
            f"< 100 credits in {M1_LBL}. Focus on absolute volume for true growth stories."
        )

with _ov_tabs[3]:
    st.subheader(f"Top 15 Decliners — {M1_LBL} → {M3_LBL}")
    decliners = df_pivoted[(df_pivoted["M1"] > 0) & (df_pivoted["M3"] < df_pivoted["M1"])].copy()
    decliners["DELTA"] = decliners["M3"] - decliners["M1"]
    decliners = decliners.sort_values("DELTA", ascending=True).head(15).reset_index(drop=True)
    decliners = add_sfdc(decliners)
    decliners.index = decliners.index + 1
    display = decliners[["ACCOUNT", "REGION", "AE", "SE", "M1", "M2", "M3", "DELTA", "PCT_CHANGE", "SFDC_LINK"]].rename(
        columns={"M1": M1_LBL, "M2": M2_LBL, "M3": M3_LBL, "DELTA": "Δ Credits", "PCT_CHANGE": "Δ%"}
    )
    styled = (
        display.style
        .applymap(pct_color, subset=["Δ%"])
        .applymap(pct_color, subset=["Δ Credits"])
        .format({M1_LBL: lambda v: fmt_cr(v), M2_LBL: lambda v: fmt_cr(v),
                 M3_LBL: lambda v: fmt_cr(v), "Δ Credits": lambda v: fmt_cr(v), "Δ%": lambda v: fmt_pct(v)})
    )
    st.dataframe(
        styled,
        column_config={"SFDC_LINK": st.column_config.LinkColumn("SFDC", display_text="↗", width="small")},
        use_container_width=True,
    )
    urgent = decliners[decliners["PCT_CHANGE"] < -50]
    if not urgent.empty:
        st.error(f"🚨 **Urgent investigation needed:** {', '.join(urgent['ACCOUNT'].tolist())} — declined > 50% in 90 days.")

# ── Tab 2: Cross-Feature ──────────────────────────────────────────────────────
with tabs[1]:
    st.subheader("Cross-Feature Adoption Patterns & Growth Signals")
    st.caption(
        "Always uses full feature stack regardless of feature filter. "
        "Feature presence = any credits > 0 over the 3-month window. "
        "Treat **account counts as the primary signal** — growth averages are skewed by ramp-phase outliers."
    )
    with st.spinner("Loading cross-feature data…"):
        df_cf = q_cross_feature(theater, region_sql)

    if not df_cf.empty:
        def signal(row):
            if row["ACCTS_WITH"] == 0:
                return "🔴 Zero adoption — priority gap"
            w, wo = row["AVG_WITH"], row["AVG_WITHOUT"]
            if pd.isna(w) or pd.isna(wo):
                return "⚪ Insufficient data"
            diff = w - wo
            return "🟢 Adopters growing faster" if diff > 20 else "🟢 Positive correlation" if diff > 0 else "⚪ Mixed signal" if diff > -20 else "🔴 Non-adopters growing faster"

        df_cf["Signal"] = df_cf.apply(signal, axis=1)
        df_cf = df_cf.rename(columns={
            "FEATURE": "Feature", "ACCTS_WITH": "Accts Using", "ACCTS_WITHOUT": "Accts Without",
            "AVG_WITH": "Avg Growth (WITH)", "AVG_WITHOUT": "Avg Growth (WITHOUT)",
        })

        def color_num(v):
            if isinstance(v, float):
                return "color:#16a34a;font-weight:600" if v > 0 else "color:#dc2626;font-weight:600" if v < 0 else ""
            return ""

        styled_cf = (
            df_cf[["Feature", "Accts Using", "Accts Without", "Avg Growth (WITH)", "Avg Growth (WITHOUT)", "Signal"]]
            .style
            .applymap(color_num, subset=["Avg Growth (WITH)", "Avg Growth (WITHOUT)"])
            .format({"Avg Growth (WITH)": lambda v: fmt_pct(v), "Avg Growth (WITHOUT)": lambda v: fmt_pct(v)})
        )
        st.dataframe(styled_cf, hide_index=True, use_container_width=True)

        st.markdown("#### 🔍 Key Signal Interpretations")
        top_feat = df_cf.iloc[0]["Feature"] if not df_cf.empty else "dbt Projects in Snowflake"
        zero_feats = df_cf[df_cf["Accts Using"] == 0]["Feature"].tolist()
        st.markdown(
            f"- **{top_feat} → Strongest Growth Predictor.** The gap between adopters and non-adopters is the clearest, outlier-free signal. Non-users are the top modernization priority.\n"
            "- **Dynamic Tables → Correlated with Higher Consumption.** DT-using accounts show meaningfully higher average growth. Sophisticated pipeline platforms are predominantly on DTs.\n"
            "- **Lakehouse Analytics & Iceberg → Next Maturity Tier.** Heavy DE accounts with near-zero Lakehouse credits are natural next movers.\n"
            + (f"- **{', '.join(zero_feats)} → Zero Adoption.** No accounts with meaningful credits. HCLS use cases (claims, EHR events, ADT notifications) are natural fits."
               if zero_feats else "")
        )

# ── Tab 3: Adoption Targets ───────────────────────────────────────────────────
with tabs[2]:
    st.subheader(f"Account Feature Adoption Targets ({M3_LBL})")
    st.caption("Top 10 highest-consuming accounts with < 500 credits of the target feature. Sorted by total DE+Lakehouse credits.")
    with st.spinner("Loading feature adoption data…"):
        df_feat = q_features(theater, region_sql)

    if not df_feat.empty:
        INTEROP_LABEL = "DE Interoperable Storage (Iceberg Ingestion & Transformation)"
        LAKEHOUSE_LABEL = "Lakehouse Analytics (Iceberg Reads)"

        feat_tabs = st.tabs(["Openflow", "Snowpipe Streaming", "DE - Interoperable Storage", "Lakehouse Analytics", "Dynamic Tables", "Snowpark"])

        simple_configs = [
            ("Openflow", "OPENFLOW", "🌊 Massive whitespace — top accounts have zero Openflow credits. The connector conversation is wide open."),
            ("Snowpipe Streaming", "STREAMING", "⚡ Real-time ingestion is the largest untapped opportunity. Claims feeds, EHR events, and ADT notifications are natural HCLS fits."),
            ("Dynamic Tables", "DT", "♻️ DT-using accounts show higher average growth. High-credit accounts on Tasks/DML are the top DT migration targets."),
            ("Snowpark", "SNOWPARK", "🐍 Snowpark is at high penetration but several growing accounts have minimal usage. Introduce for Python-based pipelines during expansion conversations."),
        ]

        def _render_interop_lakehouse(df_source, filter_col, filter_label, blurb):
            df_t = df_source[df_source[filter_col] < 500].head(10).reset_index(drop=True)
            df_t = df_t.copy()
            df_t[INTEROP_LABEL] = df_t["ICEBERG"]
            df_t[LAKEHOUSE_LABEL] = df_t["LAKEHOUSE"]
            df_t = add_sfdc(df_t)
            df_t.index = df_t.index + 1
            show = ["ACCOUNT", "REGION", "AE", "SE", "TOTAL_CREDITS", INTEROP_LABEL, LAKEHOUSE_LABEL, "SFDC_LINK"]
            disp = df_t[[c for c in show if c in df_t.columns]].rename(columns={"TOTAL_CREDITS": f"{M3_LBL} Credits"})
            styled_t = disp.style.format(
                {f"{M3_LBL} Credits": lambda v: fmt_cr(v),
                 INTEROP_LABEL: lambda v: fmt_cr(v),
                 LAKEHOUSE_LABEL: lambda v: fmt_cr(v)}
            )
            st.dataframe(
                styled_t,
                column_config={"SFDC_LINK": st.column_config.LinkColumn("SFDC", display_text="↗", width="small")},
                use_container_width=True,
            )
            st.info(blurb)

        with feat_tabs[0]:
            df_t = df_feat[df_feat["OPENFLOW"] < 500].head(10).reset_index(drop=True)
            df_t = add_sfdc(df_t)
            df_t.index = df_t.index + 1
            disp = df_t[["ACCOUNT", "REGION", "AE", "SE", "TOTAL_CREDITS", "OPENFLOW", "SFDC_LINK"]].rename(columns={"TOTAL_CREDITS": f"{M3_LBL} Credits"})
            st.dataframe(disp.style.format({f"{M3_LBL} Credits": lambda v: fmt_cr(v), "OPENFLOW": lambda v: fmt_cr(v)}),
                         column_config={"SFDC_LINK": st.column_config.LinkColumn("SFDC", display_text="↗", width="small")}, use_container_width=True)
            st.info("🌊 Massive whitespace — top accounts have zero Openflow credits. The connector conversation is wide open.")

        with feat_tabs[1]:
            df_t = df_feat[df_feat["STREAMING"] < 500].head(10).reset_index(drop=True)
            df_t = add_sfdc(df_t)
            df_t.index = df_t.index + 1
            disp = df_t[["ACCOUNT", "REGION", "AE", "SE", "TOTAL_CREDITS", "STREAMING", "SFDC_LINK"]].rename(columns={"TOTAL_CREDITS": f"{M3_LBL} Credits"})
            st.dataframe(disp.style.format({f"{M3_LBL} Credits": lambda v: fmt_cr(v), "STREAMING": lambda v: fmt_cr(v)}),
                         column_config={"SFDC_LINK": st.column_config.LinkColumn("SFDC", display_text="↗", width="small")}, use_container_width=True)
            st.info("⚡ Real-time ingestion is the largest untapped opportunity. Claims feeds, EHR events, and ADT notifications are natural HCLS fits.")

        with feat_tabs[2]:
            _render_interop_lakehouse(df_feat, "ICEBERG", INTEROP_LABEL,
                "🏔️ Accounts with high DE credits but low Iceberg usage — prime targets for the Interoperable Storage story.")

        with feat_tabs[3]:
            _render_interop_lakehouse(df_feat, "LAKEHOUSE", LAKEHOUSE_LABEL,
                "📊 Accounts with high DE credits but low Lakehouse Analytics usage — the analytics-on-Iceberg motion is wide open.")

        with feat_tabs[4]:
            df_t = df_feat[df_feat["DT"] < 500].head(10).reset_index(drop=True)
            df_t = add_sfdc(df_t)
            df_t.index = df_t.index + 1
            disp = df_t[["ACCOUNT", "REGION", "AE", "SE", "TOTAL_CREDITS", "DT", "SFDC_LINK"]].rename(columns={"TOTAL_CREDITS": f"{M3_LBL} Credits"})
            st.dataframe(disp.style.format({f"{M3_LBL} Credits": lambda v: fmt_cr(v), "DT": lambda v: fmt_cr(v)}),
                         column_config={"SFDC_LINK": st.column_config.LinkColumn("SFDC", display_text="↗", width="small")}, use_container_width=True)
            st.info("♻️ DT-using accounts show higher average growth. High-credit accounts on Tasks/DML are the top DT migration targets.")

        with feat_tabs[5]:
            df_t = df_feat[df_feat["SNOWPARK"] < 500].head(10).reset_index(drop=True)
            df_t = add_sfdc(df_t)
            df_t.index = df_t.index + 1
            disp = df_t[["ACCOUNT", "REGION", "AE", "SE", "TOTAL_CREDITS", "SNOWPARK", "SFDC_LINK"]].rename(columns={"TOTAL_CREDITS": f"{M3_LBL} Credits"})
            st.dataframe(disp.style.format({f"{M3_LBL} Credits": lambda v: fmt_cr(v), "SNOWPARK": lambda v: fmt_cr(v)}),
                         column_config={"SFDC_LINK": st.column_config.LinkColumn("SFDC", display_text="↗", width="small")}, use_container_width=True)
            st.info("🐍 Snowpark is at high penetration but several growing accounts have minimal usage. Introduce for Python-based pipelines during expansion conversations.")

# ── Tab 4: Maturity Tiers ─────────────────────────────────────────────────────
with tabs[3]:
    st.subheader(f"Feature Maturity Tiers — {M3_LBL}")
    st.caption(
        "**Tier 1 (Full Stack):** ≥ 4 of (DT, Iceberg, Snowpark, Streaming, dbt, Lakehouse) active  ·  "
        "**Tier 2 (DE Heavy):** 2-3 of (DT + Snowpark + dbt), missing Iceberg/Streaming/Lakehouse  ·  "
        "**Tier 3 (Growing):** 0-1 features"
    )
    with st.spinner("Loading maturity data…"):
        df_mat = q_maturity(theater, region_sql)

    if not df_mat.empty:
        tier_counts = df_mat["TIER"].value_counts().to_dict()
        with st.container(horizontal=True):
            st.metric("Tier 1 — Full Stack", tier_counts.get("Tier 1 — Full Stack", 0), border=True)
            st.metric("Tier 2 — DE Heavy, No Lakehouse", tier_counts.get("Tier 2 — DE Heavy, No Lakehouse", 0), border=True)
            st.metric("Tier 3 — Growing, Low Feature Depth", tier_counts.get("Tier 3 — Growing, Low Feature Depth", 0), border=True)

        tier_filter = st.multiselect(
            "Filter by Tier",
            options=sorted(df_mat["TIER"].unique().tolist()),
            default=sorted(df_mat["TIER"].unique().tolist()),
        )
        filtered = df_mat[df_mat["TIER"].isin(tier_filter)].copy() if tier_filter else df_mat.copy()
        filtered = add_sfdc(filtered)
        filtered.index = range(1, len(filtered) + 1)

        def tier_style(val):
            if "Tier 1" in str(val):
                return "background-color:#dbeafe; color:#1d4ed8; font-weight:600"
            if "Tier 2" in str(val):
                return "background-color:#fef9c3; color:#854d0e; font-weight:600"
            return "background-color:#f3f4f6; color:#374151; font-weight:600"

        show_cols = ["ACCOUNT", "REGION", "AE", "SE", "TOTAL_CREDITS", "DT", "ICEBERG", "SNOWPARK", "STREAMING", "DBT", "LAKEHOUSE", "OPENFLOW", "TIER", "SFDC_LINK"]
        disp = filtered[[c for c in show_cols if c in filtered.columns]].rename(columns={"TOTAL_CREDITS": f"{M3_LBL} Credits"})
        styled_mat = (
            disp.style
            .applymap(tier_style, subset=["TIER"])
            .format({f"{M3_LBL} Credits": lambda v: fmt_cr(v)})
        )
        st.dataframe(
            styled_mat,
            column_config={"SFDC_LINK": st.column_config.LinkColumn("SFDC", display_text="↗", width="small")},
            use_container_width=True,
            height=600,
        )


# ── Tab 8: Recommendations ───────────────────────────────────────────────────
def _build_ai_context(theater, region_label, df_sum, df_piv, df_cf, df_feat, df_mat):
    lines = [f"TERRITORY: {theater} / {region_label}"]

    if not df_sum.empty:
        s = df_sum.sort_values("MONTH")
        r1, r3 = s.iloc[0], s.iloc[-1]
        t_pct = (r3["TOTAL"] - r1["TOTAL"]) / r1["TOTAL"] * 100 if r1["TOTAL"] > 0 else 0
        lines.append(f"\n3-MONTH CONSUMPTION ({M1_LBL} to {M3_LBL}):")
        lines.append(f"  Grand Total: {int(r1['TOTAL']):,} to {int(r3['TOTAL']):,} credits ({t_pct:+.1f}%)")
        lines.append(f"  Ingestion: {int(r3['INGESTION']):,}  Transformation: {int(r3['TRANSFORMATION']):,}")
        lines.append(f"  Interop Storage (Iceberg): {int(r3['INTEROP_STORAGE']):,}  Lakehouse Analytics: {int(r3['LAKEHOUSE_ANALYTICS']):,}")
        lines.append(f"  Active accounts: {int(r3['ACTIVE_ACCOUNTS'])}")

    if not df_cf.empty:
        lines.append("\nFEATURE ADOPTION AND GROWTH CORRELATION:")
        for _, r in df_cf.iterrows():
            feat = str(r.get("FEATURE", r.get("Feature", "")))
            accts = int(r.get("ACCTS_WITH", r.get("Accts Using", 0)) or 0)
            avg_w = r.get("AVG_WITH", r.get("Avg Growth (WITH)"))
            avg_wo = r.get("AVG_WITHOUT", r.get("Avg Growth (WITHOUT)"))
            w_str = f"{avg_w:+.1f}%" if avg_w is not None and not pd.isna(avg_w) else "N/A"
            wo_str = f"{avg_wo:+.1f}%" if avg_wo is not None and not pd.isna(avg_wo) else "N/A"
            lines.append(f"  {feat}: {accts} accounts using — avg growth {w_str} (users) vs {wo_str} (non-users)")

    if not df_mat.empty:
        tc = df_mat["TIER"].value_counts().to_dict()
        lines.append("\nFEATURE MATURITY TIERS:")
        for tier, cnt in tc.items():
            lines.append(f"  {tier}: {cnt} accounts")

    if not df_piv.empty:
        top5 = df_piv[df_piv["M1"] > 0].sort_values("PCT_CHANGE", ascending=False).head(5)
        if not top5.empty:
            lines.append(f"\nTOP 5 GROWERS ({M1_LBL} to {M3_LBL}, min 1 credit base):")
            for _, r in top5.iterrows():
                region_info = f" [{r['REGION']}]" if "REGION" in r and pd.notna(r.get("REGION")) else ""
                lines.append(f"  {r['ACCOUNT']}{region_info}: {int(r['M3']):,} credits ({r['PCT_CHANGE']:+.1f}%), AE: {r['AE']}, SE: {r['SE']}")

        decliners5 = df_piv[(df_piv["M1"] > 5000) & (df_piv["M3"] < df_piv["M1"])].sort_values("PCT_CHANGE").head(5)
        if not decliners5.empty:
            lines.append("\nAT-RISK HIGH-VOLUME ACCOUNTS (declining):")
            for _, r in decliners5.iterrows():
                lines.append(f"  {r['ACCOUNT']}: {int(r['M3']):,} credits ({r['PCT_CHANGE']:+.1f}%)")

    if not df_feat.empty:
        top10 = df_feat[df_feat["TOTAL_CREDITS"] > 5000].head(10)
        if not top10.empty:
            lines.append(f"\nFEATURE WHITESPACE — HIGH-VOLUME ACCOUNTS (last month credits, gaps = 0 usage):")
            for _, r in top10.iterrows():
                gaps = [f for f, col in [("Openflow","OPENFLOW"),("Streaming","STREAMING"),
                        ("Iceberg","ICEBERG"),("Dyn Tables","DT"),("Snowpark","SNOWPARK"),
                        ("dbt","LAKEHOUSE"),("Lakehouse","LAKEHOUSE")] if r.get(col, 0) == 0]
                region_info = f" [{r['REGION']}]" if "REGION" in r and pd.notna(r.get("REGION")) else ""
                lines.append(f"  {r['ACCOUNT']}{region_info}: {int(r['TOTAL_CREDITS']):,} credits — gaps: {', '.join(gaps) if gaps else 'none'}")

    return "\n".join(lines)


@st.cache_data(ttl=3600, show_spinner=False)
def _call_cortex_recommendations(theater, region_label, context_str):
    safe = context_str.replace("'", "''").replace("\\", "\\\\")
    prompt = (
        "You are a senior Snowflake Data Engineering field expert and sales strategist. "
        "Using ONLY the territory data provided below, give two outputs:\\n\\n"
        "**PART 1 — TOP 10 TERRITORY STRATEGIES**\\n"
        "Provide 10 specific, actionable strategies to grow DE + Lakehouse Analytics consumption "
        "in this territory. Ground each strategy in the actual data (feature adoption gaps, "
        "maturity tier distribution, growth correlations). Be specific — reference feature names, "
        "trends, and account segments where relevant.\\n"
        "For each strategy, assign a **Confidence Score: XX%** (whole number, 0-100) that reflects "
        "how likely this play will drive measurable consumption growth in this specific territory. "
        "Score based on the data signals present: higher score when there is strong account-level "
        "evidence (multiple growers using this motion, positive growth correlation data, large "
        "whitespace in high-volume accounts, clear trend momentum); lower score when evidence is "
        "thin (few accounts, mixed signals, low base credits, speculative). "
        "Scores should vary meaningfully — do NOT decrease by a fixed increment. "
        "Some plays may score similarly if evidence is comparable; others may score much lower "
        "if the territory data is weak for that motion. "
        "For each strategy also include a Target Accounts line listing the specific account "
        "names from the data that are most likely to succeed with that motion — drawn from the "
        "growers, whitespace, and feature adoption data provided. Name real accounts; do not use "
        "placeholders. If fewer than 3 accounts clearly apply, name those and note why. "
        "Format EACH strategy exactly as follows (the bold name on its own line first, then bullets):\\n"
        "**[Short descriptive strategy name]**\\n"
        "- **Confidence Score: XX%**\\n"
        "- **Target Accounts:** [comma-separated account names]\\n"
        "- [Strategy description in 1-2 sentences]\\n\\n"
        "Present strategies sorted by Confidence Score descending (highest first).\\n\\n"
        "**PART 2 — TOP 10 TARGET ACCOUNTS**\\n"
        "Identify the 10 highest-priority individual accounts to engage for consumption growth. "
        "Rank them from highest to lowest priority. For each account provide:\\n"
        "  (a) **Confidence Score: XX%** — how confident you are this specific account will grow "
        "if engaged now, based on its credit trend, whitespace gaps, and growth trajectory. "
        "Score high when the account has strong recent momentum, identifiable feature gaps, and "
        "high absolute credit volume. Score lower for volatile trend, thin data, or near-zero base. "
        "Present accounts sorted by Confidence Score descending (highest first).\\n"
        "Format EACH account exactly as follows (bold account name on its own line first, then bullets):\\n"
        "**[Account Name]**\\n"
        "- **Confidence Score: XX%**\\n"
        "- **Why:** [data-driven reason]\\n"
        "- **Whitespace:** [feature or use-case gap]\\n"
        "- **Approach:** [recommended engagement action]\\n\\n"
        "Format with clear markdown headers, bold account names, and bold Confidence Score labels. "
        "Be direct and field-ready — not generic.\\n\\n"
        "TERRITORY DATA:\\n" + safe
    )
    sql = (
        "SELECT SNOWFLAKE.CORTEX.COMPLETE("
        "'claude-3-5-sonnet', "
        "'" + prompt + "'"
        ") AS RESPONSE"
    )
    try:
        return conn.query(sql, ttl=0)
    except Exception:
        sql_fallback = (
            "SELECT SNOWFLAKE.CORTEX.COMPLETE("
            "'mistral-large2', "
            "'" + prompt + "'"
            ") AS RESPONSE"
        )
        return conn.query(sql_fallback, ttl=0)


with tabs[4]:
    st.subheader("AI-Powered Consumption Recommendations")
    st.caption(
        "Powered by Snowflake Cortex · Strategies and target accounts tailored to your territory data · "
        "Refresh page to regenerate with latest data"
    )

    with st.spinner("Loading territory data for analysis…"):
        df_cf_ai = q_cross_feature(theater, region_sql)
        df_feat_ai = q_features(theater, region_sql)
        df_mat_ai = q_maturity(theater, region_sql)

    context = _build_ai_context(
        theater, region_label, df_summary, df_pivoted, df_cf_ai, df_feat_ai, df_mat_ai
    )

    with st.expander("View territory context sent to AI", expanded=False):
        st.code(context, language=None)

    with st.spinner("Generating recommendations with Cortex AI — this may take 15–30 seconds…"):
        recs_df = _call_cortex_recommendations(theater, region_label, context)

    def _colorize_confidence(text):
        def _badge(m):
            score = int(m.group(1))
            if score >= 85:
                color = "#27ae60"
            elif score >= 70:
                color = "#7dc743"
            elif score >= 55:
                color = "#f1c40f"
            elif score >= 40:
                color = "#e67e22"
            else:
                color = "#e74c3c"
            return f'<span style="color:{color};font-weight:bold">Confidence Score: {score}%</span>'
        return re.sub(r'\*{0,2}Confidence Score:\s*(\d+)%\*{0,2}', _badge, text)

    if recs_df is not None and not recs_df.empty:
        response_text = str(recs_df["RESPONSE"].iloc[0])

        if "PART 1" in response_text or "## " in response_text:
            parts = response_text.split("PART 2")
            if len(parts) == 2:
                col1, col2 = st.columns(2)
                with col1:
                    with st.container(border=True):
                        st.markdown("### Territory Strategies")
                        st.markdown(_colorize_confidence(parts[0].replace("PART 1 — TOP 10 TERRITORY STRATEGIES", "").replace("**PART 1 — TOP 10 TERRITORY STRATEGIES**", "").strip()), unsafe_allow_html=True)
                with col2:
                    with st.container(border=True):
                        st.markdown("### Top 10 Target Accounts")
                        st.markdown(_colorize_confidence(("PART 2" + parts[1]).replace("PART 2 — TOP 10 TARGET ACCOUNTS", "").replace("**PART 2 — TOP 10 TARGET ACCOUNTS**", "").strip()), unsafe_allow_html=True)
            else:
                st.markdown(_colorize_confidence(response_text), unsafe_allow_html=True)
        else:
            st.markdown(_colorize_confidence(response_text), unsafe_allow_html=True)
    else:
        st.error("Could not generate recommendations. Check that Cortex is enabled in this account.")


# ─── Gong Analysis Helpers ────────────────────────────────────────────────────

def _gong_clause(theater, region):
    if region == ALL_REGIONS:
        return f"THEATER = '{theater}'"
    return (
        f"THEATER = '{theater}' AND ACCOUNT_NAME IN ("
        f"SELECT SALESFORCE_ACCOUNT_NAME FROM sales.raven.d_salesforce_account_customers "
        f"WHERE GEO='{theater}' AND SALES_AREA='{region}' "
        f"AND IS_CAPACITY_CUSTOMER=TRUE AND IS_REVENUE_ACCOUNT=TRUE)"
    )


_DE_KW_ALL = """(
    LOWER(CLEANED_DIALOGUE) LIKE '%openflow%' OR LOWER(CLEANED_DIALOGUE) LIKE '%snowpipe%'
    OR LOWER(CLEANED_DIALOGUE) LIKE '%kafka%' OR LOWER(CLEANED_DIALOGUE) LIKE '%change data capture%'
    OR LOWER(CLEANED_DIALOGUE) LIKE '%dynamic table%' OR LOWER(CLEANED_DIALOGUE) LIKE '%snowpark%'
    OR LOWER(CLEANED_DIALOGUE) LIKE '%dbt %' OR LOWER(CLEANED_DIALOGUE) LIKE '%spark connect%'
    OR LOWER(CLEANED_DIALOGUE) LIKE '%iceberg%' OR LOWER(CLEANED_DIALOGUE) LIKE '%lakehouse%'
    OR LOWER(CLEANED_DIALOGUE) LIKE '%delta lake%' OR LOWER(CLEANED_DIALOGUE) LIKE '%unity catalog%'
    OR LOWER(CLEANED_DIALOGUE) LIKE '%parquet%' OR LOWER(CLEANED_DIALOGUE) LIKE '%fivetran%'
    OR LOWER(CLEANED_DIALOGUE) LIKE '%data ingestion%' OR LOWER(CLEANED_DIALOGUE) LIKE '%tasks and streams%'
)"""


@st.cache_data(ttl=3600, show_spinner=False)
def q_gong_volume(theater, region, days=90):
    gc = _gong_clause(theater, region)
    since = f"DATEADD('day', -{days}, CURRENT_DATE())"
    return conn.query(f"""
        SELECT
            DATE_TRUNC('month', CONV_DATE)::DATE AS MONTH,
            COUNT(*) AS TOTAL_CALLS,
            SUM(CASE WHEN LOWER(CLEANED_DIALOGUE) LIKE '%openflow%'
                     OR LOWER(CLEANED_DIALOGUE) LIKE '%snowpipe%'
                     OR LOWER(CLEANED_DIALOGUE) LIKE '%kafka%'
                     OR LOWER(CLEANED_DIALOGUE) LIKE '%change data capture%'
                     OR LOWER(CLEANED_DIALOGUE) LIKE '%fivetran%'
                     OR LOWER(CLEANED_DIALOGUE) LIKE '%data ingestion%'
                     THEN 1 ELSE 0 END) AS INGESTION_CALLS,
            SUM(CASE WHEN LOWER(CLEANED_DIALOGUE) LIKE '%dynamic table%'
                     OR LOWER(CLEANED_DIALOGUE) LIKE '%snowpark%'
                     OR LOWER(CLEANED_DIALOGUE) LIKE '%dbt %'
                     OR LOWER(CLEANED_DIALOGUE) LIKE '%spark connect%'
                     OR LOWER(CLEANED_DIALOGUE) LIKE '%data transformation%'
                     THEN 1 ELSE 0 END) AS TRANSFORMATION_CALLS,
            SUM(CASE WHEN LOWER(CLEANED_DIALOGUE) LIKE '%iceberg%'
                     OR LOWER(CLEANED_DIALOGUE) LIKE '%lakehouse%'
                     OR LOWER(CLEANED_DIALOGUE) LIKE '%delta lake%'
                     OR LOWER(CLEANED_DIALOGUE) LIKE '%unity catalog%'
                     OR LOWER(CLEANED_DIALOGUE) LIKE '%parquet%'
                     THEN 1 ELSE 0 END) AS LAKEHOUSE_CALLS
        FROM SALES.ACTIVITY.GONG_ALL_CONV_COMPETITOR
        WHERE {gc} AND CONV_DATE >= {since}
          AND {_DE_KW_ALL}
        GROUP BY 1 ORDER BY 1
    """, ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_gong_freq(theater, region, days=90):
    gc = _gong_clause(theater, region)
    since = f"DATEADD('day', -{days}, CURRENT_DATE())"
    return conn.query(f"""
        WITH de_calls AS (
            SELECT ACCOUNT_NAME, CONV_DATE, CLEANED_DIALOGUE
            FROM SALES.ACTIVITY.GONG_ALL_CONV_COMPETITOR
            WHERE {gc} AND CONV_DATE >= {since} AND {_DE_KW_ALL}
        )
        SELECT keyword,
               COUNT(*) AS CALLS_MENTIONING,
               COUNT(DISTINCT ACCOUNT_NAME) AS UNIQUE_ACCOUNTS
        FROM de_calls
        CROSS JOIN (
            SELECT 'openflow' AS keyword UNION ALL SELECT 'snowpipe'
            UNION ALL SELECT 'kafka' UNION ALL SELECT 'cdc / change data capture'
            UNION ALL SELECT 'fivetran' UNION ALL SELECT 'data ingestion'
            UNION ALL SELECT 'dynamic tables' UNION ALL SELECT 'snowpark'
            UNION ALL SELECT 'dbt' UNION ALL SELECT 'spark connect'
            UNION ALL SELECT 'tasks & streams' UNION ALL SELECT 'iceberg'
            UNION ALL SELECT 'lakehouse' UNION ALL SELECT 'delta lake'
            UNION ALL SELECT 'unity catalog' UNION ALL SELECT 'parquet'
        ) kws
        WHERE
            (keyword = 'openflow' AND LOWER(CLEANED_DIALOGUE) LIKE '%openflow%')
            OR (keyword = 'snowpipe' AND LOWER(CLEANED_DIALOGUE) LIKE '%snowpipe%')
            OR (keyword = 'kafka' AND LOWER(CLEANED_DIALOGUE) LIKE '%kafka%')
            OR (keyword = 'cdc / change data capture' AND (LOWER(CLEANED_DIALOGUE) LIKE '%change data capture%' OR LOWER(CLEANED_DIALOGUE) LIKE '% cdc %'))
            OR (keyword = 'fivetran' AND LOWER(CLEANED_DIALOGUE) LIKE '%fivetran%')
            OR (keyword = 'data ingestion' AND LOWER(CLEANED_DIALOGUE) LIKE '%data ingestion%')
            OR (keyword = 'dynamic tables' AND LOWER(CLEANED_DIALOGUE) LIKE '%dynamic table%')
            OR (keyword = 'snowpark' AND LOWER(CLEANED_DIALOGUE) LIKE '%snowpark%')
            OR (keyword = 'dbt' AND LOWER(CLEANED_DIALOGUE) LIKE '%dbt %')
            OR (keyword = 'spark connect' AND LOWER(CLEANED_DIALOGUE) LIKE '%spark connect%')
            OR (keyword = 'tasks & streams' AND LOWER(CLEANED_DIALOGUE) LIKE '%tasks and streams%')
            OR (keyword = 'iceberg' AND LOWER(CLEANED_DIALOGUE) LIKE '%iceberg%')
            OR (keyword = 'lakehouse' AND LOWER(CLEANED_DIALOGUE) LIKE '%lakehouse%')
            OR (keyword = 'delta lake' AND LOWER(CLEANED_DIALOGUE) LIKE '%delta lake%')
            OR (keyword = 'unity catalog' AND LOWER(CLEANED_DIALOGUE) LIKE '%unity catalog%')
            OR (keyword = 'parquet' AND LOWER(CLEANED_DIALOGUE) LIKE '%parquet%')
        GROUP BY keyword
        ORDER BY CALLS_MENTIONING DESC
    """, ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_gong_top_accts(theater, region, days=90):
    gc = _gong_clause(theater, region)
    since = f"DATEADD('day', -{days}, CURRENT_DATE())"
    return conn.query(f"""
        SELECT ACCOUNT_NAME, COUNT(*) AS DE_CALL_COUNT
        FROM SALES.ACTIVITY.GONG_ALL_CONV_COMPETITOR
        WHERE {gc} AND CONV_DATE >= {since} AND {_DE_KW_ALL}
        GROUP BY ACCOUNT_NAME
        ORDER BY DE_CALL_COUNT DESC
        LIMIT 20
    """, ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_gong_area_ai(theater, region, area, days=90):
    gc = _gong_clause(theater, region)
    since = f"DATEADD('day', -{days}, CURRENT_DATE())"

    if area == "Ingestion":
        kw_filter = """(LOWER(CLEANED_DIALOGUE) LIKE '%openflow%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%snowpipe%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%kafka%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%change data capture%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%fivetran%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%data ingestion%')"""
        snippet_case = """CASE
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%openflow%'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('openflow' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%snowpipe%'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('snowpipe' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%kafka%'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('kafka' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%change data capture%'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('change data capture' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%fivetran%'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('fivetran' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            ELSE SUBSTRING(CLEANED_DIALOGUE, 1, 700) END"""
        topic = "Data Engineering INGESTION (Openflow, Snowpipe, Snowpipe Streaming, Kafka, CDC, Fivetran, connectors)"
    elif area == "Transformation":
        kw_filter = """(LOWER(CLEANED_DIALOGUE) LIKE '%dynamic table%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%dbt %'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%snowpark%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%spark connect%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%tasks and streams%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%data transformation%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%stored procedure%')"""
        snippet_case = """CASE
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%dynamic table%'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('dynamic table' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%dbt %'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('dbt' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%snowpark%'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('snowpark' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%spark connect%'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('spark connect' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%tasks and streams%'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('tasks and streams' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            ELSE SUBSTRING(CLEANED_DIALOGUE, 1, 700) END"""
        topic = "TRANSFORMATION (Dynamic Tables, dbt, Snowpark, Spark Connect, Tasks & Streams, stored procedures, data transformation pipelines)"
    else:
        kw_filter = """(LOWER(CLEANED_DIALOGUE) LIKE '%iceberg%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%unity catalog%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%delta lake%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%lakehouse%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%parquet%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%open table format%'
            OR LOWER(CLEANED_DIALOGUE) LIKE '%open catalog%')"""
        snippet_case = """CASE
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%iceberg%'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('iceberg' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%unity catalog%'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('unity catalog' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%delta lake%'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('delta lake' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%lakehouse%'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('lakehouse' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            WHEN LOWER(CLEANED_DIALOGUE) LIKE '%parquet%'
                THEN SUBSTRING(CLEANED_DIALOGUE, GREATEST(1, POSITION('parquet' IN LOWER(CLEANED_DIALOGUE)) - 200), 700)
            ELSE SUBSTRING(CLEANED_DIALOGUE, 1, 700) END"""
        topic = "LAKEHOUSE (Iceberg, Delta Lake, Unity Catalog, open table formats, Parquet, Open Catalog/Polaris, catalog integrations)"

    safe_gc = gc.replace("'", "''")
    prompt_prefix = (
        f"You are analyzing up to 40 real Gong call transcript snippets from Snowflake {theater} territory"
        + (f" / {region} region" if region != ALL_REGIONS else "")
        + f". Topic: {topic}. Each snippet is labeled with customer account name and date.\\n\\n"
        "## Common Themes & Topics\\nTop 5-7 recurring themes. Be specific.\\n\\n"
        "## Most Discussed Features & Products\\nWhich features come up most? Rank them with context.\\n\\n"
        "## Customer Sentiment\\nOverall sentiment. What excites customers? What frustrates them? Quote directly.\\n\\n"
        "## What Is Working (Positive Signals)\\nTalking points and features generating positive reactions.\\n\\n"
        "## What Is NOT Working (Objections & Friction)\\nObjections, gaps, complaints, competitor advantages.\\n\\n"
        "## Competitor Mentions\\nWhich competitors and in what context?\\n\\n"
        "Be specific. Quote customers. Use account names. This is for the Snowflake DE field team.\\n"
        "Write dollar amounts as plain text without dollar signs.\\n\\nDATA:\\n"
    )
    safe_prompt = prompt_prefix.replace("'", "''")

    sql = f"""
        WITH sampled AS (
            SELECT ACCOUNT_NAME, CONV_DATE,
                {snippet_case} AS snippet,
                ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NAME ORDER BY CONV_DATE DESC) AS rn
            FROM SALES.ACTIVITY.GONG_ALL_CONV_COMPETITOR
            WHERE {gc} AND CONV_DATE >= {since} AND {kw_filter}
        ),
        diverse_sample AS (
            SELECT ACCOUNT_NAME, CONV_DATE, snippet
            FROM sampled WHERE rn = 1
            ORDER BY CONV_DATE DESC LIMIT 40
        ),
        combined AS (
            SELECT LISTAGG('CALL [' || ACCOUNT_NAME || ' | ' || CONV_DATE || ']: ' || snippet, '\\n---\\n')
                   WITHIN GROUP (ORDER BY CONV_DATE DESC) AS all_text
            FROM diverse_sample
        )
        SELECT LEFT(SNOWFLAKE.CORTEX.COMPLETE('llama3.3-70b',
            '{safe_prompt}' || all_text
        ), 8000) AS ANALYSIS FROM combined
    """
    try:
        return conn.query(sql, ttl=0)
    except Exception:
        sql2 = sql.replace("llama3.3-70b", "llama3.1-70b")
        try:
            return conn.query(sql2, ttl=0)
        except Exception:
            return None


@st.cache_data(ttl=3600, show_spinner=False)
def q_gong_cross_summary(theater, region, days=90):
    gc = _gong_clause(theater, region)
    since = f"DATEADD('day', -{days}, CURRENT_DATE())"
    safe_gc = gc.replace("'", "''")
    territory_label = f"{theater}" + (f" / {region}" if region != ALL_REGIONS else " (all regions)")
    prompt_prefix = (
        f"You are analyzing 50 real Gong call transcript snippets from Snowflake {territory_label} territory. "
        "These span all Data Engineering topics: Ingestion (Openflow, Snowpipe, Kafka, CDC, Fivetran), "
        "Transformation (Dynamic Tables, dbt, Snowpark), and Lakehouse (Iceberg, open table formats). "
        "Each snippet is labeled [area] and [account | date].\\n\\n"
        "## Overall Customer Sentiment Toward Snowflake DE\\n"
        "How are customers feeling overall? On what specifically? Quote directly.\\n\\n"
        "## Top 5 Cross-Cutting Themes\\nWhat themes span all three DE areas?\\n\\n"
        "## Biggest Opportunities (What We Should Double Down On)\\nWhere is customer excitement highest?\\n\\n"
        "## Biggest Friction Points (What Is Slowing Us Down)\\n"
        "What objections and competitor narratives consistently appear?\\n\\n"
        "## Competitive Landscape\\nWhich competitors appear most and what is the narrative?\\n\\n"
        "## Recommended Focus Areas for the Field Team\\n"
        "What should AEs and SEs prioritize in DE conversations?\\n\\n"
        "Be direct, specific, actionable. Quote customers. Name accounts.\\n"
        "Write dollar amounts as plain text without dollar signs.\\n\\nDATA:\\n"
    )
    safe_prompt = prompt_prefix.replace("'", "''")

    sql = f"""
        WITH sampled AS (
            SELECT ACCOUNT_NAME, CONV_DATE,
                CASE
                    WHEN LOWER(CLEANED_DIALOGUE) LIKE '%openflow%' THEN 'Ingestion'
                    WHEN LOWER(CLEANED_DIALOGUE) LIKE '%snowpipe%' THEN 'Ingestion'
                    WHEN LOWER(CLEANED_DIALOGUE) LIKE '%kafka%' THEN 'Ingestion'
                    WHEN LOWER(CLEANED_DIALOGUE) LIKE '%dynamic table%' THEN 'Transformation'
                    WHEN LOWER(CLEANED_DIALOGUE) LIKE '%dbt %' THEN 'Transformation'
                    WHEN LOWER(CLEANED_DIALOGUE) LIKE '%snowpark%' THEN 'Transformation'
                    WHEN LOWER(CLEANED_DIALOGUE) LIKE '%iceberg%' THEN 'Lakehouse'
                    ELSE 'DE-General'
                END AS area,
                SUBSTRING(CLEANED_DIALOGUE, 1, 500) AS snippet,
                ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NAME ORDER BY CONV_DATE DESC) AS rn
            FROM SALES.ACTIVITY.GONG_ALL_CONV_COMPETITOR
            WHERE {gc} AND CONV_DATE >= {since}
              AND ({_DE_KW_ALL})
        ),
        diverse_sample AS (
            SELECT ACCOUNT_NAME, CONV_DATE, area, snippet
            FROM sampled WHERE rn = 1
            ORDER BY RANDOM() LIMIT 50
        ),
        combined AS (
            SELECT LISTAGG('[' || area || '] CALL [' || ACCOUNT_NAME || ' | ' || CONV_DATE || ']: ' || snippet, '\\n---\\n')
                   WITHIN GROUP (ORDER BY area, CONV_DATE DESC) AS all_text
            FROM diverse_sample
        )
        SELECT LEFT(SNOWFLAKE.CORTEX.COMPLETE('llama3.3-70b',
            '{safe_prompt}' || all_text
        ), 8000) AS ANALYSIS FROM combined
    """
    try:
        return conn.query(sql, ttl=0)
    except Exception:
        sql2 = sql.replace("llama3.3-70b", "llama3.1-70b")
        try:
            return conn.query(sql2, ttl=0)
        except Exception:
            return None


# ─── Use Case Win/Loss Helpers ────────────────────────────────────────────────

_DE_FEATURE_FILTER = "PRIORITIZED_FEATURE ILIKE ANY ('%DE -%', '%Snowpark%', '%DE-%')"


@st.cache_data(ttl=3600, show_spinner=False)
def q_uc_summary(theater, region, days=365):
    region_clause = "" if region == ALL_REGIONS else f"AND REGION = '{region}'"
    since = f"DATEADD('day', -{days}, CURRENT_DATE())"
    return conn.query(f"""
        SELECT
            CASE
                WHEN PRIORITIZED_FEATURE ILIKE '%openflow%' THEN 'DE - Openflow'
                WHEN PRIORITIZED_FEATURE ILIKE '%iceberg%' THEN 'DE - Iceberg'
                WHEN PRIORITIZED_FEATURE ILIKE '%snowpipe%' THEN 'DE - Snowpipe Streaming'
                WHEN PRIORITIZED_FEATURE ILIKE '%dynamic table%' THEN 'DE - Dynamic Tables'
                WHEN PRIORITIZED_FEATURE ILIKE '%snowpark%' THEN 'DE - Snowpark'
                WHEN PRIORITIZED_FEATURE ILIKE '%dbt%' THEN 'DE - dbt Projects'
                ELSE 'DE - Other'
            END AS FEATURE_GROUP,
            SUM(CASE WHEN IS_WON OR IS_WENT_LIVE THEN 1 ELSE 0 END) AS WINS_GO_LIVES,
            SUM(CASE WHEN IS_LOST THEN 1 ELSE 0 END) AS LOSSES,
            COUNT(*) AS TOTAL,
            ROUND(100.0 * SUM(CASE WHEN IS_WON OR IS_WENT_LIVE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 0)::INT AS WIN_RATE_PCT
        FROM sales.raven.sda_use_case
        WHERE THEATER = '{theater}' {region_clause}
          AND {_DE_FEATURE_FILTER}
          AND CREATED_DATE >= {since}
        GROUP BY 1 ORDER BY WIN_RATE_PCT DESC
    """, ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_uc_wins(theater, region, days=365):
    region_clause = "" if region == ALL_REGIONS else f"AND REGION = '{region}'"
    since = f"DATEADD('day', -{days}, CURRENT_DATE())"
    return conn.query(f"""
        SELECT
            ACCOUNT_NAME,
            PRIORITIZED_FEATURE,
            REGION,
            USE_CASE_NAME,
            USE_CASE_STAGE,
            TECHNICAL_WIN_DATE,
            GO_LIVE_DATE,
            MEDDPICC_OVERALL_SCORE,
            USE_CASE_DESCRIPTION,
            USE_CASE_COMMENTS,
            COMPETITORS,
            INCUMBENT_VENDOR
        FROM sales.raven.sda_use_case
        WHERE THEATER = '{theater}' {region_clause}
          AND {_DE_FEATURE_FILTER}
          AND (IS_WON = TRUE OR IS_WENT_LIVE = TRUE)
          AND CREATED_DATE >= {since}
        ORDER BY COALESCE(GO_LIVE_DATE, TECHNICAL_WIN_DATE) DESC
        LIMIT 50
    """, ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_uc_losses(theater, region, days=365):
    region_clause = "" if region == ALL_REGIONS else f"AND REGION = '{region}'"
    since = f"DATEADD('day', -{days}, CURRENT_DATE())"
    return conn.query(f"""
        SELECT
            ACCOUNT_NAME,
            PRIORITIZED_FEATURE,
            REGION,
            USE_CASE_NAME,
            USE_CASE_STAGE,
            LOST_REASON,
            VENDOR_OF_CHOICE,
            COMPETITORS,
            USE_CASE_DESCRIPTION,
            USE_CASE_COMMENTS,
            LAST_MODIFIED_DATE
        FROM sales.raven.sda_use_case
        WHERE THEATER = '{theater}' {region_clause}
          AND {_DE_FEATURE_FILTER}
          AND IS_LOST = TRUE
          AND CREATED_DATE >= {since}
        ORDER BY LAST_MODIFIED_DATE DESC
        LIMIT 50
    """, ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_uc_recent(theater, region, date_from, date_to):
    region_clause = "" if region == ALL_REGIONS else f"AND u.REGION = '{region}'"
    return conn.query(f"""
        SELECT
            u.USE_CASE_NUMBER,
            u.USE_CASE_ID,
            u.ACCOUNT_NAME,
            CASE WHEN u.IS_WON OR u.IS_WENT_LIVE THEN 'Win / Go-Live' ELSE 'Loss' END AS OUTCOME,
            u.PRIORITIZED_FEATURE,
            u.REGION,
            u.USE_CASE_STAGE,
            u.DECISION_DATE,
            COALESCE(u.GO_LIVE_DATE, u.TECHNICAL_WIN_DATE) AS WIN_DATE,
            u.LOST_REASON,
            u.VENDOR_OF_CHOICE,
            u.COMPETITORS,
            u.INCUMBENT_VENDOR,
            ROUND(u.MEDDPICC_OVERALL_SCORE, 0)::INT AS MEDDPICC_SCORE,
            LEFT(ucl.SPECIALIST_COMMENTS, 500) AS WIN_SUMMARY,
            LEFT(ucl.LOSS_DESCRIPTION, 500) AS LOSS_DESCRIPTION_KEY_INSIGHTS
        FROM sales.raven.sda_use_case u
        LEFT JOIN (
            SELECT USE_CASE_ID, LOSS_DESCRIPTION, SPECIALIST_COMMENTS
            FROM sales.sales_engineering.use_case_level_data
            WHERE DS = (SELECT MAX(DS) FROM sales.sales_engineering.use_case_level_data)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY USE_CASE_ID ORDER BY DS DESC) = 1
        ) ucl ON ucl.USE_CASE_ID = u.USE_CASE_ID
        WHERE u.THEATER = '{theater}' {region_clause}
          AND {_DE_FEATURE_FILTER}
          AND (u.IS_WON = TRUE OR u.IS_WENT_LIVE = TRUE OR u.IS_LOST = TRUE)
          AND u.DECISION_DATE BETWEEN '{date_from}' AND '{date_to}'
        ORDER BY u.DECISION_DATE DESC
        LIMIT 200
    """, ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_uc_ai(theater, region, wins_text, losses_text):
    territory_label = f"{theater}" + (f" / {region}" if region != ALL_REGIONS else " (all regions)")
    safe_wins = wins_text.replace("'", "''")
    safe_losses = losses_text.replace("'", "''")
    prompt = (
        f"You are a senior Snowflake Data Engineering field expert. "
        f"Analyze the following Data Engineering use case win and loss data for {territory_label}.\\n\\n"
        "**PART 1 — WHAT IS WORKING (Wins & Go-Lives)**\\n"
        "Based on the wins below, identify the top 3-5 patterns leading to wins and go-lives. "
        "What are customers excited about? What use-case patterns win? Which competitors were displaced?\\n\\n"
        "**PART 2 — WHAT IS NOT WORKING (Losses)**\\n"
        "Based on the losses below, identify the top 3-5 patterns in lost use cases. "
        "What are the common lost reasons? Which competitors are winning and why? "
        "What objections keep appearing?\\n\\n"
        "**PART 3 — FIELD RECOMMENDATIONS**\\n"
        "Give 3-5 specific, actionable recommendations for the field team based on this win/loss data. "
        "What should they do more of? What risks should they watch for?\\n\\n"
        "Be direct and specific. Reference account names and use case details where helpful. "
        "Write dollar amounts as plain text without dollar signs.\\n\\n"
        "WINS & GO-LIVES DATA:\\n"
    )
    sql = (
        "SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', '"
        + prompt + safe_wins
        + "\\n\\nLOSSES DATA:\\n"
        + safe_losses
        + "') AS RESPONSE"
    )
    try:
        return conn.query(sql, ttl=0)
    except Exception:
        sql2 = sql.replace("'claude-3-5-sonnet'", "'mistral-large2'")
        try:
            return conn.query(sql2, ttl=0)
        except Exception:
            return None


@st.cache_data(ttl=3600, show_spinner=False)
def q_uc_themes(theater, region, recent_text):
    territory_label = f"{theater}" + (f" / {region}" if region != ALL_REGIONS else " (all regions)")
    safe = recent_text.replace("\\", "\\\\").replace("'", "''")
    prompt = (
        f"You are a senior Snowflake Data Engineering field expert. "
        f"Analyze the following recent DE use case wins and losses for {territory_label}. "
        "Identify the top recurring THEMES across wins and losses separately.\\n\\n"
        "**WIN THEMES** — patterns across wins and go-lives\\n"
        "Identify 3-5 themes. For each: a short bold theme name, description of the pattern, "
        "and specific evidence from the data (account names, features, business problems, "
        "architectural patterns, displaced competitors).\\n\\n"
        "**LOSS THEMES** — patterns across lost deals\\n"
        "Identify 3-5 themes. For each: a short bold theme name, description of the pattern, "
        "and specific evidence (loss reasons, vendors that won, features that were weak, "
        "common objections or deal blockers).\\n\\n"
        "Be specific and evidence-based. Reference actual account names and details from the data.\\n\\n"
        "USE CASE DATA:\\n"
    )
    sql = (
        "SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', '"
        + prompt + safe
        + "') AS RESPONSE"
    )
    try:
        return conn.query(sql, ttl=0)
    except Exception:
        try:
            sql2 = sql.replace("'claude-3-5-sonnet'", "'mistral-large2'")
            return conn.query(sql2, ttl=0)
        except Exception:
            return None


def _uc_recent_to_themes_text(df, max_rows=50):
    lines = []
    for _, r in df.head(max_rows).iterrows():
        outcome = r.get("OUTCOME", "?")
        line = f"[{outcome}] Account: {r.get('ACCOUNT_NAME','?')} | Feature: {r.get('PRIORITIZED_FEATURE','?')} | Region: {r.get('REGION','?')}"
        if r.get("LOST_REASON"):
            line += f" | Lost Reason: {r['LOST_REASON']}"
        if r.get("VENDOR_OF_CHOICE"):
            line += f" | Won By: {r['VENDOR_OF_CHOICE']}"
        if r.get("COMPETITORS"):
            line += f" | Competitors: {r['COMPETITORS']}"
        if r.get("INCUMBENT_VENDOR"):
            line += f" | Incumbent: {r['INCUMBENT_VENDOR']}"
        if r.get("MEDDPICC_SCORE"):
            line += f" | MEDDPICC: {r['MEDDPICC_SCORE']}"
        summary = r.get("WIN_SUMMARY") or r.get("LOSS_DESCRIPTION_KEY_INSIGHTS")
        if summary and str(summary).strip():
            line += f" | Summary: {str(summary)[:150]}"
        lines.append(line)
    return "\n".join(lines)


def _uc_rows_to_text(df, kind, max_rows=30):
    lines = [f"=== {kind} (up to {max_rows} records) ==="]
    for _, r in df.head(max_rows).iterrows():
        line = f"Account: {r.get('ACCOUNT_NAME','?')} | Feature: {r.get('PRIORITIZED_FEATURE','?')} | Region: {r.get('REGION','?')}"
        if r.get("USE_CASE_NAME"):
            line += f" | Use Case: {r['USE_CASE_NAME']}"
        if kind == "WINS":
            if pd.notna(r.get("GO_LIVE_DATE")):
                line += f" | Go-Live: {r['GO_LIVE_DATE']}"
            elif pd.notna(r.get("TECHNICAL_WIN_DATE")):
                line += f" | Tech Win: {r['TECHNICAL_WIN_DATE']}"
            if r.get("COMPETITORS"):
                line += f" | Displaced: {r['COMPETITORS']}"
            if r.get("INCUMBENT_VENDOR"):
                line += f" | Incumbent: {r['INCUMBENT_VENDOR']}"
        else:
            if r.get("LOST_REASON"):
                line += f" | Lost Reason: {r['LOST_REASON']}"
            if r.get("VENDOR_OF_CHOICE"):
                line += f" | Won By: {r['VENDOR_OF_CHOICE']}"
            if r.get("COMPETITORS"):
                line += f" | Competitors: {r['COMPETITORS']}"
        if r.get("USE_CASE_DESCRIPTION"):
            line += f" | Desc: {str(r['USE_CASE_DESCRIPTION'])[:200]}"
        lines.append(line)
    return "\n".join(lines)


# ─── Product Gaps & Business Problems query helpers ──────────────────────────

def _pgap_acct_filter(theater, region):
    if region == ALL_REGIONS:
        return f"GEO = '{theater}'"
    return f"GEO = '{theater}' AND SALES_AREA = '{region}'"


@st.cache_data(ttl=3600, show_spinner=False)
def q_pgap_summary(theater, region):
    acct_f = _pgap_acct_filter(theater, region)
    return conn.query(f"""
        SELECT
            p.JIRA_ISSUE_KEY AS PGAP_ID,
            p.JIRA_ISSUE_LINK AS PGAP_URL,
            p.NAME AS PGAP_TITLE,
            p.PRODUCT_LINE_C AS PRODUCT_CATEGORY,
            p.PRODUCT_AREA_C AS PRODUCT_USE_CASE,
            p.FEATURE_GROUP_C AS FEATURE_GROUP,
            COALESCE(j.STATUS, p.STATUS_C) AS STATUS,
            ARRAY_TO_STRING(ARRAY_AGG(DISTINCT ac.SALESFORCE_ACCOUNT_NAME), ', ') AS CUSTOMERS_IMPACTED,
            COUNT(DISTINCT ag.ACCOUNT_ID) AS ACCOUNT_COUNT,
            MAX(p.ACCOUNT_GAP_SUM_C) AS TOTAL_ACV
        FROM sales.dev.ACCOUNT_GAPS ag
        JOIN sales.dev.PRODUCT_GAPS p
            ON p.ID = ag.PRODUCT_GAP_ID
            AND p.DS = (SELECT MAX(DS) FROM sales.dev.PRODUCT_GAPS)
        LEFT JOIN sales.se_reporting.DIM_JIRA_PGAP j
            ON j.VIVUN_PRODUCT_GAP_ID = p.ID
        JOIN (
            SELECT DISTINCT SALESFORCE_ACCOUNT_ID, SALESFORCE_ACCOUNT_NAME
            FROM sales.raven.d_salesforce_account_customers
            WHERE {acct_f}
        ) ac ON ac.SALESFORCE_ACCOUNT_ID = ag.ACCOUNT_ID
        WHERE ag.DS = (SELECT MAX(DS) FROM sales.dev.ACCOUNT_GAPS)
          AND p.JIRA_ISSUE_KEY LIKE 'PGAP-%'
          AND UPPER(COALESCE(j.STATUS, p.STATUS_C)) NOT LIKE 'DONE%'
          AND p.PRODUCT_LINE_C = 'Data Engineering'
        GROUP BY 1,2,3,4,5,6,7
    """, ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_pgap_detail(theater, region):
    acct_f = _pgap_acct_filter(theater, region)
    return conn.query(f"""
        SELECT
            p.JIRA_ISSUE_KEY AS PGAP_ID,
            p.JIRA_ISSUE_LINK AS PGAP_URL,
            p.NAME AS PGAP_TITLE,
            p.PRODUCT_AREA_C AS PRODUCT_AREA,
            ac.SALESFORCE_ACCOUNT_NAME AS ACCOUNT_NAME,
            ag.USE_CASE_NAME,
            ag.USE_CASE_C AS USE_CASE_SFDC_ID,
            ag.ACCOUNT_GAP_AMOUNT_C AS EACV,
            ag.ACCOUNT_GAP_CREATED_DATE::DATE AS CREATED_DATE,
            ag.TYPE_C AS GAP_TYPE,
            p.STATUS_C AS STATUS
        FROM sales.dev.ACCOUNT_GAPS ag
        JOIN sales.dev.PRODUCT_GAPS p
            ON p.ID = ag.PRODUCT_GAP_ID
            AND p.DS = (SELECT MAX(DS) FROM sales.dev.PRODUCT_GAPS)
        JOIN (
            SELECT DISTINCT SALESFORCE_ACCOUNT_ID, SALESFORCE_ACCOUNT_NAME
            FROM sales.raven.d_salesforce_account_customers
            WHERE {acct_f}
        ) ac ON ac.SALESFORCE_ACCOUNT_ID = ag.ACCOUNT_ID
        WHERE ag.DS = (SELECT MAX(DS) FROM sales.dev.ACCOUNT_GAPS)
          AND p.JIRA_ISSUE_KEY LIKE 'PGAP-%'
          AND UPPER(p.STATUS_C) NOT LIKE 'DONE%'
        ORDER BY ag.ACCOUNT_GAP_AMOUNT_C DESC NULLS LAST
        LIMIT 2000
    """, ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_biz_problems(theater, region):
    region_clause = "" if region == ALL_REGIONS else f"AND REGION_NAME = '{region}'"
    return conn.query(f"""
        SELECT
            INDUSTRY_USE_CASE AS BUSINESS_PROBLEM,
            COUNT(DISTINCT USE_CASE_ID) AS UC_COUNT,
            COUNT(DISTINCT ACCOUNT_ID) AS ACCT_COUNT,
            SUM(USE_CASE_EACV) AS TOTAL_EACV
        FROM sales.sales_engineering.use_case_level_data
        WHERE DS = (SELECT MAX(DS) FROM sales.sales_engineering.use_case_level_data)
          AND THEATER_NAME = '{theater}' {region_clause}
          AND INDUSTRY_USE_CASE IS NOT NULL
          AND INDUSTRY_USE_CASE NOT IN ('Undefined / Not a Business Function', '')
          AND (
              ARRAY_CONTAINS('DE: Ingestion'::VARIANT, TECHNICAL_USE_CASE_ARRAY)
              OR ARRAY_CONTAINS('DE: Transformation'::VARIANT, TECHNICAL_USE_CASE_ARRAY)
              OR ARRAY_CONTAINS('DE: Interoperable Storage'::VARIANT, TECHNICAL_USE_CASE_ARRAY)
              OR ARRAY_CONTAINS('Analytics: Lakehouse Analytics'::VARIANT, TECHNICAL_USE_CASE_ARRAY)
          )
          AND IS_LOST = FALSE
        GROUP BY 1
        ORDER BY TOTAL_EACV DESC NULLS LAST
    """, ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_biz_problems_detail(theater, region):
    region_clause = "" if region == ALL_REGIONS else f"AND REGION_NAME = '{region}'"
    return conn.query(f"""
        SELECT
            INDUSTRY_USE_CASE AS BUSINESS_PROBLEM,
            ACCOUNT_INDUSTRY,
            USE_CASE_ID,
            USE_CASE_NAME,
            ACCOUNT_NAME,
            USE_CASE_EACV AS EACV,
            USE_CASE_STAGE,
            TECHNICAL_USE_CASE,
            DECISION_DATE
        FROM sales.sales_engineering.use_case_level_data
        WHERE DS = (SELECT MAX(DS) FROM sales.sales_engineering.use_case_level_data)
          AND THEATER_NAME = '{theater}' {region_clause}
          AND INDUSTRY_USE_CASE IS NOT NULL
          AND INDUSTRY_USE_CASE NOT IN ('Undefined / Not a Business Function', '')
          AND (
              ARRAY_CONTAINS('DE: Ingestion'::VARIANT, TECHNICAL_USE_CASE_ARRAY)
              OR ARRAY_CONTAINS('DE: Transformation'::VARIANT, TECHNICAL_USE_CASE_ARRAY)
              OR ARRAY_CONTAINS('DE: Interoperable Storage'::VARIANT, TECHNICAL_USE_CASE_ARRAY)
              OR ARRAY_CONTAINS('Analytics: Lakehouse Analytics'::VARIANT, TECHNICAL_USE_CASE_ARRAY)
          )
          AND IS_LOST = FALSE
        ORDER BY USE_CASE_EACV DESC NULLS LAST
        LIMIT 1000
    """, ttl=0)


@st.cache_data(ttl=3600, show_spinner=False)
def q_biz_problems_ai(theater, region, summary_text):
    territory_label = f"{theater}" + (f" / {region}" if region != ALL_REGIONS else " (all regions)")
    safe = summary_text.replace("'", "''")
    prompt = (
        f"You are a senior Snowflake Data Engineering field expert. "
        f"Based on the use case data below for {territory_label}, identify the top business problem themes. "
        "Group them by industry-specific scenario (e.g. Clickstream Analytics, EDW Modernization, IoT Streaming, "
        "Data Lake Consolidation, Real-Time Ingestion, Regulatory Reporting, etc.). "
        "For each theme provide: (1) a short name, (2) which industries it appears in, "
        "(3) total EACV opportunity, (4) 2-3 sentences on the business problem customers are solving, "
        "(5) the relevant Snowflake DE/Lakehouse capability that addresses it.\\n"
        "Focus on industry-specific problems, not generic categories like Finance or Operations. "
        "Format as markdown with headers for each theme. Limit to top 8 themes.\\n\\n"
        "USE CASE DATA:\\n" + safe
    )
    sql = (
        "SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', '"
        + prompt
        + "') AS RESPONSE"
    )
    try:
        return conn.query(sql, ttl=0)
    except Exception:
        sql2 = sql.replace("'claude-3-5-sonnet'", "'mistral-large2'")
        try:
            return conn.query(sql2, ttl=0)
        except Exception:
            return None


# ─── Tab 9: Gong Insights ────────────────────────────────────────────────────

with tabs[5]:
    st.subheader("Gong Call Intelligence — DE Features")
    st.caption(
        f"Last 90 days of Gong calls · {theater} · {region_label} · "
        "Source: SALES.ACTIVITY.GONG_ALL_CONV_COMPETITOR · All customer calls (no team filter)"
    )

    with st.spinner("Loading Gong call volume…"):
        df_gong_vol = q_gong_volume(theater, region)
        df_gong_freq = q_gong_freq(theater, region)
        df_gong_top = q_gong_top_accts(theater, region)

    if df_gong_vol is None or df_gong_vol.empty:
        st.info("No DE-related Gong calls found for this territory in the last 90 days.")
    else:
        total_calls = int(df_gong_vol["TOTAL_CALLS"].sum()) if "TOTAL_CALLS" in df_gong_vol.columns else 0
        ing_calls = int(df_gong_vol["INGESTION_CALLS"].sum()) if "INGESTION_CALLS" in df_gong_vol.columns else 0
        xfm_calls = int(df_gong_vol["TRANSFORMATION_CALLS"].sum()) if "TRANSFORMATION_CALLS" in df_gong_vol.columns else 0
        lkh_calls = int(df_gong_vol["LAKEHOUSE_CALLS"].sum()) if "LAKEHOUSE_CALLS" in df_gong_vol.columns else 0

        m1c, m2c, m3c, m4c = st.columns(4)
        m1c.metric("Total DE Calls", f"{total_calls:,}")
        m2c.metric("Ingestion Calls", f"{ing_calls:,}")
        m3c.metric("Transformation Calls", f"{xfm_calls:,}")
        m4c.metric("Lakehouse Calls", f"{lkh_calls:,}")

        st.divider()

        col_freq, col_top = st.columns([3, 2])
        with col_freq:
            st.markdown("**DE Keyword Frequency** (last 90 days)")
            if df_gong_freq is not None and not df_gong_freq.empty:
                st.dataframe(
                    df_gong_freq.rename(columns={"keyword": "Keyword", "CALLS_MENTIONING": "Calls Mentioning", "UNIQUE_ACCOUNTS": "Unique Accounts"}),
                    use_container_width=True,
                    hide_index=True,
                    height=380,
                )
            else:
                st.info("No keyword data available.")

        with col_top:
            st.markdown("**Top Accounts by DE Call Volume**")
            if df_gong_top is not None and not df_gong_top.empty:
                st.dataframe(
                    df_gong_top.rename(columns={"ACCOUNT_NAME": "Account", "DE_CALL_COUNT": "DE Calls"}),
                    use_container_width=True,
                    hide_index=True,
                    height=380,
                )
            else:
                st.info("No account data available.")

        st.divider()
        st.markdown("### AI Analysis")
        st.caption("Powered by Cortex llama3.3-70b · Smart sampling: 50 accounts, 500-char snippets, all DE areas")

        if st.button("Generate AI Analysis", key="gong_ai_btn", type="primary"):
            with st.spinner("Analyzing Gong transcripts across all DE areas — this may take 30–60 seconds…"):
                res_sum = q_gong_cross_summary(theater, region)
            if res_sum is not None and not res_sum.empty:
                with st.container(border=True):
                    st.markdown(str(res_sum["ANALYSIS"].iloc[0]))
            else:
                st.warning("Analysis unavailable — no matching DE calls found or Cortex model error.")
        else:
            st.info("Click **Generate AI Analysis** to run Cortex AI analysis on Gong transcripts across Ingestion, Transformation, and Lakehouse. This takes 30–60 seconds.")


# ─── Tab 10: Use Case Win/Loss ────────────────────────────────────────────────

with tabs[6]:
    st.subheader("DE Use Case Win/Loss Analysis")
    st.caption(
        f"Last 12 months · {theater} · {region_label} · "
        "Source: SALES.RAVEN.SDA_USE_CASE · DE-prioritized features only"
    )

    with st.spinner("Loading use case data…"):
        df_uc_sum = q_uc_summary(theater, region)
        df_uc_wins = q_uc_wins(theater, region)
        df_uc_losses = q_uc_losses(theater, region)

    if df_uc_sum is None or df_uc_sum.empty:
        st.info("No DE use case data found for this territory in the last 12 months.")
    else:
        total_w = int(df_uc_sum["WINS_GO_LIVES"].sum()) if "WINS_GO_LIVES" in df_uc_sum.columns else 0
        total_l = int(df_uc_sum["LOSSES"].sum()) if "LOSSES" in df_uc_sum.columns else 0
        total_t = int(df_uc_sum["TOTAL"].sum()) if "TOTAL" in df_uc_sum.columns else 0
        avg_wr = round(100.0 * total_w / total_t, 1) if total_t > 0 else 0.0

        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("Total Use Cases", f"{total_t:,}")
        mc2.metric("Wins + Go-Lives", f"{total_w:,}")
        mc3.metric("Losses", f"{total_l:,}")
        mc4.metric("Win Rate", f"{avg_wr:.1f}%")

        st.divider()

        col_sum, col_bar = st.columns([3, 2])
        with col_sum:
            st.markdown("**Win/Loss by Feature Group**")
            disp_sum = df_uc_sum.rename(columns={
                "FEATURE_GROUP": "Feature", "WINS_GO_LIVES": "Wins / Go-Lives",
                "LOSSES": "Losses", "TOTAL": "Total", "WIN_RATE_PCT": "Win Rate %"
            })
            st.dataframe(disp_sum, use_container_width=True, hide_index=True)

        with col_bar:
            st.markdown("**Win Rate by Feature**")
            if not df_uc_sum.empty and "WIN_RATE_PCT" in df_uc_sum.columns:
                import altair as alt
                bar_sorted = df_uc_sum[["FEATURE_GROUP", "WIN_RATE_PCT"]].sort_values("WIN_RATE_PCT", ascending=False)
                base = alt.Chart(bar_sorted).encode(
                    x=alt.X("FEATURE_GROUP:N", sort=list(bar_sorted["FEATURE_GROUP"]), title=None, axis=alt.Axis(labelAngle=-30)),
                    y=alt.Y("WIN_RATE_PCT:Q", scale=alt.Scale(domain=[0, 100]), title="Win Rate %"),
                )
                bars = base.mark_bar()
                labels = base.mark_text(dy=-6, fontSize=11, fontWeight="bold", color="#4C9BE8").encode(
                    text=alt.Text("WIN_RATE_PCT:Q", format="d")
                )
                chart = (bars + labels).properties(height=260)
                st.altair_chart(chart, use_container_width=True)

        st.divider()

        st.markdown(f"### Recent Wins and Losses  <small style='font-weight:normal;color:gray;'>Decision Date: {uc_date_from.strftime('%b %d, %Y')} – {uc_date_to.strftime('%b %d, %Y')}</small>", unsafe_allow_html=True)
        with st.spinner("Loading recent wins and losses…"):
            df_uc_recent = q_uc_recent(theater, region, uc_date_from, uc_date_to)

        if df_uc_recent is not None and not df_uc_recent.empty:
            SFDC_UC = "https://snowforce.lightning.force.com/lightning/r/Use_Case__c/{}/view"

            def _prep_uc(df):
                d = df.copy()
                d["UC_LINK"] = d.apply(
                    lambda r: SFDC_UC.format(r["USE_CASE_ID"]) + "#" + str(r["USE_CASE_NUMBER"])
                    if pd.notna(r.get("USE_CASE_ID")) else "", axis=1
                )
                return d

            df_wins_rec = _prep_uc(df_uc_recent[df_uc_recent["OUTCOME"] == "Win / Go-Live"])
            df_loss_rec = _prep_uc(df_uc_recent[df_uc_recent["OUTCOME"] != "Win / Go-Live"])

            tab_wins_r, tab_loss_r = st.tabs([f"✅ Wins & Go-Lives ({len(df_wins_rec)})", f"❌ Losses ({len(df_loss_rec)})"])

            with tab_wins_r:
                if not df_wins_rec.empty:
                    disp = df_wins_rec[[
                        "UC_LINK", "ACCOUNT_NAME", "PRIORITIZED_FEATURE", "REGION",
                        "DECISION_DATE", "WIN_DATE", "MEDDPICC_SCORE",
                        "WIN_SUMMARY", "COMPETITORS", "INCUMBENT_VENDOR"
                    ]].rename(columns={
                        "UC_LINK": "Use Case #", "ACCOUNT_NAME": "Account",
                        "PRIORITIZED_FEATURE": "Feature", "REGION": "Region",
                        "DECISION_DATE": "Decision Date", "WIN_DATE": "Win / Go-Live Date",
                        "MEDDPICC_SCORE": "MEDDPICC", "WIN_SUMMARY": "Win Summary",
                        "COMPETITORS": "Displaced", "INCUMBENT_VENDOR": "Incumbent"
                    })
                    st.dataframe(
                        disp, use_container_width=True, hide_index=True, height=380,
                        column_config={"Use Case #": st.column_config.LinkColumn("Use Case #", display_text=r"#(.*)")}
                    )
                else:
                    st.info("No wins or go-lives in this date range.")

            with tab_loss_r:
                if not df_loss_rec.empty:
                    disp = df_loss_rec[[
                        "UC_LINK", "ACCOUNT_NAME", "PRIORITIZED_FEATURE", "REGION",
                        "DECISION_DATE", "LOST_REASON", "LOSS_DESCRIPTION_KEY_INSIGHTS",
                        "VENDOR_OF_CHOICE", "COMPETITORS", "MEDDPICC_SCORE"
                    ]].rename(columns={
                        "UC_LINK": "Use Case #", "ACCOUNT_NAME": "Account",
                        "PRIORITIZED_FEATURE": "Feature", "REGION": "Region",
                        "DECISION_DATE": "Decision Date", "LOST_REASON": "Loss Reason",
                        "LOSS_DESCRIPTION_KEY_INSIGHTS": "Loss Description & Key Insights",
                        "VENDOR_OF_CHOICE": "Vendor of Choice",
                        "COMPETITORS": "Competitors", "MEDDPICC_SCORE": "MEDDPICC"
                    })
                    st.dataframe(
                        disp, use_container_width=True, hide_index=True, height=380,
                        column_config={"Use Case #": st.column_config.LinkColumn("Use Case #", display_text=r"#(.*)")}
                    )
                else:
                    st.info("No losses in this date range.")
        else:
            st.info(f"No wins or losses with a Decision Date between {uc_date_from.strftime('%b %d, %Y')} and {uc_date_to.strftime('%b %d, %Y')}.")

        st.divider()
        st.markdown("### Win/Loss Themes")
        st.caption("Identify recurring patterns across wins and losses — architecture, business problems, features, competitors")
        if st.button("Identify Themes", key="uc_themes_btn", type="primary"):
            if df_uc_recent is not None and not df_uc_recent.empty:
                themes_text = _uc_recent_to_themes_text(df_uc_recent)
                with st.spinner("Identifying themes — this may take 15–30 seconds…"):
                    themes_df = q_uc_themes(theater, region, themes_text)
                if themes_df is not None and not themes_df.empty:
                    response = str(themes_df["RESPONSE"].iloc[0])
                    split = response.split("**LOSS THEMES")
                    col1, col2 = st.columns(2)
                    with col1:
                        with st.container(border=True):
                            st.markdown("### Win Themes")
                            st.markdown(split[0].replace("**WIN THEMES**", "").replace("**WIN THEMES —", "").strip())
                    with col2:
                        with st.container(border=True):
                            st.markdown("### Loss Themes")
                            st.markdown(("**LOSS THEMES" + split[1]) if len(split) > 1 else "")
                else:
                    st.warning("Theme analysis unavailable — Cortex model error.")
            else:
                st.info("No recent win/loss data to analyze.")
        else:
            st.info("Click **Identify Themes** to surface recurring patterns across wins and losses.")

        st.divider()
        st.markdown("### AI Win/Loss Synthesis")
        st.caption("Powered by Snowflake Cortex claude-3-5-sonnet · Based on last 12 months of DE use cases")

        if st.button("Generate AI Win/Loss Analysis", key="uc_ai_btn", type="primary"):
            wins_text = _uc_rows_to_text(df_uc_wins, "WINS") if df_uc_wins is not None and not df_uc_wins.empty else "No wins data."
            losses_text = _uc_rows_to_text(df_uc_losses, "LOSSES") if df_uc_losses is not None and not df_uc_losses.empty else "No losses data."
            with st.spinner("Generating AI win/loss analysis — this may take 15–30 seconds…"):
                uc_ai_df = q_uc_ai(theater, region, wins_text, losses_text)
            if uc_ai_df is not None and not uc_ai_df.empty:
                response = str(uc_ai_df["RESPONSE"].iloc[0])
                parts = response.split("PART 2")
                if len(parts) == 3:
                    p1, p2, p3 = parts[0], "PART 2" + parts[1], "PART 2" + parts[2]
                    col1, col2 = st.columns(2)
                    with col1:
                        with st.container(border=True):
                            st.markdown("### What Is Working")
                            st.markdown(p1.replace("**PART 1 — WHAT IS WORKING (Wins & Go-Lives)**", "").replace("PART 1 — WHAT IS WORKING (Wins & Go-Lives)", "").strip())
                    with col2:
                        with st.container(border=True):
                            st.markdown("### What Is Not Working")
                            st.markdown(p2.replace("**PART 2 — WHAT IS NOT WORKING (Losses)**", "").replace("PART 2 — WHAT IS NOT WORKING (Losses)", "").strip())
                    with st.container(border=True):
                        st.markdown("### Field Recommendations")
                        st.markdown(p3.replace("**PART 3 — FIELD RECOMMENDATIONS**", "").replace("PART 3 — FIELD RECOMMENDATIONS", "").strip())
                else:
                    st.markdown(response)
            else:
                st.error("Could not generate win/loss analysis. Check that Cortex is enabled in this account.")
        else:
            st.info("Click **Generate AI Win/Loss Analysis** to run Cortex AI synthesis on use case wins and losses. This takes 15–30 seconds.")


# ─── Tab 11: Product Gaps ────────────────────────────────────────────────────

PGAP_BASE = "https://snowflakecomputing.atlassian.net/browse/"

with tabs[7]:
    st.subheader("Product Gaps Impacting Territory")
    st.caption(
        f"Source: SALES.DEV.ACCOUNT_GAPS + PRODUCT_GAPS · {theater} · {region_label} · "
        "Latest snapshot · PGAP links open Jira"
    )

    with st.spinner("Loading product gap data…"):
        df_pgap_sum = q_pgap_summary(theater, region)

    if df_pgap_sum is None or df_pgap_sum.empty:
        st.info("No product gap data found for this territory.")
    else:
        df_pgap_sum = df_pgap_sum.copy()
        df_pgap_sum["ACCOUNT_COUNT"] = pd.to_numeric(df_pgap_sum["ACCOUNT_COUNT"], errors="coerce").fillna(0)
        df_pgap_sum["TOTAL_ACV"] = pd.to_numeric(df_pgap_sum["TOTAL_ACV"], errors="coerce").fillna(0)
        df_pgap_sum["PGAP_LINK"] = df_pgap_sum["PGAP_URL"].fillna("") + "#" + df_pgap_sum["PGAP_ID"].fillna("")

        tab_sum_df = (
            df_pgap_sum[["PGAP_LINK", "PGAP_TITLE", "PRODUCT_USE_CASE",
                          "FEATURE_GROUP", "STATUS", "ACCOUNT_COUNT", "TOTAL_ACV",
                          "CUSTOMERS_IMPACTED"]]
            .copy()
        )
        tab_sum_df["TOTAL_ACV"] = pd.to_numeric(tab_sum_df["TOTAL_ACV"], errors="coerce").fillna(0).round(0).astype(int)
        tab_sum_df = tab_sum_df.sort_values("ACCOUNT_COUNT", ascending=False).reset_index(drop=True)

        st.markdown("#### Product Gaps Summary")
        st.dataframe(
            tab_sum_df,
            use_container_width=True,
            column_config={
                "PGAP_LINK": st.column_config.LinkColumn("PGAP", display_text=r"#(PGAP-\d+)"),
                "PGAP_TITLE": st.column_config.TextColumn("Title", width="large"),
                "PRODUCT_USE_CASE": st.column_config.TextColumn("Product Use Case"),
                "FEATURE_GROUP": st.column_config.TextColumn("Feature Group"),
                "STATUS": st.column_config.TextColumn("Status"),
                "ACCOUNT_COUNT": st.column_config.NumberColumn("# Accounts", format="%d"),
                "TOTAL_ACV": st.column_config.NumberColumn("Total ACV", format="$%,d"),
                "CUSTOMERS_IMPACTED": st.column_config.TextColumn("Customers Impacted", width="large"),
            },
            hide_index=True,
            height=560,
        )


# ─── Tab 12: Business Problems ───────────────────────────────────────────────

_DE_TUC_FILTER = """(
    ARRAY_CONTAINS('DE: Ingestion'::VARIANT, TECHNICAL_USE_CASE_ARRAY)
    OR ARRAY_CONTAINS('DE: Transformation'::VARIANT, TECHNICAL_USE_CASE_ARRAY)
    OR ARRAY_CONTAINS('DE: Interoperable Storage'::VARIANT, TECHNICAL_USE_CASE_ARRAY)
    OR ARRAY_CONTAINS('Analytics: Lakehouse Analytics'::VARIANT, TECHNICAL_USE_CASE_ARRAY)
)"""

with tabs[8]:
    st.subheader("Business Problems — DE & Lakehouse Use Cases")
    st.caption(
        f"Source: SALES.SALES_ENGINEERING.USE_CASE_LEVEL_DATA · {theater} · {region_label} · "
        "Active + Won use cases · DE/Lakehouse technical use cases only"
    )

    with st.spinner("Loading business problems data…"):
        df_biz = q_biz_problems(theater, region)
        df_biz_det = q_biz_problems_detail(theater, region)

    if df_biz is None or df_biz.empty:
        st.info("No business problem data found for this territory.")
    else:
        df_biz = df_biz.copy()
        df_biz["UC_COUNT"] = pd.to_numeric(df_biz["UC_COUNT"], errors="coerce").fillna(0)
        df_biz["ACCT_COUNT"] = pd.to_numeric(df_biz["ACCT_COUNT"], errors="coerce").fillna(0)
        df_biz["TOTAL_EACV_M"] = (pd.to_numeric(df_biz["TOTAL_EACV"], errors="coerce").fillna(0) / 1_000_000).round(2)

        col_chart, col_table = st.columns([3, 2])

        with col_chart:
            st.markdown("**Top Business Problems by EACV**")
            import altair as alt
            chart_df = df_biz.head(15).copy().sort_values("TOTAL_EACV_M", ascending=True)
            chart = (
                alt.Chart(chart_df)
                .mark_bar()
                .encode(
                    y=alt.Y("BUSINESS_PROBLEM:N", sort=list(chart_df["BUSINESS_PROBLEM"]), title=None,
                            axis=alt.Axis(labelLimit=280)),
                    x=alt.X("TOTAL_EACV_M:Q", title="Total EACV ($M)"),
                    tooltip=[
                        alt.Tooltip("BUSINESS_PROBLEM:N", title="Business Problem"),
                        alt.Tooltip("TOTAL_EACV_M:Q", title="EACV ($M)", format=".2f"),
                        alt.Tooltip("UC_COUNT:Q", title="Use Cases"),
                        alt.Tooltip("ACCT_COUNT:Q", title="Accounts"),
                    ],
                )
                .properties(height=420)
            )
            st.altair_chart(chart, use_container_width=True)

        with col_table:
            st.markdown("**Summary by Business Problem**")
            disp_biz = df_biz[["BUSINESS_PROBLEM", "UC_COUNT", "ACCT_COUNT", "TOTAL_EACV_M"]].rename(columns={
                "BUSINESS_PROBLEM": "Business Problem",
                "UC_COUNT": "Use Cases",
                "ACCT_COUNT": "Accounts",
                "TOTAL_EACV_M": "EACV ($M)",
            })
            st.dataframe(
                disp_biz,
                use_container_width=True,
                column_config={
                    "Business Problem": st.column_config.TextColumn("Business Problem", width="large"),
                    "Use Cases": st.column_config.NumberColumn("Use Cases", format="%d"),
                    "Accounts": st.column_config.NumberColumn("Accounts", format="%d"),
                    "EACV ($M)": st.column_config.NumberColumn("EACV ($M)", format="$%.2fM"),
                },
                hide_index=True,
                height=420,
            )

        st.divider()
        st.markdown("### AI Business Problem Analysis")
        st.caption("Powered by Snowflake Cortex claude-3-5-sonnet · Identifies industry-specific business problem themes")

        if st.button("Generate AI Business Problem Analysis", key="biz_ai_btn", type="primary"):
            summary_rows = []
            if df_biz_det is not None and not df_biz_det.empty:
                for _, row in df_biz_det.head(60).iterrows():
                    summary_rows.append(
                        f"Problem: {row.get('BUSINESS_PROBLEM','?')} | "
                        f"Account: {row.get('ACCOUNT_NAME','?')} | "
                        f"Industry: {row.get('ACCOUNT_INDUSTRY','?')} | "
                        f"Use Case: {row.get('USE_CASE_NAME','?')} | "
                        f"EACV: {row.get('EACV', 0)} | "
                        f"Tech: {row.get('TECHNICAL_USE_CASE','?')}"
                    )
            summary_text = "\n".join(summary_rows) or "No data available."
            with st.spinner("Generating AI business problem analysis — this may take 15–30 seconds…"):
                biz_ai_df = q_biz_problems_ai(theater, region, summary_text)
            if biz_ai_df is not None and not biz_ai_df.empty:
                with st.container(border=True):
                    st.markdown(str(biz_ai_df["RESPONSE"].iloc[0]))
            else:
                st.error("Could not generate analysis. Check that Cortex is enabled in this account.")
        else:
            st.info("Click **Generate AI Business Problem Analysis** to identify industry-specific themes from this territory's use cases.")
