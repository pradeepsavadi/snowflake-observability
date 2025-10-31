"""
Snowflake Holistic Observability Dashboard - Home Page
======================================================
Executive Overview with KPIs, Alerts, Trends, and AI Summary

This is the main entry point for the multi-page Streamlit app.
"""

import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from utils import (
    initialize_session_state,
    render_settings_sidebar,
    get_snowflake_session,
    format_bytes,
    format_number,
    create_metric_card,
    create_trend_chart,
    create_alert_badge,
    apply_custom_css,
    render_page_header,
    SnowflakeQueries,
    AIInsightsGenerator
)

# Page configuration
st.set_page_config(
    page_title="Snowflake Observability Dashboard",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom styling
apply_custom_css()

# Initialize session state and render settings
initialize_session_state()
render_settings_sidebar()

# Main header
st.markdown('<p class="main-header">❄️ Snowflake Holistic Observability Dashboard</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Comprehensive monitoring, optimization, and AI-powered insights for your Snowflake environment</p>', unsafe_allow_html=True)

# Get Snowflake session
session = get_snowflake_session()
if not session:
    st.error("Failed to connect to Snowflake. Please check your connection.")
    st.stop()

# Initialize query and AI classes
queries = SnowflakeQueries(session)
ai_insights = AIInsightsGenerator(session)

# Get time period from session state
time_period = st.session_state.time_period
credit_cost = st.session_state.credit_cost
storage_cost = st.session_state.storage_cost_per_tb

# ============================================================================
# EXECUTIVE OVERVIEW SECTION
# ============================================================================

st.markdown("---")
render_page_header("📊 Executive Overview", "Key metrics and insights at a glance")

# Quick stats sidebar info
with st.sidebar:
    st.markdown("---")
    st.markdown("### 📈 Quick Stats")

    try:
        # Get warehouses count from WAREHOUSE_METERING_HISTORY
        wh_count_query = """
        SELECT COUNT(DISTINCT WAREHOUSE_NAME) AS ACTIVE_WAREHOUSES
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_DATE())
        """
        wh_count = session.sql(wh_count_query).to_pandas()['ACTIVE_WAREHOUSES'].iloc[0]

        # Get databases count
        db_count_query = """
        SELECT COUNT(DISTINCT DATABASE_NAME) AS ACTIVE_DATABASES
        FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES
        WHERE DELETED IS NULL
        """
        db_count = session.sql(db_count_query).to_pandas()['ACTIVE_DATABASES'].iloc[0]

        # Get users count
        user_count_query = """
        SELECT COUNT(DISTINCT NAME) AS ACTIVE_USERS
        FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
        WHERE DELETED_ON IS NULL
        """
        user_count = session.sql(user_count_query).to_pandas()['ACTIVE_USERS'].iloc[0]

        # Get total credits
        credits_query = f"""
        SELECT SUM(CREDITS_USED) AS TOTAL_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        """
        total_credits = session.sql(credits_query).to_pandas()['TOTAL_CREDITS'].iloc[0]

        st.metric("Active Warehouses", int(wh_count))
        st.metric("Active Databases", int(db_count))
        st.metric("Active Users", int(user_count))
        st.metric("Total Credits", f"{total_credits:.1f}")
    except Exception as e:
        st.sidebar.error(f"Error loading quick stats: {str(e)}")

# Main KPIs
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("📈 Key Performance Indicators")

    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

    with st.spinner("Loading KPIs..."):
        try:
            # Get warehouse metrics
            warehouse_metrics = queries.get_warehouse_metrics(time_period)
            total_credits = warehouse_metrics['TOTAL_CREDITS'].sum() if not warehouse_metrics.empty else 0

            with kpi_col1:
                st.metric(
                    "Total Credits",
                    f"{total_credits:,.1f}",
                    help=f"Credits used in last {time_period} days"
                )

            # Get storage metrics
            storage_metrics = queries.get_storage_metrics(time_period)
            total_storage = storage_metrics['TOTAL_BYTES'].sum() if not storage_metrics.empty else 0

            with kpi_col2:
                st.metric(
                    "Total Storage",
                    format_bytes(total_storage),
                    help="Current total storage across all databases"
                )

            # Get query performance
            query_issues = queries.get_query_performance_insights(time_period)
            total_issues = query_issues['QUERY_COUNT'].sum() if not query_issues.empty else 0

            with kpi_col3:
                st.metric(
                    "Query Issues",
                    int(total_issues),
                    delta_color="inverse",
                    help="Queries with performance problems"
                )

            # Calculate estimated cost
            total_cost = total_credits * credit_cost + (total_storage / (1024**4)) * storage_cost

            with kpi_col4:
                st.metric(
                    "Estimated Cost",
                    f"${total_cost:,.2f}",
                    help=f"Estimated cost for {time_period} days"
                )

        except Exception as e:
            st.error(f"Error loading KPIs: {str(e)}")

with col2:
    st.subheader("🤖 AI Quick Insights")

    try:
        # Prepare context for AI
        context_metrics = {
            "Total Credits Used": f"{total_credits:,.1f}",
            "Estimated Cost": f"${total_cost:,.2f}",
            "Total Storage": format_bytes(total_storage),
            "Query Issues": int(total_issues),
            "Time Period": f"{time_period} days"
        }

        with st.spinner("Generating AI insights..."):
            if ai_insights.check_cortex_availability():
                summary = ai_insights.generate_insight(str(context_metrics), "summary")
                st.info(summary)
            else:
                st.warning("AI insights require Cortex Complete. Check permissions.")

    except Exception as e:
        st.warning(f"AI insights temporarily unavailable: {str(e)}")

# ============================================================================
# ALERTS SECTION
# ============================================================================

st.markdown("---")
st.subheader("🚨 Active Alerts & Recommendations")

alert_col1, alert_col2 = st.columns(2)

with alert_col1:
    st.markdown("#### 💰 Cost Alerts")

    try:
        # Get cost anomalies
        cost_anomalies_query = f"""
        WITH daily_costs AS (
            SELECT
                DATE_TRUNC('DAY', START_TIME) AS COST_DATE,
                SUM(CREDITS_USED) * {credit_cost} AS DAILY_COST
            FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            GROUP BY COST_DATE
        ),
        cost_stats AS (
            SELECT
                AVG(DAILY_COST) AS AVG_DAILY_COST,
                STDDEV(DAILY_COST) AS STDDEV_DAILY_COST
            FROM daily_costs
        )
        SELECT
            c.COST_DATE,
            c.DAILY_COST,
            s.AVG_DAILY_COST,
            ABS((c.DAILY_COST - s.AVG_DAILY_COST) / NULLIF(s.STDDEV_DAILY_COST, 0)) AS Z_SCORE
        FROM daily_costs c
        CROSS JOIN cost_stats s
        WHERE ABS((c.DAILY_COST - s.AVG_DAILY_COST) / NULLIF(s.STDDEV_DAILY_COST, 0)) > 2
        ORDER BY c.COST_DATE DESC
        """
        cost_anomalies = session.sql(cost_anomalies_query).to_pandas()

        if not cost_anomalies.empty:
            anomaly_count = len(cost_anomalies)
            create_alert_badge(
                f"⚠️ {anomaly_count} cost anomal{'y' if anomaly_count == 1 else 'ies'} detected",
                "warning"
            )
            latest_anomaly = cost_anomalies.iloc[0]
            st.caption(f"Latest: ${latest_anomaly['DAILY_COST']:.2f} on {latest_anomaly['COST_DATE'].strftime('%Y-%m-%d')}")
        else:
            create_alert_badge("✅ No cost anomalies detected", "success")

    except Exception as e:
        st.error(f"Error checking cost alerts: {str(e)}")

    try:
        # Query performance alerts
        if total_issues > 10:
            create_alert_badge(
                f"⚠️ {int(total_issues)} queries with performance issues",
                "warning"
            )
            st.caption("Review Performance page for details")
        else:
            create_alert_badge("✅ Query performance is healthy", "success")

    except:
        pass

with alert_col2:
    st.markdown("#### 🏢 Optimization Opportunities")

    try:
        # Storage optimization
        storage_issues = queries.get_table_storage_insights()
        if not storage_issues.empty:
            storage_savings = (storage_issues['TOTAL_BYTES'].sum() / (1024**4)) * storage_cost
            create_alert_badge(
                f"💾 ${storage_savings:,.2f}/month potential savings from storage optimization",
                "info"
            )
            st.caption(f"{len(storage_issues)} tables need attention")
        else:
            create_alert_badge("✅ Storage is optimized", "success")

    except Exception as e:
        st.warning(f"Storage check unavailable: {str(e)}")

    try:
        # Warehouse recommendations
        warehouse_recs = queries.get_warehouse_recommendations(time_period)
        needs_action = len(warehouse_recs[warehouse_recs['RECOMMENDATION'] != 'OPTIMAL']) if not warehouse_recs.empty else 0

        if needs_action > 0:
            create_alert_badge(
                f"🏢 {needs_action} warehouse(s) need optimization",
                "info"
            )
            st.caption("Review Warehouses page for recommendations")
        else:
            create_alert_badge("✅ Warehouses are optimized", "success")

    except Exception as e:
        st.warning(f"Warehouse check unavailable: {str(e)}")

# ============================================================================
# TRENDS SECTION
# ============================================================================

st.markdown("---")
st.subheader("📈 Trends & Patterns")

trend_col1, trend_col2 = st.columns(2)

with trend_col1:
    st.markdown("#### 💰 Daily Cost Trend")

    try:
        daily_cost_query = f"""
        SELECT
            DATE_TRUNC('DAY', START_TIME) AS DATE,
            SUM(CREDITS_USED) * {credit_cost} AS DAILY_COST
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        GROUP BY DATE
        ORDER BY DATE
        """
        daily_costs = session.sql(daily_cost_query).to_pandas()

        if not daily_costs.empty:
            daily_costs['DATE'] = pd.to_datetime(daily_costs['DATE'])
            chart = create_trend_chart(daily_costs, 'DATE', 'DAILY_COST', 'Daily Cost Trend ($)')
            if chart:
                st.altair_chart(chart, use_container_width=True)

                # Calculate trend
                avg_cost = daily_costs['DAILY_COST'].mean()
                latest_cost = daily_costs['DAILY_COST'].iloc[-1]
                trend_pct = ((latest_cost - avg_cost) / avg_cost * 100) if avg_cost > 0 else 0

                if trend_pct > 10:
                    st.warning(f"Cost trending up: {trend_pct:.1f}% above average")
                elif trend_pct < -10:
                    st.success(f"Cost trending down: {abs(trend_pct):.1f}% below average")
                else:
                    st.info(f"Cost stable: {trend_pct:.1f}% from average")
        else:
            st.info("No cost data available")

    except Exception as e:
        st.error(f"Error loading cost trend: {str(e)}")

with trend_col2:
    st.markdown("#### 📊 Query Volume Trend")

    try:
        query_volume_query = f"""
        SELECT
            DATE_TRUNC('DAY', START_TIME) AS DATE,
            COUNT(*) AS QUERY_COUNT
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        GROUP BY DATE
        ORDER BY DATE
        """
        query_volume = session.sql(query_volume_query).to_pandas()

        if not query_volume.empty:
            query_volume['DATE'] = pd.to_datetime(query_volume['DATE'])
            chart = create_trend_chart(query_volume, 'DATE', 'QUERY_COUNT', 'Daily Query Volume')
            if chart:
                st.altair_chart(chart, use_container_width=True)

                # Show peak day
                peak_day = query_volume.loc[query_volume['QUERY_COUNT'].idxmax()]
                st.info(f"Peak: {int(peak_day['QUERY_COUNT']):,} queries on {peak_day['DATE'].strftime('%Y-%m-%d')}")
        else:
            st.info("No query volume data available")

    except Exception as e:
        st.error(f"Error loading query volume: {str(e)}")

# ============================================================================
# USAGE BREAKDOWN
# ============================================================================

st.markdown("---")
st.subheader("📊 Usage Breakdown")

breakdown_col1, breakdown_col2 = st.columns(2)

with breakdown_col1:
    st.markdown("#### Top 5 Warehouses by Credits")

    try:
        if not warehouse_metrics.empty:
            top_5_wh = warehouse_metrics.head(5)

            # Calculate percentage
            total_wh_credits = warehouse_metrics['TOTAL_CREDITS'].sum()
            top_5_wh['PERCENTAGE'] = (top_5_wh['TOTAL_CREDITS'] / total_wh_credits * 100).round(1)

            # Create bar chart
            chart = alt.Chart(top_5_wh).mark_bar().encode(
                y=alt.Y('WAREHOUSE_NAME:N', sort='-x', title='Warehouse'),
                x=alt.X('TOTAL_CREDITS:Q', title='Credits'),
                color=alt.Color('TOTAL_CREDITS:Q', scale=alt.Scale(scheme='blues'), legend=None),
                tooltip=['WAREHOUSE_NAME', alt.Tooltip('TOTAL_CREDITS:Q', format=',.2f'), 'PERCENTAGE']
            ).properties(height=250)

            st.altair_chart(chart, use_container_width=True)

            # Show summary
            top_5_pct = top_5_wh['PERCENTAGE'].sum()
            st.caption(f"Top 5 warehouses account for {top_5_pct:.1f}% of credits")
        else:
            st.info("No warehouse data available")

    except Exception as e:
        st.error(f"Error loading warehouse breakdown: {str(e)}")

with breakdown_col2:
    st.markdown("#### Top 5 Databases by Storage")

    try:
        if not storage_metrics.empty:
            top_5_db = storage_metrics.head(5)
            top_5_db['SIZE_GB'] = top_5_db['TOTAL_BYTES'] / (1024**3)

            # Create bar chart
            chart = alt.Chart(top_5_db).mark_bar().encode(
                y=alt.Y('DATABASE_NAME:N', sort='-x', title='Database'),
                x=alt.X('SIZE_GB:Q', title='Storage (GB)'),
                color=alt.Color('SIZE_GB:Q', scale=alt.Scale(scheme='greens'), legend=None),
                tooltip=['DATABASE_NAME', alt.Tooltip('SIZE_GB:Q', format=',.2f')]
            ).properties(height=250)

            st.altair_chart(chart, use_container_width=True)

            # Show summary
            total_storage_gb = storage_metrics['TOTAL_BYTES'].sum() / (1024**3)
            top_5_storage_gb = top_5_db['SIZE_GB'].sum()
            top_5_pct = (top_5_storage_gb / total_storage_gb * 100) if total_storage_gb > 0 else 0
            st.caption(f"Top 5 databases account for {top_5_pct:.1f}% of storage")
        else:
            st.info("No storage data available")

    except Exception as e:
        st.error(f"Error loading storage breakdown: {str(e)}")

# ============================================================================
# NAVIGATION GUIDANCE
# ============================================================================

st.markdown("---")
st.subheader("🧭 Explore Dashboard Pages")

st.markdown("""
Navigate to different sections using the sidebar:

- **🏢 Warehouses** - Analyze warehouse usage, performance, and get optimization recommendations
- **💾 Storage** - Monitor storage costs, identify unused tables, track growth trends
- **🔄 Data Transfer** - Track cross-cloud and cross-region data transfers
- **👥 Users & Queries** - Analyze user activity, query patterns, and performance
- **🤖 AI & ML** - Monitor Cortex AI services usage and costs
- **🔧 Data Pipelines** - Track tasks, Snowpipes, and dynamic tables
- **⚡ Performance** - Identify query bottlenecks and optimization opportunities
- **🔒 Security** - Monitor access patterns and login activity
- **💰 Cost Management** - Detailed cost attribution and savings opportunities
- **✅ Data Quality** - Track data freshness and schema changes
- **🧠 AI Insights** - Interactive AI-powered analysis and custom insights
""")

# Footer
st.markdown("---")
col1, col2, col3 = st.columns(3)

with col1:
    st.caption(f"📅 Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

with col2:
    st.caption(f"⏱️ Time period: {time_period} days")

with col3:
    st.caption(f"💵 Credit cost: ${credit_cost}/credit")

st.caption("💡 **Tip:** Adjust settings in the sidebar to customize costs and time periods")
