"""
Snowflake Holistic Observability Dashboard - Storage Page
==========================================================
Monitor storage costs, identify unused tables, track growth trends
"""

import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
sys.path.append('..')

from utils import (
    initialize_session_state,
    render_settings_sidebar,
    get_snowflake_session,
    format_bytes,
    format_number,
    create_alert_badge,
    apply_custom_css,
    render_page_header,
    SnowflakeQueries,
    AIInsightsGenerator
)

# Page configuration
st.set_page_config(
    page_title="Storage - Snowflake Observability",
    page_icon="💾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom styling
apply_custom_css()

# Initialize session state and render settings
initialize_session_state()
render_settings_sidebar()

# Page header
render_page_header("💾 Storage Management", "Monitor storage costs, identify optimization opportunities, and track growth trends")

# Get Snowflake session
session = get_snowflake_session()
if not session:
    st.error("Failed to connect to Snowflake. Please check your connection.")
    st.stop()

# Initialize query and AI classes
queries = SnowflakeQueries(session)
ai_insights = AIInsightsGenerator(session)

# Get settings from session state
time_period = st.session_state.time_period
storage_cost = st.session_state.storage_cost_per_tb

# ============================================================================
# STORAGE OVERVIEW
# ============================================================================

st.markdown("---")
st.subheader("📊 Storage Overview")

col1, col2, col3, col4 = st.columns(4)

with st.spinner("Loading storage metrics..."):
    try:
        # Get storage metrics
        storage_metrics = queries.get_storage_metrics(time_period)

        # Calculate totals
        total_storage_bytes = storage_metrics['TOTAL_BYTES'].sum() if not storage_metrics.empty else 0
        total_storage_tb = total_storage_bytes / (1024**4)
        total_storage_cost = total_storage_tb * storage_cost

        # Get table storage insights
        table_insights = queries.get_table_storage_insights()
        total_tables = len(table_insights) if not table_insights.empty else 0

        # Get storage growth
        growth_query = f"""
        WITH current_storage AS (
            SELECT SUM(ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES) AS CURRENT_BYTES
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
            WHERE DELETED IS NULL
        ),
        past_storage AS (
            SELECT SUM(ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES) AS PAST_BYTES
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
            WHERE DELETED IS NULL
            AND CATALOG_DROPPED IS NULL
        )
        SELECT
            c.CURRENT_BYTES,
            p.PAST_BYTES,
            ((c.CURRENT_BYTES - p.PAST_BYTES) / NULLIF(p.PAST_BYTES, 0) * 100) AS GROWTH_PCT
        FROM current_storage c
        CROSS JOIN past_storage p
        """
        growth_data = session.sql(growth_query).to_pandas()
        growth_pct = growth_data['GROWTH_PCT'].iloc[0] if not growth_data.empty else 0

        with col1:
            st.metric(
                "Total Storage",
                format_bytes(total_storage_bytes),
                delta=f"{growth_pct:.1f}%" if growth_pct != 0 else None,
                help="Total storage across all databases"
            )

        with col2:
            st.metric(
                "Monthly Cost",
                f"${total_storage_cost:,.2f}",
                help=f"Based on ${storage_cost}/TB per month"
            )

        with col3:
            st.metric(
                "Total Tables",
                format_number(total_tables),
                help="Number of tables being monitored"
            )

        # Calculate potential savings from unused tables
        if not table_insights.empty:
            unused_storage = table_insights['TOTAL_BYTES'].sum()
            unused_tb = unused_storage / (1024**4)
            potential_savings = unused_tb * storage_cost
        else:
            potential_savings = 0

        with col4:
            st.metric(
                "Potential Savings",
                f"${potential_savings:,.2f}",
                help="From optimizing unused/stale tables"
            )

    except Exception as e:
        st.error(f"Error loading storage overview: {str(e)}")

# ============================================================================
# STORAGE TABS
# ============================================================================

tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Storage Trends",
    "🗄️ Database Analysis",
    "📋 Table Insights",
    "💡 Optimization Recommendations"
])

# ----------------------------------------------------------------------------
# TAB 1: Storage Trends
# ----------------------------------------------------------------------------

with tab1:
    st.markdown("### 📈 Storage Growth Trends")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Daily Storage Growth")

        try:
            daily_storage_query = f"""
            SELECT
                DATE_TRUNC('DAY', DATE) AS USAGE_DATE,
                SUM(AVERAGE_BYTES) AS TOTAL_BYTES,
                SUM(AVERAGE_BYTES) / POWER(1024, 4) AS TOTAL_TB
            FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
            WHERE USAGE_DATE >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            GROUP BY USAGE_DATE
            ORDER BY USAGE_DATE
            """
            daily_storage = session.sql(daily_storage_query).to_pandas()

            if not daily_storage.empty:
                daily_storage['USAGE_DATE'] = pd.to_datetime(daily_storage['USAGE_DATE'])

                # Create line chart
                chart = alt.Chart(daily_storage).mark_line(point=True, strokeWidth=3).encode(
                    x=alt.X('USAGE_DATE:T', title='Date', axis=alt.Axis(format='%Y-%m-%d')),
                    y=alt.Y('TOTAL_TB:Q', title='Storage (TB)'),
                    tooltip=[
                        alt.Tooltip('USAGE_DATE:T', title='Date', format='%Y-%m-%d'),
                        alt.Tooltip('TOTAL_TB:Q', title='Storage (TB)', format=',.2f'),
                        alt.Tooltip('TOTAL_BYTES:Q', title='Bytes', format=',.0f')
                    ]
                ).properties(height=300)

                st.altair_chart(chart, use_container_width=True)

                # Show growth rate
                if len(daily_storage) > 1:
                    first_value = daily_storage['TOTAL_TB'].iloc[0]
                    last_value = daily_storage['TOTAL_TB'].iloc[-1]
                    growth_rate = ((last_value - first_value) / first_value * 100) if first_value > 0 else 0

                    if growth_rate > 20:
                        st.warning(f"⚠️ Storage growing rapidly: {growth_rate:.1f}% over {time_period} days")
                    elif growth_rate > 10:
                        st.info(f"📊 Moderate storage growth: {growth_rate:.1f}% over {time_period} days")
                    else:
                        st.success(f"✅ Stable storage growth: {growth_rate:.1f}% over {time_period} days")
            else:
                st.info("No daily storage data available")

        except Exception as e:
            st.error(f"Error loading storage trends: {str(e)}")

    with col2:
        st.markdown("#### Storage Type Breakdown")

        try:
            storage_type_query = """
            SELECT
                SUM(ACTIVE_BYTES) AS ACTIVE_BYTES,
                SUM(TIME_TRAVEL_BYTES) AS TIME_TRAVEL_BYTES,
                SUM(FAILSAFE_BYTES) AS FAILSAFE_BYTES,
                SUM(RETAINED_FOR_CLONE_BYTES) AS CLONE_BYTES
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
            WHERE DELETED IS NULL
            """
            storage_types = session.sql(storage_type_query).to_pandas()

            if not storage_types.empty:
                # Prepare data for pie chart
                type_data = pd.DataFrame({
                    'Type': ['Active', 'Time Travel', 'Failsafe', 'Clone'],
                    'Bytes': [
                        storage_types['ACTIVE_BYTES'].iloc[0],
                        storage_types['TIME_TRAVEL_BYTES'].iloc[0],
                        storage_types['FAILSAFE_BYTES'].iloc[0],
                        storage_types['CLONE_BYTES'].iloc[0]
                    ]
                })
                type_data = type_data[type_data['Bytes'] > 0]  # Filter out zeros
                type_data['Size (TB)'] = type_data['Bytes'] / (1024**4)

                # Create pie chart
                fig = px.pie(
                    type_data,
                    values='Bytes',
                    names='Type',
                    title='Storage Distribution by Type',
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                fig.update_traces(
                    textposition='inside',
                    textinfo='percent+label',
                    hovertemplate='<b>%{label}</b><br>%{value:,.0f} bytes<br>%{percent}'
                )
                st.plotly_chart(fig, use_container_width=True)

                # Show recommendations for time travel and failsafe
                time_travel_tb = storage_types['TIME_TRAVEL_BYTES'].iloc[0] / (1024**4)
                failsafe_tb = storage_types['FAILSAFE_BYTES'].iloc[0] / (1024**4)

                if time_travel_tb > 1:
                    time_travel_cost = time_travel_tb * storage_cost
                    create_alert_badge(
                        f"💡 {time_travel_tb:.2f} TB in Time Travel storage (${time_travel_cost:.2f}/month)",
                        "info"
                    )
                    st.caption("Consider reducing retention period for non-critical tables")

                if failsafe_tb > 1:
                    failsafe_cost = failsafe_tb * storage_cost
                    create_alert_badge(
                        f"💡 {failsafe_tb:.2f} TB in Failsafe storage (${failsafe_cost:.2f}/month)",
                        "info"
                    )
                    st.caption("Failsafe is automatic but contributes to storage costs")
            else:
                st.info("No storage type data available")

        except Exception as e:
            st.error(f"Error loading storage type breakdown: {str(e)}")

    # Storage by object type
    st.markdown("---")
    st.markdown("#### Storage by Object Type")

    try:
        object_storage_query = """
        SELECT
            TABLE_CATALOG AS DATABASE_NAME,
            TABLE_SCHEMA AS SCHEMA_NAME,
            COUNT(*) AS TABLE_COUNT,
            SUM(ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES) AS TOTAL_BYTES
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
        WHERE DELETED IS NULL
        GROUP BY DATABASE_NAME, SCHEMA_NAME
        ORDER BY TOTAL_BYTES DESC
        LIMIT 20
        """
        object_storage = session.sql(object_storage_query).to_pandas()

        if not object_storage.empty:
            object_storage['SIZE_GB'] = object_storage['TOTAL_BYTES'] / (1024**3)
            object_storage['SCHEMA_PATH'] = object_storage['DATABASE_NAME'] + '.' + object_storage['SCHEMA_NAME']

            # Create horizontal bar chart
            chart = alt.Chart(object_storage.head(15)).mark_bar().encode(
                y=alt.Y('SCHEMA_PATH:N', sort='-x', title='Schema'),
                x=alt.X('SIZE_GB:Q', title='Storage (GB)'),
                color=alt.Color('SIZE_GB:Q', scale=alt.Scale(scheme='teals'), legend=None),
                tooltip=[
                    'SCHEMA_PATH',
                    alt.Tooltip('SIZE_GB:Q', title='Storage (GB)', format=',.2f'),
                    alt.Tooltip('TABLE_COUNT:Q', title='Tables', format=',')
                ]
            ).properties(height=400)

            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No schema storage data available")

    except Exception as e:
        st.error(f"Error loading object storage: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 2: Database Analysis
# ----------------------------------------------------------------------------

with tab2:
    st.markdown("### 🗄️ Database Storage Analysis")

    try:
        if not storage_metrics.empty:
            # Add calculated columns
            storage_metrics['SIZE_GB'] = storage_metrics['TOTAL_BYTES'] / (1024**3)
            storage_metrics['SIZE_TB'] = storage_metrics['TOTAL_BYTES'] / (1024**4)
            storage_metrics['MONTHLY_COST'] = storage_metrics['SIZE_TB'] * storage_cost

            # Display database storage table
            display_cols = ['DATABASE_NAME', 'SIZE_TB', 'MONTHLY_COST']
            display_df = storage_metrics[display_cols].copy()
            display_df.columns = ['Database', 'Storage (TB)', 'Monthly Cost ($)']

            st.dataframe(
                display_df.style.format({
                    'Storage (TB)': '{:.2f}',
                    'Monthly Cost ($)': '${:,.2f}'
                }).background_gradient(subset=['Storage (TB)'], cmap='YlOrRd'),
                use_container_width=True
            )

            # Top 10 databases chart
            st.markdown("#### Top 10 Databases by Storage")

            top_10_db = storage_metrics.head(10)

            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=top_10_db['DATABASE_NAME'],
                y=top_10_db['SIZE_TB'],
                marker_color='steelblue',
                text=top_10_db['SIZE_TB'].round(2),
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>Storage: %{y:.2f} TB<br>Cost: $%{customdata:.2f}<extra></extra>',
                customdata=top_10_db['MONTHLY_COST']
            ))

            fig.update_layout(
                xaxis_title="Database",
                yaxis_title="Storage (TB)",
                showlegend=False,
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

            # Database growth analysis
            st.markdown("---")
            st.markdown("#### Database Growth Analysis")

            db_growth_query = f"""
            WITH recent_usage AS (
                SELECT
                    DATABASE_NAME,
                    AVG(AVERAGE_BYTES) AS AVG_BYTES_RECENT
                FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
                WHERE USAGE_DATE >= DATEADD(DAY, -7, CURRENT_DATE())
                GROUP BY DATABASE_NAME
            ),
            past_usage AS (
                SELECT
                    DATABASE_NAME,
                    AVG(AVERAGE_BYTES) AS AVG_BYTES_PAST
                FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
                WHERE USAGE_DATE BETWEEN DATEADD(DAY, -{time_period}, CURRENT_DATE())
                    AND DATEADD(DAY, -7, CURRENT_DATE())
                GROUP BY DATABASE_NAME
            )
            SELECT
                r.DATABASE_NAME,
                r.AVG_BYTES_RECENT,
                p.AVG_BYTES_PAST,
                ((r.AVG_BYTES_RECENT - p.AVG_BYTES_PAST) / NULLIF(p.AVG_BYTES_PAST, 0) * 100) AS GROWTH_PCT,
                (r.AVG_BYTES_RECENT - p.AVG_BYTES_PAST) AS GROWTH_BYTES
            FROM recent_usage r
            JOIN past_usage p ON r.DATABASE_NAME = p.DATABASE_NAME
            WHERE p.AVG_BYTES_PAST > 0
            ORDER BY ABS(GROWTH_PCT) DESC
            LIMIT 15
            """

            db_growth = session.sql(db_growth_query).to_pandas()

            if not db_growth.empty:
                db_growth['GROWTH_GB'] = db_growth['GROWTH_BYTES'] / (1024**3)

                # Create growth chart
                fig = go.Figure()

                colors = ['red' if x < 0 else 'green' for x in db_growth['GROWTH_PCT']]

                fig.add_trace(go.Bar(
                    x=db_growth['DATABASE_NAME'],
                    y=db_growth['GROWTH_PCT'],
                    marker_color=colors,
                    text=db_growth['GROWTH_PCT'].round(1).astype(str) + '%',
                    textposition='outside',
                    hovertemplate='<b>%{x}</b><br>Growth: %{y:.1f}%<br>Change: %{customdata:.2f} GB<extra></extra>',
                    customdata=db_growth['GROWTH_GB']
                ))

                fig.update_layout(
                    title="Database Growth Rate (Last 7 Days vs Previous Period)",
                    xaxis_title="Database",
                    yaxis_title="Growth (%)",
                    showlegend=False,
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)

                # Highlight databases with unusual growth
                high_growth = db_growth[db_growth['GROWTH_PCT'] > 50]
                if not high_growth.empty:
                    create_alert_badge(
                        f"⚠️ {len(high_growth)} database(s) with >50% growth in last 7 days",
                        "warning"
                    )
                    for _, db in high_growth.iterrows():
                        st.caption(f"  • {db['DATABASE_NAME']}: {db['GROWTH_PCT']:.1f}% growth ({db['GROWTH_GB']:.2f} GB)")
            else:
                st.info("No database growth data available")

        else:
            st.info("No database storage data available")

    except Exception as e:
        st.error(f"Error loading database analysis: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 3: Table Insights
# ----------------------------------------------------------------------------

with tab3:
    st.markdown("### 📋 Table-Level Storage Insights")

    try:
        table_insights = queries.get_table_storage_insights()

        if not table_insights.empty:
            # Add calculated columns
            table_insights['SIZE_GB'] = table_insights['TOTAL_BYTES'] / (1024**3)
            table_insights['SIZE_TB'] = table_insights['TOTAL_BYTES'] / (1024**4)
            table_insights['MONTHLY_COST'] = table_insights['SIZE_TB'] * storage_cost
            table_insights['TABLE_PATH'] = (
                table_insights['TABLE_CATALOG'].astype(str) + '.' +
                table_insights['TABLE_SCHEMA'].astype(str) + '.' +
                table_insights['TABLE_NAME'].astype(str)
            )

            # Summary metrics
            col1, col2, col3 = st.columns(3)

            with col1:
                total_stale = len(table_insights)
                st.metric("Tables Needing Attention", format_number(total_stale))

            with col2:
                total_wasted_storage = table_insights['TOTAL_BYTES'].sum() / (1024**4)
                st.metric("Reclaimable Storage", f"{total_wasted_storage:.2f} TB")

            with col3:
                monthly_savings = total_wasted_storage * storage_cost
                st.metric("Potential Monthly Savings", f"${monthly_savings:,.2f}")

            st.markdown("---")

            # Filter options
            col1, col2 = st.columns([3, 1])

            with col1:
                search_term = st.text_input("🔍 Search tables", placeholder="Enter database, schema, or table name")

            with col2:
                min_size_gb = st.number_input("Min Size (GB)", min_value=0.0, value=0.0, step=1.0)

            # Apply filters
            filtered_tables = table_insights.copy()

            if search_term:
                filtered_tables = filtered_tables[
                    filtered_tables['TABLE_PATH'].str.contains(search_term, case=False, na=False)
                ]

            if min_size_gb > 0:
                filtered_tables = filtered_tables[filtered_tables['SIZE_GB'] >= min_size_gb]

            # Display filtered table insights
            st.markdown(f"#### Found {len(filtered_tables)} table(s)")

            if not filtered_tables.empty:
                display_cols = ['TABLE_PATH', 'SIZE_GB', 'MONTHLY_COST', 'ISSUE_TYPE']
                display_df = filtered_tables[display_cols].copy()
                display_df.columns = ['Table', 'Size (GB)', 'Monthly Cost ($)', 'Issue']

                st.dataframe(
                    display_df.style.format({
                        'Size (GB)': '{:.2f}',
                        'Monthly Cost ($)': '${:,.2f}'
                    }).background_gradient(subset=['Size (GB)'], cmap='YlOrRd'),
                    use_container_width=True,
                    height=400
                )

                # Download button for table insights
                csv = filtered_tables.to_csv(index=False)
                st.download_button(
                    label="📥 Download Table Insights CSV",
                    data=csv,
                    file_name=f"table_storage_insights_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No tables match the filter criteria")

            # Top 20 largest tables chart
            st.markdown("---")
            st.markdown("#### Top 20 Largest Tables")

            top_20_tables = table_insights.nlargest(20, 'SIZE_GB')

            chart = alt.Chart(top_20_tables).mark_bar().encode(
                y=alt.Y('TABLE_NAME:N', sort='-x', title='Table'),
                x=alt.X('SIZE_GB:Q', title='Storage (GB)'),
                color=alt.Color('SIZE_GB:Q', scale=alt.Scale(scheme='reds'), legend=None),
                tooltip=[
                    alt.Tooltip('TABLE_PATH:N', title='Full Path'),
                    alt.Tooltip('SIZE_GB:Q', title='Size (GB)', format=',.2f'),
                    alt.Tooltip('MONTHLY_COST:Q', title='Monthly Cost', format='$,.2f'),
                    'ISSUE_TYPE:N'
                ]
            ).properties(height=500)

            st.altair_chart(chart, use_container_width=True)

        else:
            st.info("No table storage insights available. This may indicate all tables are being actively used.")

    except Exception as e:
        st.error(f"Error loading table insights: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 4: Optimization Recommendations
# ----------------------------------------------------------------------------

with tab4:
    st.markdown("### 💡 Storage Optimization Recommendations")

    col1, col2 = st.columns([2, 1])

    with col1:
        try:
            recommendations = []

            # Get storage metrics for recommendations
            if not table_insights.empty:
                total_wasted_tb = table_insights['TOTAL_BYTES'].sum() / (1024**4)
                monthly_savings = total_wasted_tb * storage_cost
                annual_savings = monthly_savings * 12

                recommendations.append({
                    'priority': 'HIGH',
                    'category': 'Unused Tables',
                    'description': f"Drop or archive {len(table_insights)} unused/stale tables",
                    'potential_savings': f"${monthly_savings:,.2f}/month (${annual_savings:,.2f}/year)",
                    'action': 'Review table insights and drop unused tables or move to cheaper storage'
                })

            # Time Travel recommendations
            try:
                tt_query = """
                SELECT
                    COUNT(DISTINCT TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME) AS TABLE_COUNT,
                    SUM(TIME_TRAVEL_BYTES) AS TT_BYTES
                FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
                WHERE TIME_TRAVEL_BYTES > 1073741824  -- > 1 GB
                AND DELETED IS NULL
                """
                tt_data = session.sql(tt_query).to_pandas()

                if not tt_data.empty and tt_data['TABLE_COUNT'].iloc[0] > 0:
                    tt_tb = tt_data['TT_BYTES'].iloc[0] / (1024**4)
                    tt_savings = tt_tb * storage_cost * 0.5  # Assume 50% can be optimized

                    recommendations.append({
                        'priority': 'MEDIUM',
                        'category': 'Time Travel',
                        'description': f"{int(tt_data['TABLE_COUNT'].iloc[0])} tables with significant Time Travel storage",
                        'potential_savings': f"${tt_savings:,.2f}/month potential",
                        'action': 'Reduce retention period for non-critical tables (ALTER TABLE SET DATA_RETENTION_TIME_IN_DAYS = 1)'
                    })
            except:
                pass

            # Clone storage recommendations
            try:
                clone_query = """
                SELECT
                    SUM(RETAINED_FOR_CLONE_BYTES) AS CLONE_BYTES
                FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
                WHERE RETAINED_FOR_CLONE_BYTES > 0
                AND DELETED IS NULL
                """
                clone_data = session.sql(clone_query).to_pandas()

                if not clone_data.empty and clone_data['CLONE_BYTES'].iloc[0] > 0:
                    clone_tb = clone_data['CLONE_BYTES'].iloc[0] / (1024**4)

                    if clone_tb > 1:
                        clone_cost = clone_tb * storage_cost

                        recommendations.append({
                            'priority': 'LOW',
                            'category': 'Clone Storage',
                            'description': f"{clone_tb:.2f} TB retained for clones",
                            'potential_savings': f"${clone_cost:,.2f}/month if clones dropped",
                            'action': 'Review cloned tables and drop if no longer needed'
                        })
            except:
                pass

            # Storage growth recommendations
            try:
                if 'db_growth' in locals() and not db_growth.empty:
                    high_growth_dbs = db_growth[db_growth['GROWTH_PCT'] > 100]

                    if not high_growth_dbs.empty:
                        recommendations.append({
                            'priority': 'HIGH',
                            'category': 'Growth Alert',
                            'description': f"{len(high_growth_dbs)} database(s) with >100% growth in 7 days",
                            'potential_savings': 'Risk of unexpected costs',
                            'action': 'Investigate rapid growth: data ingestion issues, temporary tables, or pipeline problems'
                        })
            except:
                pass

            # Database consolidation
            if not storage_metrics.empty:
                small_dbs = storage_metrics[storage_metrics['SIZE_TB'] < 0.1]

                if len(small_dbs) > 5:
                    recommendations.append({
                        'priority': 'LOW',
                        'category': 'Consolidation',
                        'description': f"{len(small_dbs)} databases with <100 GB storage",
                        'potential_savings': 'Improved organization and management',
                        'action': 'Consider consolidating small databases to reduce complexity'
                    })

            # Display recommendations
            if recommendations:
                for i, rec in enumerate(recommendations, 1):
                    priority_colors = {
                        'HIGH': '🔴',
                        'MEDIUM': '🟡',
                        'LOW': '🟢'
                    }

                    with st.expander(f"{priority_colors.get(rec['priority'], '🔵')} {rec['category']} - {rec['description']}", expanded=(rec['priority'] == 'HIGH')):
                        st.markdown(f"**Priority:** {rec['priority']}")
                        st.markdown(f"**Potential Impact:** {rec['potential_savings']}")
                        st.markdown(f"**Recommended Action:**")
                        st.info(rec['action'])
            else:
                create_alert_badge("✅ No major storage optimization opportunities identified", "success")
                st.caption("Your storage is well optimized. Continue monitoring for changes.")

        except Exception as e:
            st.error(f"Error generating recommendations: {str(e)}")

    with col2:
        st.markdown("#### 🤖 AI-Powered Insights")

        try:
            if ai_insights.check_cortex_availability():
                with st.spinner("Generating AI insights..."):
                    # Prepare context
                    context = {
                        "Total Storage (TB)": f"{total_storage_tb:.2f}",
                        "Monthly Cost": f"${total_storage_cost:,.2f}",
                        "Tables Needing Attention": len(table_insights) if not table_insights.empty else 0,
                        "Storage Growth": f"{growth_pct:.1f}%",
                        "Top Database": storage_metrics.iloc[0]['DATABASE_NAME'] if not storage_metrics.empty else "N/A"
                    }

                    insight = ai_insights.generate_insight(
                        str(context),
                        "Analyze the storage metrics and provide 2-3 specific, actionable recommendations for optimization."
                    )

                    st.markdown(f'<div class="insight-card">{insight}</div>', unsafe_allow_html=True)
            else:
                st.warning("AI insights require Snowflake Cortex Complete access")

        except Exception as e:
            st.warning(f"AI insights temporarily unavailable: {str(e)}")

        # Quick actions
        st.markdown("---")
        st.markdown("#### ⚡ Quick Actions")

        st.markdown("""
        **Storage Optimization Commands:**

        ```sql
        -- Reduce Time Travel retention
        ALTER TABLE <table_name>
        SET DATA_RETENTION_TIME_IN_DAYS = 1;

        -- Drop unused table
        DROP TABLE IF EXISTS <table_name>;

        -- Create external table (cheaper storage)
        CREATE EXTERNAL TABLE <table_name>
        LOCATION = @<stage_name>;

        -- Check table clustering
        SELECT SYSTEM$CLUSTERING_INFORMATION('<table_name>');
        ```
        """)

# Footer
st.markdown("---")
st.caption(f"📅 Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | ⏱️ Time period: {time_period} days | 💵 Storage cost: ${storage_cost}/TB/month")
