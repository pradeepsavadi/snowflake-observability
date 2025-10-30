"""
Snowflake Holistic Observability Dashboard - Data Quality Page
===============================================================
Monitor data freshness, schema changes, and data consistency
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
    page_title="Data Quality - Snowflake Observability",
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom styling
apply_custom_css()

# Initialize session state and render settings
initialize_session_state()
render_settings_sidebar()

# Page header
render_page_header("✅ Data Quality Monitoring", "Track data freshness, schema changes, and table health")

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

# ============================================================================
# DATA QUALITY OVERVIEW
# ============================================================================

st.markdown("---")
st.subheader("📊 Data Quality Overview")

col1, col2, col3, col4 = st.columns(4)

with st.spinner("Loading data quality metrics..."):
    try:
        # Get table counts
        table_count_query = """
        SELECT
            COUNT(*) AS TOTAL_TABLES,
            SUM(CASE WHEN ROW_COUNT = 0 THEN 1 ELSE 0 END) AS EMPTY_TABLES,
            SUM(ROW_COUNT) AS TOTAL_ROWS,
            SUM(BYTES) AS TOTAL_BYTES
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
        WHERE DELETED IS NULL
        AND TABLE_TYPE = 'BASE TABLE'
        """
        table_stats = session.sql(table_count_query).to_pandas().iloc[0]

        total_tables = int(table_stats['TOTAL_TABLES'])
        empty_tables = int(table_stats['EMPTY_TABLES'])
        total_rows = int(table_stats['TOTAL_ROWS'])

        # Get stale tables (not updated in 30 days)
        stale_tables_query = f"""
        WITH table_updates AS (
            SELECT
                OBJECT_NAME,
                MAX(QUERY_START_TIME) AS LAST_UPDATE
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
            WHERE QUERY_START_TIME >= DATEADD(DAY, -{time_period * 2}, CURRENT_DATE())
            AND OBJECTS_MODIFIED IS NOT NULL
            GROUP BY OBJECT_NAME
        )
        SELECT COUNT(*) AS STALE_TABLES
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES t
        LEFT JOIN table_updates u ON t.TABLE_NAME = u.OBJECT_NAME
        WHERE t.DELETED IS NULL
        AND t.TABLE_TYPE = 'BASE TABLE'
        AND (u.LAST_UPDATE IS NULL OR u.LAST_UPDATE < DATEADD(DAY, -30, CURRENT_DATE()))
        """

        try:
            stale_tables = session.sql(stale_tables_query).to_pandas()['STALE_TABLES'].iloc[0]
        except:
            stale_tables = 0

        # Get schema changes count
        schema_changes_query = f"""
        SELECT COUNT(*) AS SCHEMA_CHANGES
        FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
        WHERE DELETED IS NULL
        AND (CREATED >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
             OR LAST_ALTERED >= DATEADD(DAY, -{time_period}, CURRENT_DATE()))
        """
        schema_changes = session.sql(schema_changes_query).to_pandas()['SCHEMA_CHANGES'].iloc[0]

        with col1:
            st.metric(
                "Total Tables",
                format_number(total_tables),
                help="Active base tables in account"
            )

        with col2:
            empty_pct = (empty_tables / max(total_tables, 1) * 100)
            st.metric(
                "Empty Tables",
                format_number(empty_tables),
                delta=f"{empty_pct:.1f}%",
                delta_color="inverse",
                help="Tables with zero rows"
            )

        with col3:
            st.metric(
                "Stale Tables",
                format_number(int(stale_tables)),
                delta_color="inverse",
                help="Tables not updated in 30+ days"
            )

        with col4:
            st.metric(
                "Schema Changes",
                format_number(int(schema_changes)),
                help=f"Column changes in last {time_period} days"
            )

        # Quality indicators
        st.markdown("---")

        quality_issues = []

        if empty_pct > 20:
            quality_issues.append(f"High percentage of empty tables ({empty_pct:.1f}%)")

        if stale_tables > 10:
            quality_issues.append(f"{int(stale_tables)} stale tables detected")

        if quality_issues:
            for issue in quality_issues:
                create_alert_badge(f"⚠️ {issue}", "warning")
        else:
            create_alert_badge("✅ No major data quality issues detected", "success")

    except Exception as e:
        st.error(f"Error loading data quality overview: {str(e)}")

# ============================================================================
# DATA QUALITY TABS
# ============================================================================

tab1, tab2, tab3, tab4 = st.tabs([
    "📅 Data Freshness",
    "🔧 Schema Changes",
    "📋 Table Health",
    "💡 Quality Recommendations"
])

# ----------------------------------------------------------------------------
# TAB 1: Data Freshness
# ----------------------------------------------------------------------------

with tab1:
    st.markdown("### 📅 Data Freshness Monitoring")

    try:
        # Table last update times
        freshness_query = f"""
        WITH table_updates AS (
            SELECT
                OBJECTS_MODIFIED[0]:objectName::STRING AS TABLE_NAME,
                OBJECTS_MODIFIED[0]:objectDomain::STRING AS OBJECT_TYPE,
                MAX(QUERY_START_TIME) AS LAST_UPDATE,
                COUNT(DISTINCT QUERY_ID) AS UPDATE_COUNT
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
            WHERE QUERY_START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            AND OBJECTS_MODIFIED IS NOT NULL
            AND ARRAY_SIZE(OBJECTS_MODIFIED) > 0
            GROUP BY TABLE_NAME, OBJECT_TYPE
        )
        SELECT
            t.TABLE_CATALOG AS DATABASE_NAME,
            t.TABLE_SCHEMA AS SCHEMA_NAME,
            t.TABLE_NAME,
            t.ROW_COUNT,
            t.BYTES,
            u.LAST_UPDATE,
            u.UPDATE_COUNT,
            DATEDIFF('hour', u.LAST_UPDATE, CURRENT_TIMESTAMP()) AS HOURS_SINCE_UPDATE,
            t.CREATED AS TABLE_CREATED
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES t
        LEFT JOIN table_updates u ON t.TABLE_NAME = u.TABLE_NAME
        WHERE t.DELETED IS NULL
        AND t.TABLE_TYPE = 'BASE TABLE'
        AND t.ROW_COUNT > 0
        ORDER BY HOURS_SINCE_UPDATE DESC NULLS FIRST
        LIMIT 100
        """

        freshness_data = session.sql(freshness_query).to_pandas()

        if not freshness_data.empty:
            freshness_data['LAST_UPDATE'] = pd.to_datetime(freshness_data['LAST_UPDATE'])
            freshness_data['TABLE_CREATED'] = pd.to_datetime(freshness_data['TABLE_CREATED'])

            # Categorize tables by freshness
            now = pd.Timestamp.now()

            def categorize_freshness(hours):
                if pd.isna(hours):
                    return 'Never Updated'
                elif hours <= 24:
                    return 'Fresh (<24h)'
                elif hours <= 168:  # 7 days
                    return 'Recent (1-7d)'
                elif hours <= 720:  # 30 days
                    return 'Aging (7-30d)'
                else:
                    return 'Stale (>30d)'

            freshness_data['FRESHNESS_CATEGORY'] = freshness_data['HOURS_SINCE_UPDATE'].apply(categorize_freshness)

            # Freshness distribution
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Data Freshness Distribution")

                freshness_dist = freshness_data['FRESHNESS_CATEGORY'].value_counts().reset_index()
                freshness_dist.columns = ['Category', 'Count']

                # Define color mapping
                color_map = {
                    'Fresh (<24h)': 'green',
                    'Recent (1-7d)': 'lightgreen',
                    'Aging (7-30d)': 'orange',
                    'Stale (>30d)': 'red',
                    'Never Updated': 'gray'
                }

                fig = px.pie(
                    freshness_dist,
                    values='Count',
                    names='Category',
                    title='Table Freshness Categories',
                    color='Category',
                    color_discrete_map=color_map
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("#### Freshness Metrics")

                for _, row in freshness_dist.iterrows():
                    pct = (row['Count'] / len(freshness_data) * 100)
                    st.metric(row['Category'], f"{int(row['Count'])} ({pct:.1f}%)")

            # Stale tables
            st.markdown("---")
            st.markdown("#### Stale Tables (>30 days)")

            stale_data = freshness_data[freshness_data['FRESHNESS_CATEGORY'] == 'Stale (>30d)'].copy()

            if not stale_data.empty:
                create_alert_badge(f"⚠️ {len(stale_data)} stale table(s) detected", "warning")

                stale_data['TABLE_PATH'] = stale_data['DATABASE_NAME'] + '.' + stale_data['SCHEMA_NAME'] + '.' + stale_data['TABLE_NAME']
                stale_data['DAYS_SINCE_UPDATE'] = (stale_data['HOURS_SINCE_UPDATE'] / 24).round(1)

                display_df = stale_data[[
                    'TABLE_PATH', 'ROW_COUNT', 'BYTES', 'LAST_UPDATE',
                    'DAYS_SINCE_UPDATE', 'TABLE_CREATED'
                ]].copy()

                display_df.columns = [
                    'Table', 'Rows', 'Size', 'Last Update',
                    'Days Since Update', 'Created'
                ]

                st.dataframe(
                    display_df.style.format({
                        'Rows': '{:,}',
                        'Size': lambda x: format_bytes(x),
                        'Last Update': lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) else 'Never',
                        'Days Since Update': '{:.1f}',
                        'Created': lambda x: x.strftime('%Y-%m-%d')
                    }).background_gradient(subset=['Days Since Update'], cmap='YlOrRd'),
                    use_container_width=True,
                    height=400
                )

            else:
                create_alert_badge("✅ No stale tables (>30 days) detected", "success")

            # Update frequency analysis
            st.markdown("---")
            st.markdown("#### Update Frequency Analysis")

            tables_with_updates = freshness_data[freshness_data['UPDATE_COUNT'].notna()].copy()

            if not tables_with_updates.empty:
                # Calculate updates per day
                tables_with_updates['UPDATES_PER_DAY'] = (
                    tables_with_updates['UPDATE_COUNT'] / time_period
                ).round(2)

                # Top 15 most frequently updated tables
                top_updated = tables_with_updates.nlargest(15, 'UPDATE_COUNT')
                top_updated['TABLE_PATH'] = top_updated['DATABASE_NAME'] + '.' + top_updated['SCHEMA_NAME'] + '.' + top_updated['TABLE_NAME']

                fig = px.bar(
                    top_updated,
                    x='TABLE_PATH',
                    y='UPDATE_COUNT',
                    title='Top 15 Most Frequently Updated Tables',
                    labels={'UPDATE_COUNT': 'Update Count', 'TABLE_PATH': 'Table'}
                )

                fig.update_traces(marker_color='steelblue')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)

                # Summary statistics
                col1, col2, col3 = st.columns(3)

                with col1:
                    avg_updates = tables_with_updates['UPDATE_COUNT'].mean()
                    st.metric("Avg Updates per Table", f"{avg_updates:.1f}")

                with col2:
                    max_updates = tables_with_updates['UPDATE_COUNT'].max()
                    st.metric("Max Updates", int(max_updates))

                with col3:
                    median_updates = tables_with_updates['UPDATE_COUNT'].median()
                    st.metric("Median Updates", int(median_updates))

        else:
            st.info("No freshness data available")

    except Exception as e:
        st.error(f"Error loading freshness data: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 2: Schema Changes
# ----------------------------------------------------------------------------

with tab2:
    st.markdown("### 🔧 Schema Change Tracking")

    try:
        # Recent column changes
        schema_changes_query = f"""
        SELECT
            TABLE_CATALOG AS DATABASE_NAME,
            TABLE_SCHEMA AS SCHEMA_NAME,
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CREATED,
            LAST_ALTERED,
            DELETED,
            COMMENT
        FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
        WHERE (CREATED >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
               OR LAST_ALTERED >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
               OR DELETED >= DATEADD(DAY, -{time_period}, CURRENT_DATE()))
        ORDER BY COALESCE(LAST_ALTERED, CREATED) DESC
        LIMIT 200
        """

        schema_changes = session.sql(schema_changes_query).to_pandas()

        if not schema_changes.empty:
            schema_changes['CREATED'] = pd.to_datetime(schema_changes['CREATED'])
            schema_changes['LAST_ALTERED'] = pd.to_datetime(schema_changes['LAST_ALTERED'])
            schema_changes['DELETED'] = pd.to_datetime(schema_changes['DELETED'])

            # Categorize changes
            def categorize_change(row):
                if pd.notna(row['DELETED']):
                    return 'DELETED'
                elif pd.notna(row['LAST_ALTERED']) and row['LAST_ALTERED'] > row['CREATED']:
                    return 'MODIFIED'
                else:
                    return 'ADDED'

            schema_changes['CHANGE_TYPE'] = schema_changes.apply(categorize_change, axis=1)

            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)

            added_count = len(schema_changes[schema_changes['CHANGE_TYPE'] == 'ADDED'])
            modified_count = len(schema_changes[schema_changes['CHANGE_TYPE'] == 'MODIFIED'])
            deleted_count = len(schema_changes[schema_changes['CHANGE_TYPE'] == 'DELETED'])

            with col1:
                st.metric("Total Changes", len(schema_changes))

            with col2:
                st.metric("Columns Added", added_count)

            with col3:
                st.metric("Columns Modified", modified_count)

            with col4:
                st.metric("Columns Deleted", deleted_count)

            # Change type distribution
            st.markdown("---")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Schema Change Distribution")

                change_dist = schema_changes['CHANGE_TYPE'].value_counts().reset_index()
                change_dist.columns = ['Change Type', 'Count']

                fig = px.pie(
                    change_dist,
                    values='Count',
                    names='Change Type',
                    title='Schema Changes by Type',
                    color='Change Type',
                    color_discrete_map={'ADDED': 'lightgreen', 'MODIFIED': 'orange', 'DELETED': 'salmon'}
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("#### Daily Schema Changes")

                schema_changes['CHANGE_DATE'] = schema_changes['LAST_ALTERED'].fillna(schema_changes['CREATED']).dt.date
                daily_changes = schema_changes.groupby('CHANGE_DATE').size().reset_index(name='COUNT')
                daily_changes['CHANGE_DATE'] = pd.to_datetime(daily_changes['CHANGE_DATE'])

                fig = px.bar(
                    daily_changes,
                    x='CHANGE_DATE',
                    y='COUNT',
                    title='Schema Changes Over Time',
                    labels={'COUNT': 'Change Count', 'CHANGE_DATE': 'Date'}
                )

                fig.update_traces(marker_color='steelblue')
                st.plotly_chart(fig, use_container_width=True)

            # Detailed changes table
            st.markdown("---")
            st.markdown("#### Recent Schema Changes")

            schema_changes['TABLE_PATH'] = schema_changes['DATABASE_NAME'] + '.' + schema_changes['SCHEMA_NAME'] + '.' + schema_changes['TABLE_NAME']
            schema_changes['CHANGE_DATE'] = schema_changes['LAST_ALTERED'].fillna(schema_changes['CREATED'])

            display_df = schema_changes[[
                'TABLE_PATH', 'COLUMN_NAME', 'DATA_TYPE', 'CHANGE_TYPE',
                'CHANGE_DATE', 'IS_NULLABLE'
            ]].copy()

            display_df.columns = [
                'Table', 'Column', 'Data Type', 'Change',
                'Date', 'Nullable'
            ]

            st.dataframe(
                display_df.style.format({
                    'Date': lambda x: x.strftime('%Y-%m-%d %H:%M')
                }),
                use_container_width=True,
                height=400
            )

            # Tables with most schema changes
            st.markdown("---")
            st.markdown("#### Tables with Most Schema Changes")

            table_change_counts = schema_changes.groupby('TABLE_PATH').agg({
                'COLUMN_NAME': 'count',
                'CHANGE_TYPE': lambda x: x.value_counts().to_dict()
            }).reset_index()

            table_change_counts.columns = ['Table', 'Change Count', 'Change Breakdown']
            table_change_counts = table_change_counts.sort_values('Change Count', ascending=False).head(15)

            fig = px.bar(
                table_change_counts,
                x='Table',
                y='Change Count',
                title='Tables with Most Schema Changes',
                labels={'Change Count': 'Number of Changes'}
            )

            fig.update_traces(marker_color='orange')
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        else:
            st.info(f"No schema changes detected in the last {time_period} days")

    except Exception as e:
        st.error(f"Error loading schema changes: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 3: Table Health
# ----------------------------------------------------------------------------

with tab3:
    st.markdown("### 📋 Table Health Metrics")

    try:
        # Table statistics
        table_health_query = """
        SELECT
            TABLE_CATALOG AS DATABASE_NAME,
            TABLE_SCHEMA AS SCHEMA_NAME,
            TABLE_NAME,
            TABLE_TYPE,
            ROW_COUNT,
            BYTES,
            CREATED,
            LAST_ALTERED,
            AUTO_CLUSTERING_ON,
            CLUSTERING_KEY,
            COMMENT
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
        WHERE DELETED IS NULL
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY BYTES DESC
        LIMIT 100
        """

        table_health = session.sql(table_health_query).to_pandas()

        if not table_health.empty:
            table_health['CREATED'] = pd.to_datetime(table_health['CREATED'])
            table_health['LAST_ALTERED'] = pd.to_datetime(table_health['LAST_ALTERED'])
            table_health['SIZE_GB'] = table_health['BYTES'] / (1024**3)

            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)

            empty_tables_count = len(table_health[table_health['ROW_COUNT'] == 0])
            clustered_tables = len(table_health[table_health['CLUSTERING_KEY'].notna()])
            auto_clustering = len(table_health[table_health['AUTO_CLUSTERING_ON'] == True])

            with col1:
                st.metric("Total Tables", len(table_health))

            with col2:
                empty_pct = (empty_tables_count / len(table_health) * 100)
                st.metric("Empty Tables", empty_tables_count, delta=f"{empty_pct:.1f}%", delta_color="inverse")

            with col3:
                clustered_pct = (clustered_tables / len(table_health) * 100)
                st.metric("Clustered Tables", clustered_tables, delta=f"{clustered_pct:.1f}%")

            with col4:
                st.metric("Auto-Clustering", auto_clustering)

            # Table size distribution
            st.markdown("---")
            st.markdown("#### Table Size Distribution")

            size_bins = [0, 0.1, 1, 10, 100, float('inf')]
            size_labels = ['<100 MB', '100 MB-1 GB', '1-10 GB', '10-100 GB', '>100 GB']
            table_health['SIZE_CATEGORY'] = pd.cut(table_health['SIZE_GB'], bins=size_bins, labels=size_labels)

            size_dist = table_health['SIZE_CATEGORY'].value_counts().reset_index()
            size_dist.columns = ['Category', 'Count']
            size_dist = size_dist.sort_values('Count', ascending=False)

            fig = px.bar(
                size_dist,
                x='Category',
                y='Count',
                title='Table Count by Size Category',
                labels={'Count': 'Number of Tables', 'Category': 'Size Category'}
            )

            fig.update_traces(marker_color='steelblue')
            st.plotly_chart(fig, use_container_width=True)

            # Large tables
            st.markdown("---")
            st.markdown("#### Largest Tables")

            large_tables = table_health.nlargest(20, 'BYTES').copy()
            large_tables['TABLE_PATH'] = large_tables['DATABASE_NAME'] + '.' + large_tables['SCHEMA_NAME'] + '.' + large_tables['TABLE_NAME']

            display_df = large_tables[[
                'TABLE_PATH', 'ROW_COUNT', 'SIZE_GB', 'CLUSTERING_KEY',
                'AUTO_CLUSTERING_ON', 'CREATED'
            ]].copy()

            display_df.columns = [
                'Table', 'Rows', 'Size (GB)', 'Clustering Key',
                'Auto-Clustering', 'Created'
            ]

            st.dataframe(
                display_df.style.format({
                    'Rows': '{:,}',
                    'Size (GB)': '{:.2f}',
                    'Created': lambda x: x.strftime('%Y-%m-%d')
                }).background_gradient(subset=['Size (GB)'], cmap='Blues'),
                use_container_width=True,
                height=400
            )

            # Empty tables
            st.markdown("---")
            st.markdown("#### Empty Tables")

            empty_tables_df = table_health[table_health['ROW_COUNT'] == 0].copy()

            if not empty_tables_df.empty:
                create_alert_badge(f"⚠️ {len(empty_tables_df)} empty table(s) detected", "warning")

                empty_tables_df['TABLE_PATH'] = empty_tables_df['DATABASE_NAME'] + '.' + empty_tables_df['SCHEMA_NAME'] + '.' + empty_tables_df['TABLE_NAME']

                display_df = empty_tables_df[['TABLE_PATH', 'CREATED', 'LAST_ALTERED']].copy()
                display_df.columns = ['Table', 'Created', 'Last Altered']

                st.dataframe(
                    display_df.style.format({
                        'Created': lambda x: x.strftime('%Y-%m-%d'),
                        'Last Altered': lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else 'Never'
                    }),
                    use_container_width=True,
                    height=300
                )

                st.caption("**Recommendation:** Consider dropping unused empty tables to reduce clutter")

            else:
                create_alert_badge("✅ No empty tables detected", "success")

            # Clustering recommendations
            st.markdown("---")
            st.markdown("#### Clustering Recommendations")

            large_unclustered = table_health[
                (table_health['SIZE_GB'] > 1) &
                (table_health['CLUSTERING_KEY'].isna())
            ].copy()

            if not large_unclustered.empty:
                create_alert_badge(
                    f"💡 {len(large_unclustered)} large table(s) without clustering keys",
                    "info"
                )

                large_unclustered['TABLE_PATH'] = large_unclustered['DATABASE_NAME'] + '.' + large_unclustered['SCHEMA_NAME'] + '.' + large_unclustered['TABLE_NAME']

                display_df = large_unclustered[['TABLE_PATH', 'ROW_COUNT', 'SIZE_GB']].head(15).copy()
                display_df.columns = ['Table', 'Rows', 'Size (GB)']

                st.dataframe(
                    display_df.style.format({
                        'Rows': '{:,}',
                        'Size (GB)': '{:.2f}'
                    }),
                    use_container_width=True
                )

                st.caption("**Recommendation:** Consider adding clustering keys to large tables to improve query performance")

            else:
                st.success("All large tables have clustering keys configured")

        else:
            st.info("No table health data available")

    except Exception as e:
        st.error(f"Error loading table health data: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 4: Quality Recommendations
# ----------------------------------------------------------------------------

with tab4:
    st.markdown("### 💡 Data Quality Recommendations")

    col1, col2 = st.columns([2, 1])

    with col1:
        recommendations = []

        try:
            # 1. Stale data recommendation
            if 'stale_data' in locals() and not stale_data.empty:
                recommendations.append({
                    'priority': 'HIGH',
                    'category': 'Stale Data',
                    'issue': f"{len(stale_data)} table(s) not updated in 30+ days",
                    'impact': "Data may be outdated, affecting analytics and decision-making",
                    'action': """
                    **Investigate and Remediate:**

                    1. Review pipeline health for stale tables
                    2. Verify data sources are still active
                    3. Check for broken ETL processes
                    4. Consider archiving or dropping unused tables

                    **Monitoring:**
                    ```sql
                    -- Set up freshness alerts
                    SELECT
                        TABLE_NAME,
                        DATEDIFF('day', MAX(LAST_ALTERED), CURRENT_DATE()) AS DAYS_STALE
                    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    AND DELETED IS NULL
                    GROUP BY TABLE_NAME
                    HAVING DAYS_STALE > 30;
                    ```
                    """
                })

            # 2. Empty tables recommendation
            if 'empty_tables_df' in locals() and not empty_tables_df.empty:
                recommendations.append({
                    'priority': 'MEDIUM',
                    'category': 'Empty Tables',
                    'issue': f"{len(empty_tables_df)} empty table(s) found",
                    'impact': "Wasted storage and unnecessary complexity",
                    'action': """
                    **Cleanup Actions:**

                    1. Identify purpose of empty tables
                    2. Drop tables created by mistake:
                    ```sql
                    DROP TABLE IF EXISTS <database>.<schema>.<table_name>;
                    ```

                    3. Keep empty tables that are intentionally waiting for data
                    4. Document retention policy for empty tables
                    5. Implement automated cleanup process

                    **Best Practice:** Use transient or temporary tables for staging
                    """
                })

            # 3. Schema change management
            if 'schema_changes' in locals() and len(schema_changes) > 50:
                recommendations.append({
                    'priority': 'MEDIUM',
                    'category': 'Schema Changes',
                    'issue': f"{len(schema_changes)} schema changes in last {time_period} days",
                    'impact': "Frequent schema changes may indicate design issues",
                    'action': """
                    **Schema Management Best Practices:**

                    1. Implement schema version control
                    2. Use change management process
                    3. Document all schema changes
                    4. Test schema changes in dev/staging first
                    5. Consider using ALTER TABLE ADD COLUMN instead of modifying existing columns

                    **Schema Evolution Pattern:**
                    ```sql
                    -- Add new column (non-breaking)
                    ALTER TABLE my_table ADD COLUMN new_col VARCHAR;

                    -- Instead of changing type (breaking)
                    -- Create new column, migrate data, drop old
                    ALTER TABLE my_table ADD COLUMN col_v2 INT;
                    UPDATE my_table SET col_v2 = TRY_CAST(col_v1 AS INT);
                    ALTER TABLE my_table DROP COLUMN col_v1;
                    ALTER TABLE my_table RENAME COLUMN col_v2 TO col_v1;
                    ```
                    """
                })

            # 4. Clustering recommendation
            if 'large_unclustered' in locals() and not large_unclustered.empty:
                recommendations.append({
                    'priority': 'LOW',
                    'category': 'Table Clustering',
                    'issue': f"{len(large_unclustered)} large table(s) without clustering",
                    'impact': "Suboptimal query performance on large tables",
                    'action': """
                    **Clustering Strategy:**

                    1. Identify frequently filtered columns
                    2. Add clustering keys:
                    ```sql
                    ALTER TABLE my_large_table
                    CLUSTER BY (date_column, category_column);
                    ```

                    3. Enable automatic clustering for large, frequently updated tables:
                    ```sql
                    ALTER TABLE my_large_table
                    RESUME RECLUSTER;
                    ```

                    4. Monitor clustering effectiveness:
                    ```sql
                    SELECT SYSTEM$CLUSTERING_INFORMATION('my_table');
                    ```

                    **Best Practices:**
                    - Cluster on columns used in WHERE clauses
                    - Use 1-4 columns for clustering key
                    - Monitor reclustering costs vs performance benefits
                    """
                })

            # 5. Data quality checks
            recommendations.append({
                'priority': 'LOW',
                'category': 'Data Quality Monitoring',
                'issue': "Proactive data quality monitoring not configured",
                'impact': "Data issues may go undetected",
                'action': """
                **Implement Data Quality Framework:**

                1. **Null Checks:**
                ```sql
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT_IF(critical_column IS NULL) AS null_count,
                    (null_count / total_rows * 100) AS null_pct
                FROM my_table;
                ```

                2. **Duplicate Detection:**
                ```sql
                SELECT
                    id,
                    COUNT(*) AS duplicate_count
                FROM my_table
                GROUP BY id
                HAVING COUNT(*) > 1;
                ```

                3. **Range Validation:**
                ```sql
                SELECT COUNT(*)
                FROM my_table
                WHERE amount < 0  -- Invalid negative amounts
                OR date_column > CURRENT_DATE();  -- Future dates
                ```

                4. **Referential Integrity:**
                ```sql
                SELECT COUNT(*)
                FROM orders o
                LEFT JOIN customers c ON o.customer_id = c.id
                WHERE c.id IS NULL;  -- Orphaned orders
                ```

                **Automation:** Implement these checks as tasks or stored procedures
                """
            })

            # Display recommendations
            if recommendations:
                for i, rec in enumerate(recommendations, 1):
                    priority_colors = {
                        'HIGH': '🔴',
                        'MEDIUM': '🟡',
                        'LOW': '🟢'
                    }

                    with st.expander(
                        f"{priority_colors.get(rec['priority'], '🔵')} {rec['category']} - {rec['issue']}",
                        expanded=(rec['priority'] == 'HIGH')
                    ):
                        st.markdown(f"**Priority:** {rec['priority']}")
                        st.markdown(f"**Issue:** {rec['issue']}")
                        st.markdown(f"**Impact:** {rec['impact']}")
                        st.markdown(f"**Recommended Actions:**")
                        st.info(rec['action'])

            else:
                create_alert_badge("✅ No data quality issues identified", "success")

        except Exception as e:
            st.error(f"Error generating recommendations: {str(e)}")

    with col2:
        st.markdown("#### 📊 Quality Metrics")

        if 'freshness_data' in locals() and not freshness_data.empty:
            fresh_count = len(freshness_data[freshness_data['FRESHNESS_CATEGORY'] == 'Fresh (<24h)'])
            fresh_pct = (fresh_count / len(freshness_data) * 100)

            st.metric("Fresh Data %", f"{fresh_pct:.1f}%")

        if 'table_health' in locals() and not table_health.empty:
            clustered_pct = (clustered_tables / len(table_health) * 100) if 'clustered_tables' in locals() else 0
            st.metric("Clustered %", f"{clustered_pct:.1f}%")

        st.markdown("---")
        st.markdown("#### ✅ Quality Checklist")

        st.markdown("""
        **Data Freshness:**
        - [ ] All critical tables updated daily
        - [ ] Stale data alerts configured
        - [ ] Pipeline monitoring in place

        **Schema Management:**
        - [ ] Change control process
        - [ ] Version control for schemas
        - [ ] Documentation updated

        **Table Optimization:**
        - [ ] Large tables clustered
        - [ ] Empty tables cleaned up
        - [ ] Partitioning strategy

        **Quality Checks:**
        - [ ] Null value monitoring
        - [ ] Duplicate detection
        - [ ] Referential integrity
        - [ ] Range validation

        **Monitoring:**
        - [ ] Automated quality checks
        - [ ] Alerting configured
        - [ ] Regular quality reports
        """)

# Footer
st.markdown("---")
st.caption(f"📅 Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | ⏱️ Time period: {time_period} days")
