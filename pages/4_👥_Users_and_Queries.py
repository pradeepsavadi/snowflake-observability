"""
Snowflake Holistic Observability Dashboard - Users and Queries Page
===================================================================
Analyze user activity, query patterns, and performance by user
"""

import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
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
    page_title="Users & Queries - Snowflake Observability",
    page_icon="👥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom styling
apply_custom_css()

# Initialize session state and render settings
initialize_session_state()
render_settings_sidebar()

# Page header
render_page_header("👥 Users & Query Analytics", "Monitor user activity, query patterns, and resource consumption")

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
credit_cost = st.session_state.credit_cost

# ============================================================================
# USER OVERVIEW
# ============================================================================

st.markdown("---")
st.subheader("📊 User Activity Overview")

col1, col2, col3, col4 = st.columns(4)

with st.spinner("Loading user metrics..."):
    try:
        # Get user statistics
        user_stats_query = f"""
        SELECT
            COUNT(DISTINCT USER_NAME) AS TOTAL_USERS,
            COUNT(DISTINCT QUERY_ID) AS TOTAL_QUERIES,
            SUM(EXECUTION_TIME) / 1000 / 3600 AS TOTAL_EXEC_HOURS,
            SUM(CREDITS_USED_CLOUD_SERVICES) AS TOTAL_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        """
        user_stats = session.sql(user_stats_query).to_pandas().iloc[0]

        total_users = int(user_stats['TOTAL_USERS'])
        total_queries = int(user_stats['TOTAL_QUERIES'])
        total_exec_hours = user_stats['TOTAL_EXEC_HOURS']
        total_credits = user_stats['TOTAL_CREDITS']

        # Get active users (queried in last 7 days)
        active_users_query = f"""
        SELECT COUNT(DISTINCT USER_NAME) AS ACTIVE_USERS
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_DATE())
        """
        active_users = session.sql(active_users_query).to_pandas()['ACTIVE_USERS'].iloc[0]

        with col1:
            st.metric(
                "Total Users",
                format_number(total_users),
                help=f"Users who ran queries in last {time_period} days"
            )

        with col2:
            st.metric(
                "Active Users (7d)",
                format_number(int(active_users)),
                help="Users active in last 7 days"
            )

        with col3:
            st.metric(
                "Total Queries",
                format_number(total_queries),
                help=f"Queries executed in last {time_period} days"
            )

        with col4:
            queries_per_user = total_queries / max(total_users, 1)
            st.metric(
                "Queries per User",
                f"{queries_per_user:,.0f}",
                help="Average queries per user"
            )

    except Exception as e:
        st.error(f"Error loading user overview: {str(e)}")

# ============================================================================
# USER TABS
# ============================================================================

tab1, tab2, tab3, tab4 = st.tabs([
    "👤 User Analytics",
    "📊 Query Patterns",
    "💰 Cost Attribution",
    "🔍 User Deep Dive"
])

# ----------------------------------------------------------------------------
# TAB 1: User Analytics
# ----------------------------------------------------------------------------

with tab1:
    st.markdown("### 👤 User Activity Analysis")

    try:
        # Get detailed user metrics
        user_metrics_query = f"""
        SELECT
            USER_NAME,
            COUNT(DISTINCT QUERY_ID) AS QUERY_COUNT,
            COUNT(DISTINCT DATE(START_TIME)) AS ACTIVE_DAYS,
            AVG(EXECUTION_TIME) / 1000 AS AVG_EXEC_TIME_SEC,
            SUM(EXECUTION_TIME) / 1000 / 3600 AS TOTAL_EXEC_HOURS,
            SUM(BYTES_SCANNED) AS TOTAL_BYTES_SCANNED,
            SUM(ROWS_PRODUCED) AS TOTAL_ROWS_PRODUCED,
            SUM(CREDITS_USED_CLOUD_SERVICES) AS TOTAL_CREDITS,
            COUNT(CASE WHEN ERROR_CODE IS NOT NULL THEN 1 END) AS ERROR_COUNT,
            MAX(START_TIME) AS LAST_QUERY_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        GROUP BY USER_NAME
        ORDER BY QUERY_COUNT DESC
        """
        user_metrics = session.sql(user_metrics_query).to_pandas()

        if not user_metrics.empty:
            user_metrics['LAST_QUERY_TIME'] = pd.to_datetime(user_metrics['LAST_QUERY_TIME'])
            user_metrics['COST'] = user_metrics['TOTAL_CREDITS'] * credit_cost
            user_metrics['ERROR_RATE'] = (user_metrics['ERROR_COUNT'] / user_metrics['QUERY_COUNT'] * 100).round(2)

            # Summary cards
            col1, col2, col3 = st.columns(3)

            with col1:
                top_user = user_metrics.iloc[0]
                st.metric(
                    "Most Active User",
                    top_user['USER_NAME'],
                    delta=f"{int(top_user['QUERY_COUNT']):,} queries"
                )

            with col2:
                costly_user = user_metrics.loc[user_metrics['COST'].idxmax()]
                st.metric(
                    "Highest Cost User",
                    costly_user['USER_NAME'],
                    delta=f"${costly_user['COST']:.2f}"
                )

            with col3:
                avg_error_rate = user_metrics['ERROR_RATE'].mean()
                st.metric(
                    "Avg Error Rate",
                    f"{avg_error_rate:.2f}%",
                    delta_color="inverse"
                )

            st.markdown("---")

            # User metrics table
            st.markdown("#### User Activity Summary")

            # Filter options
            col1, col2, col3 = st.columns(3)

            with col1:
                min_queries = st.number_input("Min Queries", min_value=0, value=0, step=10)

            with col2:
                sort_by = st.selectbox(
                    "Sort By",
                    options=['QUERY_COUNT', 'COST', 'TOTAL_BYTES_SCANNED', 'ERROR_RATE'],
                    format_func=lambda x: {
                        'QUERY_COUNT': 'Query Count',
                        'COST': 'Cost',
                        'TOTAL_BYTES_SCANNED': 'Bytes Scanned',
                        'ERROR_RATE': 'Error Rate'
                    }[x]
                )

            with col3:
                show_inactive = st.checkbox("Show Inactive Users (>7 days)", value=False)

            # Apply filters
            filtered_users = user_metrics[user_metrics['QUERY_COUNT'] >= min_queries].copy()

            if not show_inactive:
                seven_days_ago = pd.Timestamp.now() - pd.Timedelta(days=7)
                filtered_users = filtered_users[filtered_users['LAST_QUERY_TIME'] >= seven_days_ago]

            filtered_users = filtered_users.sort_values(sort_by, ascending=False)

            # Display table
            display_df = filtered_users[[
                'USER_NAME', 'QUERY_COUNT', 'ACTIVE_DAYS', 'AVG_EXEC_TIME_SEC',
                'TOTAL_BYTES_SCANNED', 'COST', 'ERROR_RATE', 'LAST_QUERY_TIME'
            ]].copy()

            display_df.columns = [
                'User', 'Queries', 'Active Days', 'Avg Exec (s)',
                'Bytes Scanned', 'Cost ($)', 'Error Rate (%)', 'Last Active'
            ]

            st.dataframe(
                display_df.style.format({
                    'Queries': '{:,}',
                    'Active Days': '{:,}',
                    'Avg Exec (s)': '{:.2f}',
                    'Bytes Scanned': lambda x: format_bytes(x),
                    'Cost ($)': '${:,.2f}',
                    'Error Rate (%)': '{:.2f}%',
                    'Last Active': lambda x: x.strftime('%Y-%m-%d %H:%M')
                }).background_gradient(subset=['Queries'], cmap='Blues'),
                use_container_width=True,
                height=400
            )

            # Visualizations
            st.markdown("---")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Top 10 Users by Query Count")

                top_10_users = user_metrics.head(10)

                fig = px.bar(
                    top_10_users,
                    x='QUERY_COUNT',
                    y='USER_NAME',
                    orientation='h',
                    title='Top 10 Most Active Users',
                    labels={'QUERY_COUNT': 'Query Count', 'USER_NAME': 'User'},
                    color='QUERY_COUNT',
                    color_continuous_scale='Blues'
                )

                fig.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("#### Top 10 Users by Cost")

                top_10_costly = user_metrics.nlargest(10, 'COST')

                fig = px.bar(
                    top_10_costly,
                    x='COST',
                    y='USER_NAME',
                    orientation='h',
                    title='Top 10 Users by Cost',
                    labels={'COST': 'Cost ($)', 'USER_NAME': 'User'},
                    color='COST',
                    color_continuous_scale='Reds'
                )

                fig.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig, use_container_width=True)

            # User activity timeline
            st.markdown("---")
            st.markdown("#### Daily User Activity")

            daily_users_query = f"""
            SELECT
                DATE(START_TIME) AS ACTIVITY_DATE,
                COUNT(DISTINCT USER_NAME) AS UNIQUE_USERS,
                COUNT(DISTINCT QUERY_ID) AS QUERY_COUNT
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            GROUP BY ACTIVITY_DATE
            ORDER BY ACTIVITY_DATE
            """
            daily_users = session.sql(daily_users_query).to_pandas()

            if not daily_users.empty:
                daily_users['ACTIVITY_DATE'] = pd.to_datetime(daily_users['ACTIVITY_DATE'])

                fig = go.Figure()

                fig.add_trace(go.Scatter(
                    x=daily_users['ACTIVITY_DATE'],
                    y=daily_users['UNIQUE_USERS'],
                    mode='lines+markers',
                    name='Unique Users',
                    line=dict(color='steelblue', width=3),
                    yaxis='y'
                ))

                fig.add_trace(go.Scatter(
                    x=daily_users['ACTIVITY_DATE'],
                    y=daily_users['QUERY_COUNT'],
                    mode='lines+markers',
                    name='Query Count',
                    line=dict(color='lightcoral', width=3),
                    yaxis='y2'
                ))

                fig.update_layout(
                    title="Daily User Activity Trend",
                    xaxis_title="Date",
                    yaxis=dict(title="Unique Users"),
                    yaxis2=dict(title="Query Count", overlaying='y', side='right'),
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)

        else:
            st.info("No user activity data available")

    except Exception as e:
        st.error(f"Error loading user analytics: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 2: Query Patterns
# ----------------------------------------------------------------------------

with tab2:
    st.markdown("### 📊 Query Pattern Analysis")

    try:
        # Query patterns by user
        query_patterns_query = f"""
        SELECT
            USER_NAME,
            QUERY_TYPE,
            COUNT(*) AS QUERY_COUNT,
            AVG(EXECUTION_TIME) / 1000 AS AVG_EXEC_TIME_SEC,
            SUM(BYTES_SCANNED) AS TOTAL_BYTES_SCANNED
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        AND QUERY_TYPE IS NOT NULL
        GROUP BY USER_NAME, QUERY_TYPE
        ORDER BY USER_NAME, QUERY_COUNT DESC
        """
        query_patterns = session.sql(query_patterns_query).to_pandas()

        if not query_patterns.empty:
            # Query type distribution
            st.markdown("#### Query Type Distribution Across Users")

            # Pivot for heatmap
            pivot_data = query_patterns.pivot_table(
                index='USER_NAME',
                columns='QUERY_TYPE',
                values='QUERY_COUNT',
                fill_value=0
            )

            # Top 20 users for readability
            top_users = query_patterns.groupby('USER_NAME')['QUERY_COUNT'].sum().nlargest(20).index
            pivot_data = pivot_data.loc[pivot_data.index.isin(top_users)]

            fig = px.imshow(
                pivot_data,
                labels=dict(x="Query Type", y="User", color="Query Count"),
                title="Query Type Heatmap (Top 20 Users)",
                aspect="auto",
                color_continuous_scale='Blues'
            )

            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

            # Query type breakdown
            st.markdown("---")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Overall Query Type Distribution")

                type_distribution = query_patterns.groupby('QUERY_TYPE')['QUERY_COUNT'].sum().reset_index()
                type_distribution = type_distribution.sort_values('QUERY_COUNT', ascending=False)

                fig = px.pie(
                    type_distribution,
                    values='QUERY_COUNT',
                    names='QUERY_TYPE',
                    title='Query Types',
                    hole=0.4
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("#### Avg Execution Time by Query Type")

                avg_exec_by_type = query_patterns.groupby('QUERY_TYPE')['AVG_EXEC_TIME_SEC'].mean().reset_index()
                avg_exec_by_type = avg_exec_by_type.sort_values('AVG_EXEC_TIME_SEC', ascending=False)

                fig = px.bar(
                    avg_exec_by_type,
                    x='QUERY_TYPE',
                    y='AVG_EXEC_TIME_SEC',
                    title='Average Execution Time',
                    labels={'AVG_EXEC_TIME_SEC': 'Avg Exec Time (s)', 'QUERY_TYPE': 'Type'}
                )

                fig.update_traces(marker_color='steelblue')
                st.plotly_chart(fig, use_container_width=True)

            # Failed queries by user
            st.markdown("---")
            st.markdown("#### Failed Queries by User")

            failed_queries_query = f"""
            SELECT
                USER_NAME,
                ERROR_CODE,
                ERROR_MESSAGE,
                COUNT(*) AS ERROR_COUNT
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            AND ERROR_CODE IS NOT NULL
            GROUP BY USER_NAME, ERROR_CODE, ERROR_MESSAGE
            ORDER BY ERROR_COUNT DESC
            LIMIT 20
            """
            failed_queries = session.sql(failed_queries_query).to_pandas()

            if not failed_queries.empty:
                failed_queries['ERROR_MESSAGE_SHORT'] = failed_queries['ERROR_MESSAGE'].str[:80] + '...'

                display_df = failed_queries[['USER_NAME', 'ERROR_CODE', 'ERROR_MESSAGE_SHORT', 'ERROR_COUNT']].copy()
                display_df.columns = ['User', 'Error Code', 'Error Message', 'Count']

                st.dataframe(
                    display_df.style.format({'Count': '{:,}'}),
                    use_container_width=True,
                    height=300
                )

                # Error distribution by user
                error_by_user = failed_queries.groupby('USER_NAME')['ERROR_COUNT'].sum().reset_index()
                error_by_user = error_by_user.sort_values('ERROR_COUNT', ascending=False).head(10)

                fig = px.bar(
                    error_by_user,
                    x='USER_NAME',
                    y='ERROR_COUNT',
                    title='Top 10 Users by Error Count',
                    labels={'ERROR_COUNT': 'Error Count', 'USER_NAME': 'User'}
                )

                fig.update_traces(marker_color='salmon')
                st.plotly_chart(fig, use_container_width=True)

                if len(failed_queries) > 0:
                    create_alert_badge(
                        f"⚠️ {len(failed_queries)} different error patterns detected",
                        "warning"
                    )
            else:
                create_alert_badge("✅ No query errors in selected period", "success")

        else:
            st.info("No query pattern data available")

    except Exception as e:
        st.error(f"Error loading query patterns: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 3: Cost Attribution
# ----------------------------------------------------------------------------

with tab3:
    st.markdown("### 💰 Cost Attribution by User")

    try:
        # User cost breakdown
        user_cost_query = f"""
        WITH user_compute_cost AS (
            SELECT
                USER_NAME,
                SUM(CREDITS_USED_CLOUD_SERVICES) AS CLOUD_CREDITS,
                COUNT(DISTINCT QUERY_ID) AS QUERY_COUNT,
                SUM(EXECUTION_TIME) / 1000 / 3600 AS EXEC_HOURS,
                SUM(BYTES_SCANNED) AS BYTES_SCANNED
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            GROUP BY USER_NAME
        )
        SELECT
            USER_NAME,
            CLOUD_CREDITS,
            CLOUD_CREDITS * {credit_cost} AS CLOUD_COST,
            QUERY_COUNT,
            EXEC_HOURS,
            BYTES_SCANNED,
            (CLOUD_CREDITS * {credit_cost}) / NULLIF(QUERY_COUNT, 0) AS COST_PER_QUERY
        FROM user_compute_cost
        ORDER BY CLOUD_COST DESC
        """
        user_costs = session.sql(user_cost_query).to_pandas()

        if not user_costs.empty:
            total_cloud_cost = user_costs['CLOUD_COST'].sum()
            user_costs['COST_PCT'] = (user_costs['CLOUD_COST'] / total_cloud_cost * 100).round(2)

            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Total User Cost", f"${total_cloud_cost:,.2f}")

            with col2:
                top_user_cost = user_costs.iloc[0]['CLOUD_COST']
                st.metric("Top User Cost", f"${top_user_cost:,.2f}")

            with col3:
                avg_cost_per_user = total_cloud_cost / len(user_costs)
                st.metric("Avg Cost per User", f"${avg_cost_per_user:,.2f}")

            with col4:
                users_80pct = len(user_costs[user_costs['COST_PCT'].cumsum() <= 80])
                st.metric("Users for 80% Cost", users_80pct)

            st.markdown("---")

            # User cost table
            st.markdown("#### User Cost Details")

            display_df = user_costs[[
                'USER_NAME', 'QUERY_COUNT', 'EXEC_HOURS', 'BYTES_SCANNED',
                'CLOUD_CREDITS', 'CLOUD_COST', 'COST_PER_QUERY', 'COST_PCT'
            ]].copy()

            display_df.columns = [
                'User', 'Queries', 'Exec Hours', 'Bytes Scanned',
                'Credits', 'Cost ($)', 'Cost/Query ($)', '% of Total'
            ]

            st.dataframe(
                display_df.style.format({
                    'Queries': '{:,}',
                    'Exec Hours': '{:.2f}',
                    'Bytes Scanned': lambda x: format_bytes(x),
                    'Credits': '{:.4f}',
                    'Cost ($)': '${:,.2f}',
                    'Cost/Query ($)': '${:.4f}',
                    '% of Total': '{:.2f}%'
                }).background_gradient(subset=['Cost ($)'], cmap='YlOrRd'),
                use_container_width=True,
                height=400
            )

            # Visualizations
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Cost Distribution (Top 15 Users)")

                top_15 = user_costs.head(15)

                fig = px.sunburst(
                    top_15,
                    path=['USER_NAME'],
                    values='CLOUD_COST',
                    title='User Cost Distribution',
                    color='CLOUD_COST',
                    color_continuous_scale='Reds'
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("#### Cost per Query (Top 10)")

                top_10_cpq = user_costs.nlargest(10, 'COST_PER_QUERY')

                fig = px.bar(
                    top_10_cpq,
                    x='COST_PER_QUERY',
                    y='USER_NAME',
                    orientation='h',
                    title='Highest Cost per Query',
                    labels={'COST_PER_QUERY': 'Cost per Query ($)', 'USER_NAME': 'User'}
                )

                fig.update_traces(marker_color='orange')
                st.plotly_chart(fig, use_container_width=True)

            # Pareto analysis
            st.markdown("---")
            st.markdown("#### Cost Pareto Analysis")

            user_costs_sorted = user_costs.sort_values('CLOUD_COST', ascending=False).copy()
            user_costs_sorted['CUMULATIVE_PCT'] = user_costs_sorted['COST_PCT'].cumsum()

            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=user_costs_sorted['USER_NAME'].head(20),
                y=user_costs_sorted['CLOUD_COST'].head(20),
                name='Cost',
                marker_color='steelblue',
                yaxis='y'
            ))

            fig.add_trace(go.Scatter(
                x=user_costs_sorted['USER_NAME'].head(20),
                y=user_costs_sorted['CUMULATIVE_PCT'].head(20),
                name='Cumulative %',
                marker_color='red',
                yaxis='y2',
                mode='lines+markers'
            ))

            fig.update_layout(
                title="User Cost Pareto Chart (Top 20)",
                xaxis_title="User",
                yaxis=dict(title="Cost ($)"),
                yaxis2=dict(title="Cumulative %", overlaying='y', side='right', range=[0, 100]),
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

            # Identify high-cost users
            high_cost_threshold = user_costs['CLOUD_COST'].quantile(0.9)
            high_cost_users = user_costs[user_costs['CLOUD_COST'] > high_cost_threshold]

            if not high_cost_users.empty:
                create_alert_badge(
                    f"💡 {len(high_cost_users)} user(s) in top 10% of costs - Consider reviewing their query patterns",
                    "info"
                )

        else:
            st.info("No cost attribution data available")

    except Exception as e:
        st.error(f"Error loading cost attribution: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 4: User Deep Dive
# ----------------------------------------------------------------------------

with tab4:
    st.markdown("### 🔍 User Deep Dive")

    try:
        if 'user_metrics' in locals() and not user_metrics.empty:
            # User selector
            selected_user = st.selectbox(
                "Select User to Analyze",
                options=sorted(user_metrics['USER_NAME'].unique()),
                help="Choose a user for detailed analysis"
            )

            if selected_user:
                st.markdown(f"#### Analysis for: **{selected_user}**")

                # User summary
                user_summary = user_metrics[user_metrics['USER_NAME'] == selected_user].iloc[0]

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Total Queries", format_number(int(user_summary['QUERY_COUNT'])))

                with col2:
                    st.metric("Active Days", format_number(int(user_summary['ACTIVE_DAYS'])))

                with col3:
                    st.metric("Avg Exec Time", f"{user_summary['AVG_EXEC_TIME_SEC']:.2f}s")

                with col4:
                    st.metric("Total Cost", f"${user_summary['COST']:.2f}")

                st.markdown("---")

                # User queries details
                user_queries_query = f"""
                SELECT
                    QUERY_ID,
                    QUERY_TEXT,
                    QUERY_TYPE,
                    WAREHOUSE_NAME,
                    EXECUTION_TIME / 1000 AS EXEC_TIME_SEC,
                    BYTES_SCANNED,
                    ROWS_PRODUCED,
                    CREDITS_USED_CLOUD_SERVICES,
                    ERROR_CODE,
                    ERROR_MESSAGE,
                    START_TIME
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE USER_NAME = '{selected_user}'
                AND START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
                ORDER BY START_TIME DESC
                LIMIT 100
                """
                user_queries = session.sql(user_queries_query).to_pandas()

                if not user_queries.empty:
                    user_queries['START_TIME'] = pd.to_datetime(user_queries['START_TIME'])
                    user_queries['COST'] = user_queries['CREDITS_USED_CLOUD_SERVICES'] * credit_cost

                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown("#### Query Type Distribution")

                        type_dist = user_queries['QUERY_TYPE'].value_counts().reset_index()
                        type_dist.columns = ['QUERY_TYPE', 'COUNT']

                        fig = px.pie(
                            type_dist,
                            values='COUNT',
                            names='QUERY_TYPE',
                            title=f'{selected_user} Query Types'
                        )

                        st.plotly_chart(fig, use_container_width=True)

                    with col2:
                        st.markdown("#### Warehouse Usage")

                        wh_dist = user_queries['WAREHOUSE_NAME'].value_counts().reset_index()
                        wh_dist.columns = ['WAREHOUSE_NAME', 'COUNT']

                        fig = px.bar(
                            wh_dist,
                            x='WAREHOUSE_NAME',
                            y='COUNT',
                            title=f'{selected_user} Warehouse Usage',
                            labels={'COUNT': 'Query Count', 'WAREHOUSE_NAME': 'Warehouse'}
                        )

                        fig.update_traces(marker_color='lightblue')
                        st.plotly_chart(fig, use_container_width=True)

                    # Query timeline
                    st.markdown("---")
                    st.markdown("#### Query Activity Timeline")

                    daily_activity = user_queries.groupby(user_queries['START_TIME'].dt.date).agg({
                        'QUERY_ID': 'count',
                        'EXEC_TIME_SEC': 'sum',
                        'COST': 'sum'
                    }).reset_index()

                    daily_activity.columns = ['DATE', 'QUERY_COUNT', 'TOTAL_EXEC_TIME', 'TOTAL_COST']
                    daily_activity['DATE'] = pd.to_datetime(daily_activity['DATE'])

                    fig = go.Figure()

                    fig.add_trace(go.Bar(
                        x=daily_activity['DATE'],
                        y=daily_activity['QUERY_COUNT'],
                        name='Query Count',
                        marker_color='lightblue',
                        yaxis='y'
                    ))

                    fig.add_trace(go.Scatter(
                        x=daily_activity['DATE'],
                        y=daily_activity['TOTAL_COST'],
                        name='Cost',
                        marker_color='red',
                        yaxis='y2',
                        mode='lines+markers'
                    ))

                    fig.update_layout(
                        title=f"{selected_user} Daily Activity",
                        xaxis_title="Date",
                        yaxis=dict(title="Query Count"),
                        yaxis2=dict(title="Cost ($)", overlaying='y', side='right'),
                        height=400
                    )

                    st.plotly_chart(fig, use_container_width=True)

                    # Recent queries
                    st.markdown("---")
                    st.markdown("#### Recent Queries")

                    display_df = user_queries[[
                        'QUERY_ID', 'QUERY_TYPE', 'WAREHOUSE_NAME', 'EXEC_TIME_SEC',
                        'BYTES_SCANNED', 'COST', 'ERROR_CODE', 'START_TIME'
                    ]].head(20).copy()

                    display_df.columns = [
                        'Query ID', 'Type', 'Warehouse', 'Exec (s)',
                        'Bytes Scanned', 'Cost ($)', 'Error', 'Time'
                    ]

                    st.dataframe(
                        display_df.style.format({
                            'Exec (s)': '{:.2f}',
                            'Bytes Scanned': lambda x: format_bytes(x),
                            'Cost ($)': '${:.4f}',
                            'Time': lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
                        }),
                        use_container_width=True,
                        height=400
                    )

                    # AI insights for user
                    st.markdown("---")
                    st.markdown("#### 🤖 AI User Insights")

                    if ai_insights.check_cortex_availability():
                        if st.button("Generate AI Insights for This User"):
                            with st.spinner("Analyzing user behavior..."):
                                try:
                                    user_context = f"""
                                    User: {selected_user}
                                    Total Queries: {user_summary['QUERY_COUNT']}
                                    Active Days: {user_summary['ACTIVE_DAYS']}
                                    Avg Execution Time: {user_summary['AVG_EXEC_TIME_SEC']:.2f}s
                                    Total Cost: ${user_summary['COST']:.2f}
                                    Error Rate: {user_summary['ERROR_RATE']:.2f}%
                                    """

                                    insight = ai_insights.generate_insight(
                                        user_context,
                                        f"Analyze {selected_user}'s usage patterns and provide recommendations for optimization and cost reduction."
                                    )

                                    st.markdown(f'<div class="insight-card">{insight}</div>', unsafe_allow_html=True)
                                except Exception as e:
                                    st.warning(f"AI analysis unavailable: {str(e)}")

                else:
                    st.info(f"No query data available for {selected_user}")

        else:
            st.info("No user data available for deep dive")

    except Exception as e:
        st.error(f"Error in user deep dive: {str(e)}")

# Footer
st.markdown("---")
st.caption(f"📅 Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | ⏱️ Time period: {time_period} days | 💵 Credit cost: ${credit_cost}/credit")
