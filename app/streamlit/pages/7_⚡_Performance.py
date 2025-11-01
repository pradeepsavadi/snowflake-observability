"""
Snowflake Holistic Observability Dashboard - Performance Page
==============================================================
Identify query bottlenecks, spilling, pruning issues, and optimization opportunities
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
    page_title="Performance - Snowflake Observability",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom styling
apply_custom_css()

# Initialize session state and render settings
initialize_session_state()
render_settings_sidebar()

# Page header
render_page_header("⚡ Performance Analysis", "Identify query bottlenecks and optimization opportunities")

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
# PERFORMANCE OVERVIEW
# ============================================================================

st.markdown("---")
st.subheader("📊 Performance Overview")

col1, col2, col3, col4 = st.columns(4)

with st.spinner("Loading performance metrics..."):
    try:
        # Get query performance insights
        perf_insights = queries.get_query_performance_insights(time_period)

        total_slow_queries = perf_insights['QUERY_COUNT'].sum() if not perf_insights.empty else 0

        # Get spilling queries
        spilling_query = f"""
        SELECT
            COUNT(DISTINCT QUERY_ID) AS SPILLING_QUERIES,
            SUM(BYTES_SPILLED_TO_LOCAL_STORAGE) AS LOCAL_SPILL,
            SUM(BYTES_SPILLED_TO_REMOTE_STORAGE) AS REMOTE_SPILL
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        AND (BYTES_SPILLED_TO_LOCAL_STORAGE > 0 OR BYTES_SPILLED_TO_REMOTE_STORAGE > 0)
        """
        spilling_data = session.sql(spilling_query).to_pandas()

        spilling_queries = spilling_data['SPILLING_QUERIES'].iloc[0] if not spilling_data.empty else 0
        local_spill = spilling_data['LOCAL_SPILL'].iloc[0] if not spilling_data.empty else 0
        remote_spill = spilling_data['REMOTE_SPILL'].iloc[0] if not spilling_data.empty else 0

        # Get total queries
        total_queries_query = f"""
        SELECT COUNT(*) AS TOTAL_QUERIES
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        """
        total_queries = session.sql(total_queries_query).to_pandas()['TOTAL_QUERIES'].iloc[0]

        # Calculate performance score (simple metric)
        perf_score = max(0, 100 - (total_slow_queries / max(total_queries, 1) * 1000))

        with col1:
            st.metric(
                "Performance Score",
                f"{perf_score:.1f}/100",
                help="Based on slow query ratio"
            )

        with col2:
            slow_query_pct = (total_slow_queries / max(total_queries, 1) * 100)
            st.metric(
                "Slow Queries",
                format_number(int(total_slow_queries)),
                delta=f"{slow_query_pct:.2f}%",
                delta_color="inverse",
                help="Queries with execution time > 60s"
            )

        with col3:
            spilling_pct = (spilling_queries / max(total_queries, 1) * 100)
            st.metric(
                "Spilling Queries",
                format_number(int(spilling_queries)),
                delta=f"{spilling_pct:.2f}%",
                delta_color="inverse",
                help="Queries that spilled to disk"
            )

        with col4:
            total_spill = local_spill + remote_spill
            st.metric(
                "Total Spill",
                format_bytes(total_spill),
                help="Data spilled to local/remote storage"
            )

        # Performance alerts
        st.markdown("---")

        if slow_query_pct > 5:
            create_alert_badge(
                f"⚠️ {slow_query_pct:.1f}% of queries are slow - Review Performance tab for details",
                "warning"
            )

        if spilling_pct > 10:
            create_alert_badge(
                f"⚠️ {spilling_pct:.1f}% of queries are spilling - Consider warehouse sizing",
                "warning"
            )

        if perf_score < 70:
            create_alert_badge(
                "⚠️ Performance score is low - Multiple optimization opportunities available",
                "warning"
            )
        elif perf_score >= 90:
            create_alert_badge("✅ Excellent query performance", "success")

    except Exception as e:
        st.error(f"Error loading performance overview: {str(e)}")

# ============================================================================
# PERFORMANCE TABS
# ============================================================================

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🐌 Slow Queries",
    "💾 Spilling Analysis",
    "🎯 Pruning Effectiveness",
    "🔄 Query Patterns",
    "💡 Optimization Recommendations"
])

# ----------------------------------------------------------------------------
# TAB 1: Slow Queries
# ----------------------------------------------------------------------------

with tab1:
    st.markdown("### 🐌 Slow Query Analysis")

    # Threshold selector
    slow_query_threshold = st.slider(
        "Slow Query Threshold (seconds)",
        min_value=10,
        max_value=600,
        value=60,
        step=10,
        help="Define what constitutes a 'slow' query"
    )

    try:
        slow_queries_query = f"""
        SELECT
            QUERY_ID,
            QUERY_TEXT,
            USER_NAME,
            WAREHOUSE_NAME,
            DATABASE_NAME,
            EXECUTION_TIME / 1000 AS EXECUTION_TIME_SEC,
            QUEUED_PROVISIONING_TIME / 1000 AS QUEUED_TIME_SEC,
            BYTES_SCANNED,
            ROWS_PRODUCED,
            CREDITS_USED_CLOUD_SERVICES,
            START_TIME,
            COMPILATION_TIME / 1000 AS COMPILATION_TIME_SEC,
            BYTES_SPILLED_TO_LOCAL_STORAGE + BYTES_SPILLED_TO_REMOTE_STORAGE AS TOTAL_SPILL
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        AND EXECUTION_TIME > {slow_query_threshold * 1000}
        ORDER BY EXECUTION_TIME DESC
        LIMIT 100
        """
        slow_queries = session.sql(slow_queries_query).to_pandas()

        if not slow_queries.empty:
            slow_queries['START_TIME'] = pd.to_datetime(slow_queries['START_TIME'])
            slow_queries['QUERY_TEXT_SHORT'] = slow_queries['QUERY_TEXT'].str[:100] + '...'

            # Summary metrics
            col1, col2, col3 = st.columns(3)

            with col1:
                avg_exec_time = slow_queries['EXECUTION_TIME_SEC'].mean()
                st.metric("Average Exec Time", f"{avg_exec_time:.1f}s")

            with col2:
                max_exec_time = slow_queries['EXECUTION_TIME_SEC'].max()
                st.metric("Max Exec Time", f"{max_exec_time:.1f}s")

            with col3:
                total_credits = slow_queries['CREDITS_USED_CLOUD_SERVICES'].sum()
                st.metric("Total Credits", f"{total_credits:.2f}")

            st.markdown("---")

            # Top slow queries table
            st.markdown("#### Top Slow Queries")

            display_df = slow_queries[[
                'QUERY_ID', 'USER_NAME', 'WAREHOUSE_NAME', 'EXECUTION_TIME_SEC',
                'QUEUED_TIME_SEC', 'BYTES_SCANNED', 'TOTAL_SPILL', 'START_TIME'
            ]].copy()

            display_df.columns = [
                'Query ID', 'User', 'Warehouse', 'Exec Time (s)',
                'Queued (s)', 'Bytes Scanned', 'Spill', 'Start Time'
            ]

            st.dataframe(
                display_df.style.format({
                    'Exec Time (s)': '{:.2f}',
                    'Queued (s)': '{:.2f}',
                    'Bytes Scanned': lambda x: format_bytes(x),
                    'Spill': lambda x: format_bytes(x),
                    'Start Time': lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
                }).background_gradient(subset=['Exec Time (s)'], cmap='YlOrRd'),
                use_container_width=True,
                height=400
            )

            # Query details expander
            st.markdown("---")
            st.markdown("#### Query Details")

            selected_query_id = st.selectbox(
                "Select a query to analyze",
                options=slow_queries['QUERY_ID'].tolist(),
                format_func=lambda x: f"{x} ({slow_queries[slow_queries['QUERY_ID']==x]['EXECUTION_TIME_SEC'].iloc[0]:.1f}s)"
            )

            if selected_query_id:
                query_details = slow_queries[slow_queries['QUERY_ID'] == selected_query_id].iloc[0]

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("**Query Metrics:**")
                    st.write(f"**User:** {query_details['USER_NAME']}")
                    st.write(f"**Warehouse:** {query_details['WAREHOUSE_NAME']}")
                    st.write(f"**Database:** {query_details['DATABASE_NAME']}")
                    st.write(f"**Execution Time:** {query_details['EXECUTION_TIME_SEC']:.2f}s")
                    st.write(f"**Queued Time:** {query_details['QUEUED_TIME_SEC']:.2f}s")
                    st.write(f"**Compilation Time:** {query_details['COMPILATION_TIME_SEC']:.2f}s")

                with col2:
                    st.markdown("**Resource Usage:**")
                    st.write(f"**Bytes Scanned:** {format_bytes(query_details['BYTES_SCANNED'])}")
                    st.write(f"**Rows Produced:** {format_number(int(query_details['ROWS_PRODUCED']))}")
                    st.write(f"**Credits Used:** {query_details['CREDITS_USED_CLOUD_SERVICES']:.4f}")
                    st.write(f"**Total Spill:** {format_bytes(query_details['TOTAL_SPILL'])}")

                st.markdown("**Query Text:**")
                st.code(query_details['QUERY_TEXT'], language='sql')

                # AI-powered query analysis
                if ai_insights.check_cortex_availability():
                    if st.button("🤖 Get AI Optimization Suggestions"):
                        with st.spinner("Analyzing query..."):
                            try:
                                query_context = f"""
                                Query ID: {selected_query_id}
                                Execution Time: {query_details['EXECUTION_TIME_SEC']:.2f}s
                                Bytes Scanned: {format_bytes(query_details['BYTES_SCANNED'])}
                                Spilling: {format_bytes(query_details['TOTAL_SPILL'])}
                                Query: {query_details['QUERY_TEXT'][:500]}
                                """

                                suggestion = ai_insights.generate_insight(
                                    query_context,
                                    "Analyze this slow query and provide specific optimization recommendations."
                                )

                                st.markdown(f'<div class="insight-card">{suggestion}</div>', unsafe_allow_html=True)
                            except Exception as e:
                                st.warning(f"AI analysis unavailable: {str(e)}")

            # Execution time distribution
            st.markdown("---")
            st.markdown("#### Execution Time Distribution")

            fig = px.histogram(
                slow_queries,
                x='EXECUTION_TIME_SEC',
                nbins=30,
                title='Distribution of Slow Query Execution Times',
                labels={'EXECUTION_TIME_SEC': 'Execution Time (seconds)', 'count': 'Number of Queries'}
            )

            fig.update_traces(marker_color='steelblue')
            st.plotly_chart(fig, use_container_width=True)

            # Slow queries by warehouse
            st.markdown("---")
            st.markdown("#### Slow Queries by Warehouse")

            wh_slow_queries = slow_queries.groupby('WAREHOUSE_NAME').agg({
                'QUERY_ID': 'count',
                'EXECUTION_TIME_SEC': 'mean'
            }).reset_index()

            wh_slow_queries.columns = ['WAREHOUSE_NAME', 'SLOW_QUERY_COUNT', 'AVG_EXEC_TIME']
            wh_slow_queries = wh_slow_queries.sort_values('SLOW_QUERY_COUNT', ascending=False)

            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=wh_slow_queries['WAREHOUSE_NAME'],
                y=wh_slow_queries['SLOW_QUERY_COUNT'],
                name='Slow Query Count',
                marker_color='lightcoral',
                yaxis='y',
                hovertemplate='%{x}<br>Count: %{y}<extra></extra>'
            ))

            fig.add_trace(go.Scatter(
                x=wh_slow_queries['WAREHOUSE_NAME'],
                y=wh_slow_queries['AVG_EXEC_TIME'],
                name='Avg Exec Time',
                marker_color='blue',
                yaxis='y2',
                mode='lines+markers',
                hovertemplate='%{x}<br>Avg Time: %{y:.1f}s<extra></extra>'
            ))

            fig.update_layout(
                title="Slow Queries by Warehouse",
                xaxis_title="Warehouse",
                yaxis=dict(title="Slow Query Count"),
                yaxis2=dict(title="Avg Exec Time (s)", overlaying='y', side='right'),
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

        else:
            create_alert_badge(
                f"✅ No queries slower than {slow_query_threshold}s found",
                "success"
            )

    except Exception as e:
        st.error(f"Error loading slow queries: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 2: Spilling Analysis
# ----------------------------------------------------------------------------

with tab2:
    st.markdown("### 💾 Spilling Analysis")

    try:
        spilling_queries_query = f"""
        SELECT
            QUERY_ID,
            QUERY_TEXT,
            USER_NAME,
            WAREHOUSE_NAME,
            WAREHOUSE_SIZE,
            BYTES_SPILLED_TO_LOCAL_STORAGE,
            BYTES_SPILLED_TO_REMOTE_STORAGE,
            (BYTES_SPILLED_TO_LOCAL_STORAGE + BYTES_SPILLED_TO_REMOTE_STORAGE) AS TOTAL_SPILL,
            EXECUTION_TIME / 1000 AS EXECUTION_TIME_SEC,
            BYTES_SCANNED,
            START_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        AND (BYTES_SPILLED_TO_LOCAL_STORAGE > 0 OR BYTES_SPILLED_TO_REMOTE_STORAGE > 0)
        ORDER BY TOTAL_SPILL DESC
        LIMIT 100
        """
        spilling_queries_df = session.sql(spilling_queries_query).to_pandas()

        if not spilling_queries_df.empty:
            spilling_queries_df['START_TIME'] = pd.to_datetime(spilling_queries_df['START_TIME'])

            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                total_local_spill = spilling_queries_df['BYTES_SPILLED_TO_LOCAL_STORAGE'].sum()
                st.metric("Total Local Spill", format_bytes(total_local_spill))

            with col2:
                total_remote_spill = spilling_queries_df['BYTES_SPILLED_TO_REMOTE_STORAGE'].sum()
                st.metric("Total Remote Spill", format_bytes(total_remote_spill))

            with col3:
                spilling_query_count = len(spilling_queries_df)
                st.metric("Spilling Queries", format_number(spilling_query_count))

            with col4:
                avg_spill = spilling_queries_df['TOTAL_SPILL'].mean()
                st.metric("Avg Spill per Query", format_bytes(avg_spill))

            # Spilling severity indicator
            if total_remote_spill > total_local_spill:
                create_alert_badge(
                    "🚨 High remote spilling detected - Significant performance impact likely",
                    "warning"
                )
                st.markdown("""
                **Impact:** Remote spilling is much slower than local spilling and indicates serious memory pressure.

                **Recommendations:**
                - Increase warehouse size to provide more memory
                - Optimize queries to reduce memory usage
                - Consider breaking large queries into smaller chunks
                """)
            elif total_local_spill > 0:
                create_alert_badge(
                    "⚠️ Local spilling detected - Some performance impact",
                    "warning"
                )

            st.markdown("---")

            # Spilling queries table
            st.markdown("#### Queries with Spilling")

            display_df = spilling_queries_df[[
                'QUERY_ID', 'USER_NAME', 'WAREHOUSE_NAME', 'WAREHOUSE_SIZE',
                'BYTES_SPILLED_TO_LOCAL_STORAGE', 'BYTES_SPILLED_TO_REMOTE_STORAGE',
                'EXECUTION_TIME_SEC', 'START_TIME'
            ]].copy()

            display_df.columns = [
                'Query ID', 'User', 'Warehouse', 'Size',
                'Local Spill', 'Remote Spill', 'Exec Time (s)', 'Start Time'
            ]

            st.dataframe(
                display_df.style.format({
                    'Local Spill': lambda x: format_bytes(x),
                    'Remote Spill': lambda x: format_bytes(x),
                    'Exec Time (s)': '{:.2f}',
                    'Start Time': lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
                }).background_gradient(subset=['Remote Spill'], cmap='YlOrRd'),
                use_container_width=True,
                height=400
            )

            # Spill type comparison
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Spill Type Distribution")

                spill_comparison = pd.DataFrame({
                    'Type': ['Local Spill', 'Remote Spill'],
                    'Bytes': [total_local_spill, total_remote_spill]
                })

                fig = px.pie(
                    spill_comparison,
                    values='Bytes',
                    names='Type',
                    title='Local vs Remote Spilling',
                    color_discrete_sequence=['#90EE90', '#FF6B6B']
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("#### Spilling by Warehouse Size")

                wh_size_spill = spilling_queries_df.groupby('WAREHOUSE_SIZE').agg({
                    'TOTAL_SPILL': 'sum',
                    'QUERY_ID': 'count'
                }).reset_index()

                wh_size_spill.columns = ['WAREHOUSE_SIZE', 'TOTAL_SPILL', 'QUERY_COUNT']

                fig = px.bar(
                    wh_size_spill,
                    x='WAREHOUSE_SIZE',
                    y='TOTAL_SPILL',
                    title='Total Spilling by Warehouse Size',
                    labels={'TOTAL_SPILL': 'Total Spill (Bytes)', 'WAREHOUSE_SIZE': 'Warehouse Size'},
                    text='QUERY_COUNT'
                )

                fig.update_traces(
                    marker_color='lightblue',
                    texttemplate='%{text} queries',
                    textposition='outside'
                )

                st.plotly_chart(fig, use_container_width=True)

            # Daily spilling trend
            st.markdown("---")
            st.markdown("#### Daily Spilling Trend")

            daily_spill = spilling_queries_df.groupby(spilling_queries_df['START_TIME'].dt.date).agg({
                'BYTES_SPILLED_TO_LOCAL_STORAGE': 'sum',
                'BYTES_SPILLED_TO_REMOTE_STORAGE': 'sum',
                'QUERY_ID': 'count'
            }).reset_index()

            daily_spill.columns = ['DATE', 'LOCAL_SPILL', 'REMOTE_SPILL', 'QUERY_COUNT']
            daily_spill['TOTAL_SPILL'] = daily_spill['LOCAL_SPILL'] + daily_spill['REMOTE_SPILL']
            daily_spill['DATE'] = pd.to_datetime(daily_spill['DATE'])

            fig = go.Figure()

            fig.add_trace(go.Scatter(
                x=daily_spill['DATE'],
                y=daily_spill['LOCAL_SPILL'],
                mode='lines+markers',
                name='Local Spill',
                fill='tonexty',
                marker_color='lightgreen'
            ))

            fig.add_trace(go.Scatter(
                x=daily_spill['DATE'],
                y=daily_spill['REMOTE_SPILL'],
                mode='lines+markers',
                name='Remote Spill',
                fill='tonexty',
                marker_color='salmon'
            ))

            fig.update_layout(
                title="Daily Spilling Trend",
                xaxis_title="Date",
                yaxis_title="Bytes Spilled",
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

        else:
            create_alert_badge("✅ No spilling detected - Excellent memory management", "success")

    except Exception as e:
        st.error(f"Error loading spilling analysis: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 3: Pruning Effectiveness
# ----------------------------------------------------------------------------

with tab3:
    st.markdown("### 🎯 Partition Pruning Effectiveness")

    try:
        pruning_query = f"""
        SELECT
            t.TABLE_CATALOG,
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            t.PARTITION_KEY,
            SUM(p.PARTITIONS_SCANNED) AS TOTAL_PARTITIONS_SCANNED,
            SUM(p.PARTITIONS_TOTAL) AS TOTAL_PARTITIONS,
            AVG(CASE
                WHEN p.PARTITIONS_TOTAL > 0
                THEN (p.PARTITIONS_SCANNED::FLOAT / p.PARTITIONS_TOTAL * 100)
                ELSE 0
            END) AS AVG_SCAN_PCT,
            COUNT(DISTINCT p.QUERY_ID) AS QUERY_COUNT
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS t
        JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a
            ON t.TABLE_CATALOG = a.OBJECTS_MODIFIED[0]:objectDomain::STRING
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
            ON a.QUERY_ID = q.QUERY_ID
        LEFT JOIN (
            SELECT
                QUERY_ID,
                SUM(PARTITIONS_SCANNED) AS PARTITIONS_SCANNED,
                SUM(PARTITIONS_TOTAL) AS PARTITIONS_TOTAL
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            AND PARTITIONS_TOTAL > 0
            GROUP BY QUERY_ID
        ) p ON q.QUERY_ID = p.QUERY_ID
        WHERE q.START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        AND p.PARTITIONS_TOTAL > 0
        GROUP BY t.TABLE_CATALOG, t.TABLE_SCHEMA, t.TABLE_NAME, t.PARTITION_KEY
        HAVING SUM(p.PARTITIONS_TOTAL) > 0
        ORDER BY AVG_SCAN_PCT DESC
        LIMIT 50
        """

        # Simplified pruning query for better compatibility
        pruning_simple_query = f"""
        SELECT
            QUERY_ID,
            QUERY_TEXT,
            DATABASE_NAME,
            PARTITIONS_SCANNED,
            PARTITIONS_TOTAL,
            CASE
                WHEN PARTITIONS_TOTAL > 0
                THEN (PARTITIONS_SCANNED::FLOAT / PARTITIONS_TOTAL * 100)
                ELSE 0
            END AS SCAN_PCT,
            BYTES_SCANNED,
            EXECUTION_TIME / 1000 AS EXECUTION_TIME_SEC,
            START_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        AND PARTITIONS_TOTAL > 0
        ORDER BY SCAN_PCT DESC
        LIMIT 100
        """

        pruning_data = session.sql(pruning_simple_query).to_pandas()

        if not pruning_data.empty:
            pruning_data['START_TIME'] = pd.to_datetime(pruning_data['START_TIME'])

            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)

            avg_scan_pct = pruning_data['SCAN_PCT'].mean()
            poor_pruning = len(pruning_data[pruning_data['SCAN_PCT'] > 50])
            excellent_pruning = len(pruning_data[pruning_data['SCAN_PCT'] < 10])

            with col1:
                st.metric(
                    "Avg Scan %",
                    f"{avg_scan_pct:.1f}%",
                    help="Lower is better - indicates effective pruning"
                )

            with col2:
                st.metric(
                    "Poor Pruning",
                    format_number(poor_pruning),
                    help="Queries scanning >50% of partitions"
                )

            with col3:
                st.metric(
                    "Excellent Pruning",
                    format_number(excellent_pruning),
                    help="Queries scanning <10% of partitions"
                )

            with col4:
                total_partitions_scanned = pruning_data['PARTITIONS_SCANNED'].sum()
                st.metric(
                    "Total Partitions Scanned",
                    format_number(int(total_partitions_scanned))
                )

            # Pruning effectiveness indicator
            if avg_scan_pct > 70:
                create_alert_badge(
                    f"🚨 Poor partition pruning ({avg_scan_pct:.1f}% avg) - Queries scanning most partitions",
                    "warning"
                )
            elif avg_scan_pct < 30:
                create_alert_badge(
                    f"✅ Excellent partition pruning ({avg_scan_pct:.1f}% avg)",
                    "success"
                )

            st.markdown("---")

            # Pruning effectiveness table
            st.markdown("#### Queries with Poor Pruning")

            poor_pruning_queries = pruning_data[pruning_data['SCAN_PCT'] > 50].copy()

            if not poor_pruning_queries.empty:
                display_df = poor_pruning_queries[[
                    'QUERY_ID', 'DATABASE_NAME', 'PARTITIONS_SCANNED', 'PARTITIONS_TOTAL',
                    'SCAN_PCT', 'BYTES_SCANNED', 'EXECUTION_TIME_SEC'
                ]].copy()

                display_df.columns = [
                    'Query ID', 'Database', 'Scanned', 'Total',
                    'Scan %', 'Bytes Scanned', 'Exec Time (s)'
                ]

                st.dataframe(
                    display_df.style.format({
                        'Scanned': '{:,}',
                        'Total': '{:,}',
                        'Scan %': '{:.1f}%',
                        'Bytes Scanned': lambda x: format_bytes(x),
                        'Exec Time (s)': '{:.2f}'
                    }).background_gradient(subset=['Scan %'], cmap='RdYlGn_r'),
                    use_container_width=True,
                    height=300
                )
            else:
                st.info("No queries with poor pruning found")

            # Pruning distribution
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Pruning Effectiveness Distribution")

                # Create bins for scan percentage
                pruning_data['PRUNING_CATEGORY'] = pd.cut(
                    pruning_data['SCAN_PCT'],
                    bins=[0, 10, 30, 50, 70, 100],
                    labels=['Excellent (0-10%)', 'Good (10-30%)', 'Fair (30-50%)', 'Poor (50-70%)', 'Very Poor (70-100%)']
                )

                category_counts = pruning_data['PRUNING_CATEGORY'].value_counts().reset_index()
                category_counts.columns = ['Category', 'Count']

                fig = px.pie(
                    category_counts,
                    values='Count',
                    names='Category',
                    title='Pruning Effectiveness Categories',
                    color_discrete_sequence=px.colors.sequential.RdYlGn_r
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("#### Scan % vs Execution Time")

                fig = px.scatter(
                    pruning_data,
                    x='SCAN_PCT',
                    y='EXECUTION_TIME_SEC',
                    size='BYTES_SCANNED',
                    color='SCAN_PCT',
                    title='Relationship between Pruning and Performance',
                    labels={
                        'SCAN_PCT': 'Partition Scan %',
                        'EXECUTION_TIME_SEC': 'Execution Time (s)',
                        'BYTES_SCANNED': 'Bytes Scanned'
                    },
                    color_continuous_scale='Reds'
                )

                st.plotly_chart(fig, use_container_width=True)

            # Recommendations
            st.markdown("---")
            st.markdown("#### 💡 Pruning Optimization Tips")

            st.markdown("""
            **To improve partition pruning:**

            1. **Use clustering keys** on commonly filtered columns
            2. **Add WHERE clause filters** on clustering key columns
            3. **Avoid functions** on clustering key columns in WHERE clauses
            4. **Use partition-aligned queries** when possible
            5. **Monitor clustering depth** and reclustering if needed

            **Example:**
            ```sql
            -- Good: Direct filter on clustering key
            SELECT * FROM table WHERE date_col >= '2024-01-01'

            -- Bad: Function prevents pruning
            SELECT * FROM table WHERE YEAR(date_col) = 2024

            -- Check clustering
            SELECT SYSTEM$CLUSTERING_INFORMATION('table_name');
            ```
            """)

        else:
            st.info("No partition pruning data available. This may indicate tables are not clustered or queries don't use partitioned tables.")

    except Exception as e:
        st.error(f"Error loading pruning analysis: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 4: Query Patterns
# ----------------------------------------------------------------------------

with tab4:
    st.markdown("### 🔄 Query Pattern Analysis")

    try:
        # Query type distribution
        query_type_query = f"""
        SELECT
            QUERY_TYPE,
            COUNT(*) AS QUERY_COUNT,
            AVG(EXECUTION_TIME) / 1000 AS AVG_EXEC_TIME_SEC,
            SUM(BYTES_SCANNED) AS TOTAL_BYTES_SCANNED,
            SUM(CREDITS_USED_CLOUD_SERVICES) AS TOTAL_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        AND QUERY_TYPE IS NOT NULL
        GROUP BY QUERY_TYPE
        ORDER BY QUERY_COUNT DESC
        """
        query_types = session.sql(query_type_query).to_pandas()

        if not query_types.empty:
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Query Type Distribution")

                fig = px.pie(
                    query_types,
                    values='QUERY_COUNT',
                    names='QUERY_TYPE',
                    title='Queries by Type',
                    hole=0.4
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("#### Average Execution Time by Type")

                fig = px.bar(
                    query_types,
                    x='QUERY_TYPE',
                    y='AVG_EXEC_TIME_SEC',
                    title='Avg Execution Time by Query Type',
                    labels={'AVG_EXEC_TIME_SEC': 'Avg Exec Time (s)', 'QUERY_TYPE': 'Query Type'}
                )

                fig.update_traces(marker_color='steelblue')
                st.plotly_chart(fig, use_container_width=True)

            # Query type details
            st.markdown("---")
            st.markdown("#### Query Type Details")

            display_df = query_types.copy()
            display_df['AVG_BYTES_PER_QUERY'] = display_df['TOTAL_BYTES_SCANNED'] / display_df['QUERY_COUNT']
            display_df.columns = ['Type', 'Count', 'Avg Exec Time (s)', 'Total Bytes', 'Credits', 'Avg Bytes/Query']

            st.dataframe(
                display_df.style.format({
                    'Count': '{:,}',
                    'Avg Exec Time (s)': '{:.2f}',
                    'Total Bytes': lambda x: format_bytes(x),
                    'Credits': '{:.4f}',
                    'Avg Bytes/Query': lambda x: format_bytes(x)
                }),
                use_container_width=True
            )

        # Repeated queries
        st.markdown("---")
        st.markdown("#### Repeated Query Patterns")

        repeated_query = f"""
        SELECT
            QUERY_PARAMETERIZED_HASH,
            COUNT(*) AS EXECUTION_COUNT,
            AVG(EXECUTION_TIME) / 1000 AS AVG_EXEC_TIME_SEC,
            MAX(EXECUTION_TIME) / 1000 AS MAX_EXEC_TIME_SEC,
            SUM(BYTES_SCANNED) AS TOTAL_BYTES_SCANNED,
            ANY_VALUE(QUERY_TEXT) AS SAMPLE_QUERY
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        AND QUERY_PARAMETERIZED_HASH IS NOT NULL
        GROUP BY QUERY_PARAMETERIZED_HASH
        HAVING COUNT(*) > 10
        ORDER BY EXECUTION_COUNT DESC
        LIMIT 20
        """
        repeated_queries = session.sql(repeated_query).to_pandas()

        if not repeated_queries.empty:
            repeated_queries['SAMPLE_QUERY_SHORT'] = repeated_queries['SAMPLE_QUERY'].str[:100] + '...'

            st.markdown("Top frequently executed query patterns (good candidates for caching/optimization):")

            display_df = repeated_queries[[
                'EXECUTION_COUNT', 'AVG_EXEC_TIME_SEC', 'MAX_EXEC_TIME_SEC',
                'TOTAL_BYTES_SCANNED', 'SAMPLE_QUERY_SHORT'
            ]].copy()

            display_df.columns = ['Executions', 'Avg Time (s)', 'Max Time (s)', 'Total Bytes', 'Sample Query']

            st.dataframe(
                display_df.style.format({
                    'Executions': '{:,}',
                    'Avg Time (s)': '{:.2f}',
                    'Max Time (s)': '{:.2f}',
                    'Total Bytes': lambda x: format_bytes(x)
                }).background_gradient(subset=['Executions'], cmap='Blues'),
                use_container_width=True,
                height=400
            )

            st.caption("💡 Tip: Consider result caching or materialized views for frequently executed queries")

        else:
            st.info("No frequently repeated query patterns found")

        # Hourly query pattern
        st.markdown("---")
        st.markdown("#### Hourly Query Pattern")

        hourly_pattern_query = f"""
        SELECT
            HOUR(START_TIME) AS HOUR_OF_DAY,
            COUNT(*) AS QUERY_COUNT,
            AVG(EXECUTION_TIME) / 1000 AS AVG_EXEC_TIME_SEC
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        GROUP BY HOUR_OF_DAY
        ORDER BY HOUR_OF_DAY
        """
        hourly_pattern = session.sql(hourly_pattern_query).to_pandas()

        if not hourly_pattern.empty:
            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=hourly_pattern['HOUR_OF_DAY'],
                y=hourly_pattern['QUERY_COUNT'],
                name='Query Count',
                marker_color='lightblue',
                yaxis='y'
            ))

            fig.add_trace(go.Scatter(
                x=hourly_pattern['HOUR_OF_DAY'],
                y=hourly_pattern['AVG_EXEC_TIME_SEC'],
                name='Avg Exec Time',
                marker_color='red',
                yaxis='y2',
                mode='lines+markers'
            ))

            fig.update_layout(
                title="Query Activity by Hour of Day",
                xaxis_title="Hour",
                yaxis=dict(title="Query Count"),
                yaxis2=dict(title="Avg Exec Time (s)", overlaying='y', side='right'),
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

            peak_hour = hourly_pattern.loc[hourly_pattern['QUERY_COUNT'].idxmax(), 'HOUR_OF_DAY']
            st.caption(f"Peak activity hour: {int(peak_hour)}:00")

    except Exception as e:
        st.error(f"Error loading query patterns: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 5: Optimization Recommendations
# ----------------------------------------------------------------------------

with tab5:
    st.markdown("### 💡 Performance Optimization Recommendations")

    col1, col2 = st.columns([2, 1])

    with col1:
        try:
            recommendations = []

            # 1. Slow query optimization
            if 'slow_queries' in locals() and not slow_queries.empty:
                slow_query_cost = slow_queries['CREDITS_USED_CLOUD_SERVICES'].sum() * credit_cost

                recommendations.append({
                    'priority': 'HIGH',
                    'category': 'Slow Queries',
                    'issue': f"{len(slow_queries)} queries slower than {slow_query_threshold}s",
                    'impact': f"${slow_query_cost:.2f} in credits, potential user dissatisfaction",
                    'action': """
                    - Review query execution plans
                    - Add appropriate indexes/clustering
                    - Optimize JOIN operations
                    - Consider materialized views for complex queries
                    - Review warehouse sizing
                    """
                })

            # 2. Spilling optimization
            if 'spilling_queries_df' in locals() and not spilling_queries_df.empty:
                if total_remote_spill > 0:
                    priority = 'CRITICAL'
                    message = f"{format_bytes(total_remote_spill)} remote spilling"
                else:
                    priority = 'MEDIUM'
                    message = f"{format_bytes(total_local_spill)} local spilling"

                recommendations.append({
                    'priority': priority,
                    'category': 'Memory Spilling',
                    'issue': message,
                    'impact': "Degraded query performance, increased execution time",
                    'action': """
                    - Increase warehouse size for memory-intensive queries
                    - Optimize query memory usage (reduce large JOINs)
                    - Break complex queries into CTEs
                    - Add appropriate filters early in query
                    - Consider using smaller result sets
                    """
                })

            # 3. Pruning optimization
            if 'poor_pruning_queries' in locals() and not poor_pruning_queries.empty:
                recommendations.append({
                    'priority': 'MEDIUM',
                    'category': 'Poor Partition Pruning',
                    'issue': f"{len(poor_pruning_queries)} queries scanning >50% of partitions",
                    'impact': "Unnecessary data scanning, higher costs, slower queries",
                    'action': """
                    - Define clustering keys on frequently filtered columns
                    - Add WHERE clauses on clustering key columns
                    - Avoid using functions on clustered columns in filters
                    - Monitor and maintain clustering with SYSTEM$CLUSTERING_INFORMATION
                    - Consider automatic clustering for large tables
                    """
                })

            # 4. Result caching
            if 'repeated_queries' in locals() and not repeated_queries.empty:
                top_repeated = repeated_queries.iloc[0]['EXECUTION_COUNT']

                recommendations.append({
                    'priority': 'LOW',
                    'category': 'Result Caching',
                    'issue': f"Query patterns repeated up to {int(top_repeated)} times",
                    'impact': "Opportunity for performance improvement and cost savings",
                    'action': """
                    - Enable USE_CACHED_RESULT for repeated queries
                    - Consider materialized views for frequently accessed aggregations
                    - Implement application-level caching
                    - Use Snowflake's result cache (automatic for 24 hours)
                    """
                })

            # Display recommendations
            if recommendations:
                # Sort by priority
                priority_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
                recommendations.sort(key=lambda x: priority_order.get(x['priority'], 4))

                for i, rec in enumerate(recommendations, 1):
                    priority_colors = {
                        'CRITICAL': '🔴',
                        'HIGH': '🟠',
                        'MEDIUM': '🟡',
                        'LOW': '🟢'
                    }

                    with st.expander(
                        f"{priority_colors.get(rec['priority'], '🔵')} {rec['category']} - {rec['issue']}",
                        expanded=(rec['priority'] in ['CRITICAL', 'HIGH'])
                    ):
                        st.markdown(f"**Priority:** {rec['priority']}")
                        st.markdown(f"**Issue:** {rec['issue']}")
                        st.markdown(f"**Impact:** {rec['impact']}")
                        st.markdown(f"**Recommended Actions:**")
                        st.info(rec['action'])
            else:
                create_alert_badge("✅ No major performance issues detected", "success")

        except Exception as e:
            st.error(f"Error generating recommendations: {str(e)}")

    with col2:
        st.markdown("#### 🤖 AI Performance Insights")

        try:
            if ai_insights.check_cortex_availability():
                with st.spinner("Generating AI insights..."):
                    context = {
                        "Performance Score": f"{perf_score:.1f}/100" if 'perf_score' in locals() else "N/A",
                        "Slow Queries": int(total_slow_queries) if 'total_slow_queries' in locals() else 0,
                        "Spilling Queries": int(spilling_queries) if 'spilling_queries' in locals() else 0,
                        "Total Queries": int(total_queries) if 'total_queries' in locals() else 0
                    }

                    insight = ai_insights.generate_insight(
                        str(context),
                        "Analyze the performance metrics and provide 3 specific, prioritized recommendations to improve query performance."
                    )

                    st.markdown(f'<div class="insight-card">{insight}</div>', unsafe_allow_html=True)
            else:
                st.warning("AI insights require Snowflake Cortex Complete access")

        except Exception as e:
            st.warning(f"AI insights temporarily unavailable: {str(e)}")

        st.markdown("---")
        st.markdown("#### 📚 Quick Reference")

        st.markdown("""
        **Performance Best Practices:**

        1. **Warehouse Sizing**
           - Right-size for workload
           - Use multi-cluster for concurrency
           - Auto-suspend/resume

        2. **Query Optimization**
           - Limit result sets
           - Use QUALIFY for window functions
           - Avoid SELECT *

        3. **Table Design**
           - Cluster large tables
           - Use appropriate data types
           - Partition by date when possible

        4. **Monitoring**
           - Set up query profiling
           - Monitor resource usage
           - Review slow queries regularly
        """)

# Footer
st.markdown("---")
st.caption(f"📅 Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | ⏱️ Time period: {time_period} days")
