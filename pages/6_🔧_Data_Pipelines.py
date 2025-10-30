"""
Snowflake Holistic Observability Dashboard - Data Pipelines Page
=================================================================
Monitor Tasks, Snowpipes, Snowpipe Streaming, and Dynamic Tables
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
    page_title="Data Pipelines - Snowflake Observability",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom styling
apply_custom_css()

# Initialize session state and render settings
initialize_session_state()
render_settings_sidebar()

# Page header
render_page_header("🔧 Data Pipeline Monitoring", "Monitor tasks, Snowpipes, streaming, and dynamic tables")

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
# PIPELINE OVERVIEW
# ============================================================================

st.markdown("---")
st.subheader("📊 Pipeline Overview")

col1, col2, col3, col4 = st.columns(4)

with st.spinner("Loading pipeline metrics..."):
    try:
        # Count active tasks
        tasks_query = """
        SELECT
            COUNT(*) AS TOTAL_TASKS,
            SUM(CASE WHEN STATE = 'started' THEN 1 ELSE 0 END) AS ACTIVE_TASKS,
            SUM(CASE WHEN STATE = 'suspended' THEN 1 ELSE 0 END) AS SUSPENDED_TASKS
        FROM SNOWFLAKE.ACCOUNT_USAGE.TASKS
        WHERE DELETED IS NULL
        """
        task_counts = session.sql(tasks_query).to_pandas().iloc[0]

        # Count Snowpipes
        pipes_query = """
        SELECT COUNT(*) AS TOTAL_PIPES
        FROM SNOWFLAKE.ACCOUNT_USAGE.PIPES
        WHERE DELETED IS NULL
        """
        pipe_count = session.sql(pipes_query).to_pandas()['TOTAL_PIPES'].iloc[0]

        # Count Dynamic Tables
        dynamic_tables_query = """
        SELECT COUNT(*) AS TOTAL_DYNAMIC_TABLES
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
        WHERE TABLE_TYPE = 'DYNAMIC'
        AND DELETED IS NULL
        """
        try:
            dynamic_table_count = session.sql(dynamic_tables_query).to_pandas()['TOTAL_DYNAMIC_TABLES'].iloc[0]
        except:
            dynamic_table_count = 0

        # Get task execution stats
        task_exec_query = f"""
        SELECT
            COUNT(DISTINCT QUERY_ID) AS EXECUTIONS,
            SUM(CASE WHEN STATE = 'FAILED' THEN 1 ELSE 0 END) AS FAILURES
        FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
        WHERE SCHEDULED_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        """
        task_exec_stats = session.sql(task_exec_query).to_pandas().iloc[0]

        with col1:
            st.metric(
                "Total Tasks",
                format_number(int(task_counts['TOTAL_TASKS'])),
                delta=f"{int(task_counts['ACTIVE_TASKS'])} active",
                help="Total tasks in account"
            )

        with col2:
            st.metric(
                "Snowpipes",
                format_number(int(pipe_count)),
                help="Active Snowpipe configurations"
            )

        with col3:
            st.metric(
                "Dynamic Tables",
                format_number(int(dynamic_table_count)),
                help="Automatically refreshed materialized views"
            )

        with col4:
            failure_rate = (task_exec_stats['FAILURES'] / max(task_exec_stats['EXECUTIONS'], 1) * 100) if task_exec_stats['EXECUTIONS'] > 0 else 0
            st.metric(
                "Task Failure Rate",
                f"{failure_rate:.1f}%",
                delta_color="inverse",
                help=f"{int(task_exec_stats['FAILURES'])} failures in last {time_period} days"
            )

        # Health indicators
        st.markdown("---")

        if failure_rate > 10:
            create_alert_badge(f"⚠️ High task failure rate ({failure_rate:.1f}%)", "warning")
        elif failure_rate > 5:
            create_alert_badge(f"🟡 Moderate task failure rate ({failure_rate:.1f}%)", "info")
        else:
            create_alert_badge(f"✅ Healthy task execution ({failure_rate:.1f}% failures)", "success")

    except Exception as e:
        st.error(f"Error loading pipeline overview: {str(e)}")

# ============================================================================
# PIPELINE TABS
# ============================================================================

tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Tasks",
    "📥 Snowpipe",
    "🌊 Streaming",
    "🔄 Dynamic Tables"
])

# ----------------------------------------------------------------------------
# TAB 1: Tasks
# ----------------------------------------------------------------------------

with tab1:
    st.markdown("### 📋 Task Monitoring")

    try:
        # Task list
        tasks_list_query = """
        SELECT
            DATABASE_NAME,
            SCHEMA_NAME,
            NAME AS TASK_NAME,
            STATE,
            SCHEDULE,
            WAREHOUSE,
            PREDECESSOR,
            CREATED,
            LAST_COMMITTED_ON
        FROM SNOWFLAKE.ACCOUNT_USAGE.TASKS
        WHERE DELETED IS NULL
        ORDER BY DATABASE_NAME, SCHEMA_NAME, NAME
        """
        tasks_list = session.sql(tasks_list_query).to_pandas()

        if not tasks_list.empty:
            tasks_list['CREATED'] = pd.to_datetime(tasks_list['CREATED'])
            tasks_list['LAST_COMMITTED_ON'] = pd.to_datetime(tasks_list['LAST_COMMITTED_ON'])

            # Summary metrics
            col1, col2, col3 = st.columns(3)

            with col1:
                active_count = len(tasks_list[tasks_list['STATE'] == 'started'])
                st.metric("Active Tasks", active_count)

            with col2:
                suspended_count = len(tasks_list[tasks_list['STATE'] == 'suspended'])
                st.metric("Suspended Tasks", suspended_count)

            with col3:
                scheduled_count = len(tasks_list[tasks_list['SCHEDULE'].notna()])
                st.metric("Scheduled Tasks", scheduled_count)

            st.markdown("---")

            # Task list table
            st.markdown("#### Task Inventory")

            display_df = tasks_list[['DATABASE_NAME', 'SCHEMA_NAME', 'TASK_NAME', 'STATE', 'SCHEDULE', 'WAREHOUSE']].copy()
            display_df.columns = ['Database', 'Schema', 'Task', 'State', 'Schedule', 'Warehouse']

            st.dataframe(
                display_df,
                use_container_width=True,
                height=300
            )

            # Task execution history
            st.markdown("---")
            st.markdown("#### Task Execution History")

            task_history_query = f"""
            SELECT
                DATABASE_NAME,
                SCHEMA_NAME,
                NAME AS TASK_NAME,
                QUERY_ID,
                STATE,
                SCHEDULED_TIME,
                COMPLETED_TIME,
                DATEDIFF('second', SCHEDULED_TIME, COMPLETED_TIME) AS DURATION_SEC,
                ERROR_CODE,
                ERROR_MESSAGE
            FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
            WHERE SCHEDULED_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            ORDER BY SCHEDULED_TIME DESC
            LIMIT 100
            """
            task_history = session.sql(task_history_query).to_pandas()

            if not task_history.empty:
                task_history['SCHEDULED_TIME'] = pd.to_datetime(task_history['SCHEDULED_TIME'])
                task_history['COMPLETED_TIME'] = pd.to_datetime(task_history['COMPLETED_TIME'])

                # Execution statistics
                success_count = len(task_history[task_history['STATE'] == 'SUCCEEDED'])
                failed_count = len(task_history[task_history['STATE'] == 'FAILED'])
                total_executions = len(task_history)
                success_rate = (success_count / total_executions * 100) if total_executions > 0 else 0

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Total Executions", format_number(total_executions))

                with col2:
                    st.metric("Successful", format_number(success_count), delta=f"{success_rate:.1f}%")

                with col3:
                    st.metric("Failed", format_number(failed_count), delta_color="inverse")

                with col4:
                    avg_duration = task_history['DURATION_SEC'].mean()
                    st.metric("Avg Duration", f"{avg_duration:.1f}s")

                # Daily execution trend
                st.markdown("---")
                st.markdown("#### Daily Task Execution Trend")

                daily_executions = task_history.groupby(task_history['SCHEDULED_TIME'].dt.date).agg({
                    'QUERY_ID': 'count',
                    'STATE': lambda x: (x == 'SUCCEEDED').sum(),
                    'DURATION_SEC': 'mean'
                }).reset_index()

                daily_executions.columns = ['DATE', 'TOTAL_RUNS', 'SUCCESSFUL_RUNS', 'AVG_DURATION']
                daily_executions['FAILED_RUNS'] = daily_executions['TOTAL_RUNS'] - daily_executions['SUCCESSFUL_RUNS']
                daily_executions['DATE'] = pd.to_datetime(daily_executions['DATE'])

                fig = go.Figure()

                fig.add_trace(go.Bar(
                    x=daily_executions['DATE'],
                    y=daily_executions['SUCCESSFUL_RUNS'],
                    name='Successful',
                    marker_color='lightgreen'
                ))

                fig.add_trace(go.Bar(
                    x=daily_executions['DATE'],
                    y=daily_executions['FAILED_RUNS'],
                    name='Failed',
                    marker_color='salmon'
                ))

                fig.update_layout(
                    title="Daily Task Executions",
                    xaxis_title="Date",
                    yaxis_title="Execution Count",
                    barmode='stack',
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)

                # Failed task details
                if failed_count > 0:
                    st.markdown("---")
                    st.markdown("#### Failed Task Executions")

                    failed_tasks = task_history[task_history['STATE'] == 'FAILED'].copy()
                    failed_tasks['TASK_PATH'] = failed_tasks['DATABASE_NAME'] + '.' + failed_tasks['SCHEMA_NAME'] + '.' + failed_tasks['TASK_NAME']
                    failed_tasks['ERROR_MESSAGE_SHORT'] = failed_tasks['ERROR_MESSAGE'].str[:100] + '...'

                    display_df = failed_tasks[['TASK_PATH', 'SCHEDULED_TIME', 'ERROR_CODE', 'ERROR_MESSAGE_SHORT']].copy()
                    display_df.columns = ['Task', 'Scheduled Time', 'Error Code', 'Error Message']

                    st.dataframe(
                        display_df.style.format({
                            'Scheduled Time': lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
                        }),
                        use_container_width=True,
                        height=300
                    )

                # Task performance by name
                st.markdown("---")
                st.markdown("#### Task Performance Summary")

                task_perf = task_history.groupby(['DATABASE_NAME', 'SCHEMA_NAME', 'TASK_NAME']).agg({
                    'QUERY_ID': 'count',
                    'STATE': lambda x: (x == 'SUCCEEDED').sum(),
                    'DURATION_SEC': ['mean', 'max']
                }).reset_index()

                task_perf.columns = ['Database', 'Schema', 'Task', 'Total_Runs', 'Successful_Runs', 'Avg_Duration', 'Max_Duration']
                task_perf['Success_Rate'] = (task_perf['Successful_Runs'] / task_perf['Total_Runs'] * 100).round(2)
                task_perf['Task_Path'] = task_perf['Database'] + '.' + task_perf['Schema'] + '.' + task_perf['Task']

                display_df = task_perf[['Task_Path', 'Total_Runs', 'Success_Rate', 'Avg_Duration', 'Max_Duration']].copy()
                display_df.columns = ['Task', 'Runs', 'Success Rate (%)', 'Avg Duration (s)', 'Max Duration (s)']

                st.dataframe(
                    display_df.style.format({
                        'Runs': '{:,}',
                        'Success Rate (%)': '{:.1f}',
                        'Avg Duration (s)': '{:.2f}',
                        'Max Duration (s)': '{:.2f}'
                    }).background_gradient(subset=['Success Rate (%)'], cmap='RdYlGn', vmin=0, vmax=100),
                    use_container_width=True,
                    height=300
                )

            else:
                st.info("No task execution history available")

        else:
            st.info("No tasks found in the account")

    except Exception as e:
        st.error(f"Error loading task data: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 2: Snowpipe
# ----------------------------------------------------------------------------

with tab2:
    st.markdown("### 📥 Snowpipe Monitoring")

    try:
        # Snowpipe list
        pipes_list_query = """
        SELECT
            PIPE_CATALOG AS DATABASE_NAME,
            PIPE_SCHEMA AS SCHEMA_NAME,
            PIPE_NAME,
            PIPE_OWNER,
            DEFINITION,
            CREATED,
            LAST_ALTERED
        FROM SNOWFLAKE.ACCOUNT_USAGE.PIPES
        WHERE DELETED IS NULL
        ORDER BY PIPE_CATALOG, PIPE_SCHEMA, PIPE_NAME
        """
        pipes_list = session.sql(pipes_list_query).to_pandas()

        if not pipes_list.empty:
            pipes_list['CREATED'] = pd.to_datetime(pipes_list['CREATED'])
            pipes_list['LAST_ALTERED'] = pd.to_datetime(pipes_list['LAST_ALTERED'])

            # Snowpipe inventory
            st.markdown("#### Snowpipe Inventory")

            display_df = pipes_list[['DATABASE_NAME', 'SCHEMA_NAME', 'PIPE_NAME', 'PIPE_OWNER', 'CREATED']].copy()
            display_df.columns = ['Database', 'Schema', 'Pipe', 'Owner', 'Created']

            st.dataframe(
                display_df.style.format({
                    'Created': lambda x: x.strftime('%Y-%m-%d')
                }),
                use_container_width=True,
                height=300
            )

            # Snowpipe usage history
            st.markdown("---")
            st.markdown("#### Snowpipe Usage History")

            pipe_usage_query = f"""
            SELECT
                PIPE_NAME,
                DATE_TRUNC('DAY', START_TIME) AS LOAD_DATE,
                COUNT(*) AS FILE_COUNT,
                SUM(FILE_SIZE) AS TOTAL_BYTES,
                SUM(ROW_COUNT) AS TOTAL_ROWS,
                SUM(CREDITS_USED) AS TOTAL_CREDITS
            FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            GROUP BY PIPE_NAME, LOAD_DATE
            ORDER BY LOAD_DATE DESC, TOTAL_CREDITS DESC
            """
            pipe_usage = session.sql(pipe_usage_query).to_pandas()

            if not pipe_usage.empty:
                pipe_usage['LOAD_DATE'] = pd.to_datetime(pipe_usage['LOAD_DATE'])
                pipe_usage['COST'] = pipe_usage['TOTAL_CREDITS'] * credit_cost

                # Summary metrics
                total_files = pipe_usage['FILE_COUNT'].sum()
                total_bytes = pipe_usage['TOTAL_BYTES'].sum()
                total_rows = pipe_usage['TOTAL_ROWS'].sum()
                total_cost = pipe_usage['COST'].sum()

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Files Loaded", format_number(int(total_files)))

                with col2:
                    st.metric("Data Volume", format_bytes(total_bytes))

                with col3:
                    st.metric("Rows Loaded", format_number(int(total_rows)))

                with col4:
                    st.metric("Total Cost", f"${total_cost:,.2f}")

                # Daily load trend
                st.markdown("---")
                st.markdown("#### Daily Load Volume")

                daily_loads = pipe_usage.groupby('LOAD_DATE').agg({
                    'FILE_COUNT': 'sum',
                    'TOTAL_BYTES': 'sum',
                    'TOTAL_ROWS': 'sum',
                    'COST': 'sum'
                }).reset_index()

                fig = go.Figure()

                fig.add_trace(go.Scatter(
                    x=daily_loads['LOAD_DATE'],
                    y=daily_loads['FILE_COUNT'],
                    mode='lines+markers',
                    name='Files',
                    line=dict(color='steelblue', width=3),
                    yaxis='y'
                ))

                fig.add_trace(go.Scatter(
                    x=daily_loads['LOAD_DATE'],
                    y=daily_loads['TOTAL_BYTES'] / (1024**3),  # Convert to GB
                    mode='lines+markers',
                    name='Data (GB)',
                    line=dict(color='orange', width=3),
                    yaxis='y2'
                ))

                fig.update_layout(
                    title="Snowpipe Daily Load Trend",
                    xaxis_title="Date",
                    yaxis=dict(title="File Count"),
                    yaxis2=dict(title="Data Volume (GB)", overlaying='y', side='right'),
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)

                # Pipe performance
                st.markdown("---")
                st.markdown("#### Snowpipe Performance by Pipe")

                pipe_perf = pipe_usage.groupby('PIPE_NAME').agg({
                    'FILE_COUNT': 'sum',
                    'TOTAL_BYTES': 'sum',
                    'TOTAL_ROWS': 'sum',
                    'TOTAL_CREDITS': 'sum',
                    'COST': 'sum'
                }).reset_index()

                pipe_perf = pipe_perf.sort_values('COST', ascending=False)

                display_df = pipe_perf.copy()
                display_df.columns = ['Pipe', 'Files', 'Bytes', 'Rows', 'Credits', 'Cost ($)']

                st.dataframe(
                    display_df.style.format({
                        'Files': '{:,}',
                        'Bytes': lambda x: format_bytes(x),
                        'Rows': '{:,}',
                        'Credits': '{:.4f}',
                        'Cost ($)': '${:,.2f}'
                    }).background_gradient(subset=['Cost ($)'], cmap='YlOrRd'),
                    use_container_width=True,
                    height=300
                )

                # Top pipes chart
                top_10_pipes = pipe_perf.head(10)

                fig = px.bar(
                    top_10_pipes,
                    x='PIPE_NAME',
                    y='COST',
                    title='Top 10 Pipes by Cost',
                    labels={'COST': 'Cost ($)', 'PIPE_NAME': 'Pipe'}
                )

                fig.update_traces(marker_color='lightblue')
                st.plotly_chart(fig, use_container_width=True)

            else:
                st.info("No Snowpipe usage data available")

        else:
            st.info("No Snowpipes found in the account")

    except Exception as e:
        st.error(f"Error loading Snowpipe data: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 3: Streaming
# ----------------------------------------------------------------------------

with tab3:
    st.markdown("### 🌊 Snowpipe Streaming Monitoring")

    try:
        # Check if streaming data exists
        streaming_query = f"""
        SELECT
            TABLE_NAME,
            DATE_TRUNC('DAY', START_TIME) AS STREAM_DATE,
            SUM(BYTES_STREAMED) AS TOTAL_BYTES,
            SUM(ROWS_STREAMED) AS TOTAL_ROWS,
            SUM(CREDITS_USED) AS TOTAL_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        GROUP BY TABLE_NAME, STREAM_DATE
        ORDER BY STREAM_DATE DESC
        """

        try:
            streaming_data = session.sql(streaming_query).to_pandas()
        except:
            streaming_data = pd.DataFrame()

        if not streaming_data.empty:
            streaming_data['STREAM_DATE'] = pd.to_datetime(streaming_data['STREAM_DATE'])
            streaming_data['COST'] = streaming_data['TOTAL_CREDITS'] * credit_cost

            # Summary metrics
            total_bytes = streaming_data['TOTAL_BYTES'].sum()
            total_rows = streaming_data['TOTAL_ROWS'].sum()
            total_cost = streaming_data['COST'].sum()

            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Data Streamed", format_bytes(total_bytes))

            with col2:
                st.metric("Rows Streamed", format_number(int(total_rows)))

            with col3:
                st.metric("Streaming Cost", f"${total_cost:,.2f}")

            st.markdown("---")

            # Daily streaming trend
            st.markdown("#### Daily Streaming Volume")

            daily_streaming = streaming_data.groupby('STREAM_DATE').agg({
                'TOTAL_BYTES': 'sum',
                'TOTAL_ROWS': 'sum',
                'COST': 'sum'
            }).reset_index()

            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=daily_streaming['STREAM_DATE'],
                y=daily_streaming['TOTAL_BYTES'] / (1024**3),  # Convert to GB
                name='Data (GB)',
                marker_color='lightblue'
            ))

            fig.update_layout(
                title="Daily Streaming Volume",
                xaxis_title="Date",
                yaxis_title="Data Volume (GB)",
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

            # Streaming by table
            st.markdown("---")
            st.markdown("#### Streaming by Table")

            table_streaming = streaming_data.groupby('TABLE_NAME').agg({
                'TOTAL_BYTES': 'sum',
                'TOTAL_ROWS': 'sum',
                'TOTAL_CREDITS': 'sum',
                'COST': 'sum'
            }).reset_index()

            table_streaming = table_streaming.sort_values('COST', ascending=False)

            display_df = table_streaming.copy()
            display_df.columns = ['Table', 'Bytes', 'Rows', 'Credits', 'Cost ($)']

            st.dataframe(
                display_df.style.format({
                    'Bytes': lambda x: format_bytes(x),
                    'Rows': '{:,}',
                    'Credits': '{:.4f}',
                    'Cost ($)': '${:,.2f}'
                }),
                use_container_width=True
            )

        else:
            st.info("No Snowpipe Streaming data available. This feature may not be in use or data is not available yet.")

            st.markdown("""
            **About Snowpipe Streaming:**

            Snowpipe Streaming enables low-latency data ingestion to Snowflake with the following benefits:
            - Sub-second latency for data availability
            - Automatic micro-batch loading
            - Pay-per-use pricing based on bytes streamed
            - Ideal for real-time analytics and event streaming

            **To get started:**
            ```python
            # Use Snowflake Ingest SDK
            from snowflake.ingest import SimpleIngestManager
            from snowflake.ingest import StagedFile

            # Configure and stream data
            ingest_manager = SimpleIngestManager(...)
            ingest_manager.ingest_files([staged_file])
            ```
            """)

    except Exception as e:
        st.error(f"Error loading streaming data: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 4: Dynamic Tables
# ----------------------------------------------------------------------------

with tab4:
    st.markdown("### 🔄 Dynamic Tables Monitoring")

    try:
        # Dynamic tables list
        dynamic_tables_list_query = """
        SELECT
            TABLE_CATALOG AS DATABASE_NAME,
            TABLE_SCHEMA AS SCHEMA_NAME,
            TABLE_NAME,
            TABLE_OWNER,
            CREATED,
            LAST_ALTERED,
            ROW_COUNT,
            BYTES
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
        WHERE TABLE_TYPE = 'DYNAMIC'
        AND DELETED IS NULL
        ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
        """

        try:
            dynamic_tables = session.sql(dynamic_tables_list_query).to_pandas()
        except:
            dynamic_tables = pd.DataFrame()

        if not dynamic_tables.empty:
            dynamic_tables['CREATED'] = pd.to_datetime(dynamic_tables['CREATED'])
            dynamic_tables['LAST_ALTERED'] = pd.to_datetime(dynamic_tables['LAST_ALTERED'])

            # Summary metrics
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Dynamic Tables", len(dynamic_tables))

            with col2:
                total_rows = dynamic_tables['ROW_COUNT'].sum()
                st.metric("Total Rows", format_number(int(total_rows)))

            with col3:
                total_bytes = dynamic_tables['BYTES'].sum()
                st.metric("Total Storage", format_bytes(total_bytes))

            st.markdown("---")

            # Dynamic tables inventory
            st.markdown("#### Dynamic Tables Inventory")

            display_df = dynamic_tables[['DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME', 'TABLE_OWNER', 'ROW_COUNT', 'BYTES', 'CREATED']].copy()
            display_df.columns = ['Database', 'Schema', 'Table', 'Owner', 'Rows', 'Bytes', 'Created']

            st.dataframe(
                display_df.style.format({
                    'Rows': '{:,}',
                    'Bytes': lambda x: format_bytes(x),
                    'Created': lambda x: x.strftime('%Y-%m-%d')
                }),
                use_container_width=True,
                height=300
            )

            # Dynamic table refresh history
            st.markdown("---")
            st.markdown("#### Dynamic Table Refresh History")

            refresh_history_query = f"""
            SELECT
                DATABASE_NAME,
                SCHEMA_NAME,
                NAME AS TABLE_NAME,
                REFRESH_START_TIME,
                REFRESH_END_TIME,
                STATE,
                DATEDIFF('second', REFRESH_START_TIME, REFRESH_END_TIME) AS DURATION_SEC,
                CREDITS_USED
            FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY
            WHERE REFRESH_START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            ORDER BY REFRESH_START_TIME DESC
            LIMIT 100
            """

            try:
                refresh_history = session.sql(refresh_history_query).to_pandas()
            except:
                refresh_history = pd.DataFrame()

            if not refresh_history.empty:
                refresh_history['REFRESH_START_TIME'] = pd.to_datetime(refresh_history['REFRESH_START_TIME'])
                refresh_history['REFRESH_END_TIME'] = pd.to_datetime(refresh_history['REFRESH_END_TIME'])
                refresh_history['COST'] = refresh_history['CREDITS_USED'] * credit_cost

                # Refresh statistics
                success_count = len(refresh_history[refresh_history['STATE'] == 'SUCCEEDED'])
                failed_count = len(refresh_history[refresh_history['STATE'] == 'FAILED'])
                total_refreshes = len(refresh_history)
                success_rate = (success_count / total_refreshes * 100) if total_refreshes > 0 else 0

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Total Refreshes", format_number(total_refreshes))

                with col2:
                    st.metric("Success Rate", f"{success_rate:.1f}%")

                with col3:
                    avg_duration = refresh_history['DURATION_SEC'].mean()
                    st.metric("Avg Duration", f"{avg_duration:.1f}s")

                with col4:
                    total_cost = refresh_history['COST'].sum()
                    st.metric("Refresh Cost", f"${total_cost:,.2f}")

                # Daily refresh trend
                daily_refreshes = refresh_history.groupby(refresh_history['REFRESH_START_TIME'].dt.date).agg({
                    'TABLE_NAME': 'count',
                    'STATE': lambda x: (x == 'SUCCEEDED').sum(),
                    'DURATION_SEC': 'mean',
                    'COST': 'sum'
                }).reset_index()

                daily_refreshes.columns = ['DATE', 'TOTAL_REFRESHES', 'SUCCESSFUL', 'AVG_DURATION', 'COST']
                daily_refreshes['FAILED'] = daily_refreshes['TOTAL_REFRESHES'] - daily_refreshes['SUCCESSFUL']
                daily_refreshes['DATE'] = pd.to_datetime(daily_refreshes['DATE'])

                fig = go.Figure()

                fig.add_trace(go.Bar(
                    x=daily_refreshes['DATE'],
                    y=daily_refreshes['SUCCESSFUL'],
                    name='Successful',
                    marker_color='lightgreen'
                ))

                fig.add_trace(go.Bar(
                    x=daily_refreshes['DATE'],
                    y=daily_refreshes['FAILED'],
                    name='Failed',
                    marker_color='salmon'
                ))

                fig.update_layout(
                    title="Daily Dynamic Table Refreshes",
                    xaxis_title="Date",
                    yaxis_title="Refresh Count",
                    barmode='stack',
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)

                # Table refresh performance
                st.markdown("---")
                st.markdown("#### Refresh Performance by Table")

                table_refresh_perf = refresh_history.groupby(['DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME']).agg({
                    'STATE': ['count', lambda x: (x == 'SUCCEEDED').sum()],
                    'DURATION_SEC': 'mean',
                    'CREDITS_USED': 'sum',
                    'COST': 'sum'
                }).reset_index()

                table_refresh_perf.columns = ['Database', 'Schema', 'Table', 'Total_Refreshes', 'Successful', 'Avg_Duration', 'Credits', 'Cost']
                table_refresh_perf['Success_Rate'] = (table_refresh_perf['Successful'] / table_refresh_perf['Total_Refreshes'] * 100).round(2)
                table_refresh_perf['Table_Path'] = table_refresh_perf['Database'] + '.' + table_refresh_perf['Schema'] + '.' + table_refresh_perf['Table']

                display_df = table_refresh_perf[['Table_Path', 'Total_Refreshes', 'Success_Rate', 'Avg_Duration', 'Cost']].copy()
                display_df.columns = ['Table', 'Refreshes', 'Success Rate (%)', 'Avg Duration (s)', 'Cost ($)']

                st.dataframe(
                    display_df.style.format({
                        'Refreshes': '{:,}',
                        'Success Rate (%)': '{:.1f}',
                        'Avg Duration (s)': '{:.2f}',
                        'Cost ($)': '${:,.2f}'
                    }).background_gradient(subset=['Success Rate (%)'], cmap='RdYlGn', vmin=0, vmax=100),
                    use_container_width=True
                )

            else:
                st.info("No refresh history available for Dynamic Tables")

        else:
            st.info("No Dynamic Tables found in the account")

            st.markdown("""
            **About Dynamic Tables:**

            Dynamic Tables are declarative, automatically-refreshed materialized views that simplify data pipelines:

            - **Automatic Refresh:** Define target lag, Snowflake handles the rest
            - **Incremental Processing:** Only processes changed data
            - **Dependency Management:** Automatically tracks upstream dependencies
            - **Cost Efficient:** Pay only for refreshes, not continuous compute

            **Example:**
            ```sql
            CREATE DYNAMIC TABLE sales_summary
            TARGET_LAG = '1 hour'
            WAREHOUSE = compute_wh
            AS
            SELECT
                product_id,
                DATE(sale_time) AS sale_date,
                SUM(amount) AS total_sales
            FROM sales
            GROUP BY product_id, sale_date;
            ```
            """)

    except Exception as e:
        st.error(f"Error loading Dynamic Tables data: {str(e)}")

# Footer
st.markdown("---")
st.caption(f"📅 Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | ⏱️ Time period: {time_period} days | 💵 Credit cost: ${credit_cost}/credit")
