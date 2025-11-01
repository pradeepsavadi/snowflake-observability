"""
Snowflake Observability Dashboard - Shared Utilities
=====================================================
Shared classes, functions, and configurations used across all pages.

This module contains:
- Configuration management with user inputs
- Query functions organized by domain
- AI insights generation using Cortex Complete
- Visualization helper functions
- Common utilities
"""

import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from scipy import stats
import numpy as np

# =============================================================================
# CONFIGURATION & SESSION STATE MANAGEMENT
# =============================================================================

def initialize_session_state():
    """Initialize session state variables with defaults"""
    defaults = {
        'credit_cost': 2.5,
        'storage_cost_per_tb': 23.0,
        'time_period': 30,
        'alert_cost_spike_pct': 50,
        'alert_query_time_sec': 300,
        'alert_failure_rate_pct': 10,
        'alert_freshness_hours': 24,
        'cache_ttl': 3600,
        'max_results': 1000
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

def render_settings_sidebar():
    """Render settings in sidebar for user configuration"""
    initialize_session_state()

    with st.sidebar:
        st.markdown("### ⚙️ Dashboard Settings")

        with st.expander("💰 Cost Configuration", expanded=False):
            st.session_state.credit_cost = st.number_input(
                "Credit Cost ($)",
                min_value=0.0,
                max_value=10.0,
                value=st.session_state.credit_cost,
                step=0.1,
                help="Your Snowflake credit cost in dollars"
            )

            st.session_state.storage_cost_per_tb = st.number_input(
                "Storage Cost ($/TB/month)",
                min_value=0.0,
                max_value=50.0,
                value=st.session_state.storage_cost_per_tb,
                step=1.0,
                help="Your Snowflake storage cost per TB per month"
            )

        with st.expander("📅 Time Period", expanded=True):
            st.session_state.time_period = st.selectbox(
                "Analysis Period",
                [1, 7, 14, 30, 60, 90],
                index=[1, 7, 14, 30, 60, 90].index(st.session_state.time_period),
                format_func=lambda x: f"Last {x} days",
                help="Time period for analysis"
            )

        with st.expander("🚨 Alert Thresholds", expanded=False):
            st.session_state.alert_cost_spike_pct = st.slider(
                "Cost Spike Alert (%)",
                min_value=10,
                max_value=100,
                value=st.session_state.alert_cost_spike_pct,
                step=5,
                help="Percentage increase to trigger cost alert"
            )

            st.session_state.alert_query_time_sec = st.number_input(
                "Long Query Alert (seconds)",
                min_value=60,
                max_value=3600,
                value=st.session_state.alert_query_time_sec,
                step=30,
                help="Query time threshold for alerts"
            )

            st.session_state.alert_failure_rate_pct = st.slider(
                "Failure Rate Alert (%)",
                min_value=1,
                max_value=50,
                value=st.session_state.alert_failure_rate_pct,
                step=1,
                help="Query failure rate threshold"
            )

            st.session_state.alert_freshness_hours = st.number_input(
                "Data Freshness Alert (hours)",
                min_value=1,
                max_value=168,
                value=st.session_state.alert_freshness_hours,
                step=1,
                help="Hours since last update to consider data stale"
            )

        st.markdown("---")
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        # Display current settings
        st.markdown("### 📊 Current Settings")
        st.caption(f"Credit Cost: ${st.session_state.credit_cost}")
        st.caption(f"Storage Cost: ${st.session_state.storage_cost_per_tb}/TB")
        st.caption(f"Time Period: {st.session_state.time_period} days")

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def format_bytes(bytes_val):
    """Format bytes to human-readable format"""
    if bytes_val is None or pd.isna(bytes_val) or bytes_val == 0:
        return "0 B"
    bytes_val = float(bytes_val)
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    for unit in units:
        if bytes_val < 1024 or unit == units[-1]:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024

def format_number(num):
    """Format large numbers with K, M, B suffixes"""
    if num is None or pd.isna(num):
        return "0"
    num = float(num)
    if num >= 1e9:
        return f"{num/1e9:.2f}B"
    elif num >= 1e6:
        return f"{num/1e6:.2f}M"
    elif num >= 1e3:
        return f"{num/1e3:.2f}K"
    return f"{num:.0f}"

def safe_divide(numerator, denominator, default=0):
    """Safe division with default value"""
    if denominator == 0 or pd.isna(denominator) or pd.isna(numerator):
        return default
    return numerator / denominator

def get_snowflake_session():
    """Get active Snowflake session"""
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception as e:
        st.error(f"Failed to get Snowflake session: {str(e)}")
        return None

# =============================================================================
# QUERY FUNCTIONS CLASS
# =============================================================================

class SnowflakeQueries:
    """Centralized query functions for all observability data"""

    def __init__(self, session):
        self.session = session

    # -------------------------------------------------------------------------
    # WAREHOUSE QUERIES
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=3600)
    def get_warehouse_metrics(_self, days):
        """Get comprehensive warehouse usage metrics"""
        query = f"""
        WITH warehouse_usage AS (
            SELECT
                WAREHOUSE_NAME,
                SUM(CREDITS_USED) AS TOTAL_CREDITS,
                SUM(CREDITS_USED) / {days} AS AVG_DAILY_CREDITS,
                MAX(CREDITS_USED) AS MAX_HOURLY_CREDITS,
                COUNT(DISTINCT DATE_TRUNC('DAY', START_TIME)) AS ACTIVE_DAYS
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
            GROUP BY WAREHOUSE_NAME
        ),
        warehouse_load AS (
            SELECT
                WAREHOUSE_NAME,
                AVG(AVG_RUNNING) AS AVG_RUNNING_QUERIES,
                AVG(AVG_QUEUED_LOAD) AS AVG_QUEUED_LOAD,
                AVG(AVG_QUEUED_PROVISIONING) AS AVG_QUEUED_PROVISIONING,
                AVG(AVG_BLOCKED) AS AVG_BLOCKED_QUERIES
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
            GROUP BY WAREHOUSE_NAME
        )
        SELECT
            u.*,
            l.AVG_RUNNING_QUERIES,
            l.AVG_QUEUED_LOAD,
            l.AVG_QUEUED_PROVISIONING,
            l.AVG_BLOCKED_QUERIES,
            (u.TOTAL_CREDITS / NULLIF(u.ACTIVE_DAYS, 0)) AS AVG_CREDITS_PER_ACTIVE_DAY
        FROM warehouse_usage u
        LEFT JOIN warehouse_load l ON u.WAREHOUSE_NAME = l.WAREHOUSE_NAME
        ORDER BY TOTAL_CREDITS DESC
        """
        return _self.session.sql(query).to_pandas()

    @st.cache_data(ttl=3600)
    def get_warehouse_recommendations(_self, days):
        """Generate warehouse optimization recommendations"""
        query = f"""
        WITH warehouse_base AS (
            SELECT DISTINCT
                WAREHOUSE_NAME,
                WAREHOUSE_SIZE
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        ),
        warehouse_stats AS (
            SELECT
                w.WAREHOUSE_NAME,
                w.WAREHOUSE_SIZE,
                COUNT(DISTINCT q.QUERY_ID) AS QUERY_COUNT,
                AVG(q.TOTAL_ELAPSED_TIME)/1000 AS AVG_QUERY_TIME_SEC,
                AVG(q.QUEUED_OVERLOAD_TIME)/1000 AS AVG_QUEUE_TIME_SEC,
                SUM(m.CREDITS_USED) AS TOTAL_CREDITS,
                AVG(l.AVG_RUNNING) AS AVG_CONCURRENT_QUERIES
            FROM warehouse_base w
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
                ON w.WAREHOUSE_NAME = q.WAREHOUSE_NAME
                AND q.START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY m
                ON w.WAREHOUSE_NAME = m.WAREHOUSE_NAME
                AND m.START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY l
                ON w.WAREHOUSE_NAME = l.WAREHOUSE_NAME
                AND l.START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
            GROUP BY w.WAREHOUSE_NAME, w.WAREHOUSE_SIZE
        )
        SELECT
            *,
            CASE
                WHEN AVG_QUEUE_TIME_SEC > 5 THEN 'UPSIZE'
                WHEN AVG_CONCURRENT_QUERIES < 1 AND WAREHOUSE_SIZE IN ('LARGE', 'X-LARGE', '2X-LARGE', '3X-LARGE', '4X-LARGE') THEN 'DOWNSIZE'
                WHEN QUERY_COUNT = 0 THEN 'SUSPEND_OR_DROP'
                ELSE 'OPTIMAL'
            END AS RECOMMENDATION,
            CASE
                WHEN AVG_QUEUE_TIME_SEC > 5 THEN 'High queue times detected - consider increasing warehouse size'
                WHEN AVG_CONCURRENT_QUERIES < 1 AND WAREHOUSE_SIZE IN ('LARGE', 'X-LARGE', '2X-LARGE', '3X-LARGE', '4X-LARGE')
                    THEN 'Low utilization - consider reducing warehouse size'
                WHEN QUERY_COUNT = 0 THEN 'No queries executed - consider suspending or dropping'
                ELSE 'Warehouse is optimally sized'
            END AS REASON
        FROM warehouse_stats
        ORDER BY TOTAL_CREDITS DESC
        """
        return _self.session.sql(query).to_pandas()

    # -------------------------------------------------------------------------
    # STORAGE QUERIES
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=3600)
    def get_storage_metrics(_self, days):
        """Get comprehensive storage metrics"""
        query = f"""
        WITH latest_storage AS (
            SELECT
                DATABASE_NAME,
                SUM(AVERAGE_DATABASE_BYTES) AS DATABASE_BYTES,
                SUM(AVERAGE_FAILSAFE_BYTES) AS FAILSAFE_BYTES,
                SUM(COALESCE(AVERAGE_HYBRID_TABLE_STORAGE_BYTES, 0)) AS HYBRID_TABLE_BYTES,
                MAX(USAGE_DATE) AS LAST_MEASURED
            FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
            WHERE USAGE_DATE >= DATEADD(DAY, -{days}, CURRENT_DATE())
            GROUP BY DATABASE_NAME
        ),
        stage_storage AS (
            SELECT
                COALESCE(SUM(AVERAGE_STAGE_BYTES), 0) AS TOTAL_STAGE_BYTES
            FROM SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY
            WHERE USAGE_DATE >= DATEADD(DAY, -{days}, CURRENT_DATE())
        )
        SELECT
            l.*,
            s.TOTAL_STAGE_BYTES,
            (l.DATABASE_BYTES + l.FAILSAFE_BYTES + l.HYBRID_TABLE_BYTES) AS TOTAL_DATABASE_BYTES
        FROM latest_storage l
        CROSS JOIN stage_storage s
        ORDER BY TOTAL_DATABASE_BYTES DESC
        """
        return _self.session.sql(query).to_pandas()

    @st.cache_data(ttl=3600)
    def get_table_storage_insights(_self):
        """Identify storage optimization opportunities"""
        query = """
        WITH table_metrics AS (
            SELECT
                TABLE_CATALOG AS DATABASE_NAME,
                TABLE_SCHEMA AS SCHEMA_NAME,
                TABLE_NAME,
                ACTIVE_BYTES,
                TIME_TRAVEL_BYTES,
                FAILSAFE_BYTES,
                RETAINED_FOR_CLONE_BYTES,
                (ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES + RETAINED_FOR_CLONE_BYTES) AS TOTAL_BYTES,
                IS_TRANSIENT,
                TABLE_CREATED,
                DELETED
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
        ),
        accessed_tables AS (
            SELECT DISTINCT
                f.value:objectName::STRING AS FULL_TABLE_NAME
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a,
                 LATERAL FLATTEN(input => a.BASE_OBJECTS_ACCESSED) f
            WHERE a.QUERY_START_TIME >= DATEADD(DAY, -90, CURRENT_DATE())
                AND f.value:objectDomain::STRING = 'Table'
        ),
        unused_tables AS (
            SELECT
                t.DATABASE_NAME,
                t.SCHEMA_NAME,
                t.TABLE_NAME,
                t.TOTAL_BYTES,
                'No queries in 90 days' AS ISSUE
            FROM table_metrics t
            LEFT JOIN accessed_tables a
                ON a.FULL_TABLE_NAME = CONCAT(t.DATABASE_NAME, '.', t.SCHEMA_NAME, '.', t.TABLE_NAME)
            WHERE a.FULL_TABLE_NAME IS NULL
                AND t.DELETED = FALSE
                AND t.TOTAL_BYTES > 1073741824
        ),
        high_overhead_tables AS (
            SELECT
                DATABASE_NAME,
                SCHEMA_NAME,
                TABLE_NAME,
                TOTAL_BYTES,
                'High time travel/failsafe overhead' AS ISSUE
            FROM table_metrics
            WHERE (TIME_TRAVEL_BYTES + FAILSAFE_BYTES) > ACTIVE_BYTES * 0.5
                AND DELETED = FALSE
                AND IS_TRANSIENT = 'NO'
        )
        SELECT * FROM unused_tables
        UNION ALL
        SELECT * FROM high_overhead_tables
        ORDER BY TOTAL_BYTES DESC
        LIMIT 100
        """
        return _self.session.sql(query).to_pandas()

    # -------------------------------------------------------------------------
    # AI/ML WORKLOAD QUERIES (CORTEX) - ENHANCED
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=3600)
    def get_cortex_usage(_self, days):
        """Monitor Cortex AI usage across all functions - Enhanced for 2025"""
        queries = {}

        # Cortex Analyst usage
        try:
            queries['analyst'] = _self.session.sql(f"""
                SELECT
                    DATE_TRUNC('DAY', START_TIME) AS USAGE_DATE,
                    SEMANTIC_MODEL_NAME,
                    COUNT(*) AS REQUEST_COUNT,
                    AVG(CREDITS_USED) AS AVG_CREDITS,
                    SUM(CREDITS_USED) AS TOTAL_CREDITS
                FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
                WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
                GROUP BY USAGE_DATE, SEMANTIC_MODEL_NAME
                ORDER BY USAGE_DATE DESC
            """).to_pandas()
        except:
            queries['analyst'] = pd.DataFrame()

        # Cortex Search usage
        try:
            queries['search'] = _self.session.sql(f"""
                SELECT
                    USAGE_DATE,
                    SERVICE_NAME,
                    SUM(NUM_QUERIES) AS TOTAL_QUERIES,
                    SUM(NUM_TOKENS) AS TOTAL_TOKENS
                FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY
                WHERE USAGE_DATE >= DATEADD(DAY, -{days}, CURRENT_DATE())
                GROUP BY USAGE_DATE, SERVICE_NAME
                ORDER BY USAGE_DATE DESC
            """).to_pandas()
        except:
            queries['search'] = pd.DataFrame()

        # Cortex Fine-tuning
        try:
            queries['finetuning'] = _self.session.sql(f"""
                SELECT
                    DATE_TRUNC('DAY', START_TIME) AS USAGE_DATE,
                    USER_NAME,
                    MODEL_NAME,
                    SUM(CREDITS_USED) AS TOTAL_CREDITS,
                    COUNT(*) AS JOB_COUNT
                FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FINETUNING_HISTORY
                WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
                GROUP BY USAGE_DATE, USER_NAME, MODEL_NAME
                ORDER BY USAGE_DATE DESC
            """).to_pandas()
        except:
            queries['finetuning'] = pd.DataFrame()

        # Try Cortex Complete usage (may not exist yet - graceful fallback)
        try:
            queries['complete'] = _self.session.sql(f"""
                SELECT
                    DATE_TRUNC('DAY', START_TIME) AS USAGE_DATE,
                    USER_NAME,
                    MODEL_NAME,
                    COUNT(*) AS REQUEST_COUNT,
                    SUM(TOTAL_TOKENS) AS TOTAL_TOKENS,
                    SUM(PROMPT_TOKENS) AS PROMPT_TOKENS,
                    SUM(COMPLETION_TOKENS) AS COMPLETION_TOKENS,
                    SUM(CREDITS_USED) AS TOTAL_CREDITS
                FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_COMPLETE_USAGE_HISTORY
                WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
                GROUP BY USAGE_DATE, USER_NAME, MODEL_NAME
                ORDER BY USAGE_DATE DESC
            """).to_pandas()
        except:
            # Fallback: try to get from metering history
            try:
                queries['complete'] = _self.session.sql(f"""
                    SELECT
                        DATE_TRUNC('DAY', START_TIME) AS USAGE_DATE,
                        SERVICE_TYPE,
                        SUM(CREDITS_USED) AS TOTAL_CREDITS
                    FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
                    WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
                        AND SERVICE_TYPE LIKE '%CORTEX%'
                    GROUP BY USAGE_DATE, SERVICE_TYPE
                    ORDER BY USAGE_DATE DESC
                """).to_pandas()
            except:
                queries['complete'] = pd.DataFrame()

        return queries

    # -------------------------------------------------------------------------
    # QUERY PERFORMANCE QUERIES
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=3600)
    def get_query_performance_insights(_self, days):
        """Identify query performance issues"""
        query = f"""
        WITH query_metrics AS (
            SELECT
                QUERY_ID,
                QUERY_TEXT,
                USER_NAME,
                ROLE_NAME,
                WAREHOUSE_NAME,
                DATABASE_NAME,
                TOTAL_ELAPSED_TIME/1000 AS ELAPSED_SEC,
                COMPILATION_TIME/1000 AS COMPILATION_SEC,
                EXECUTION_TIME/1000 AS EXECUTION_SEC,
                QUEUED_OVERLOAD_TIME/1000 AS QUEUED_SEC,
                BYTES_SCANNED,
                BYTES_SPILLED_TO_LOCAL_STORAGE,
                BYTES_SPILLED_TO_REMOTE_STORAGE,
                PARTITIONS_SCANNED,
                PARTITIONS_TOTAL,
                START_TIME,
                EXECUTION_STATUS
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        ),
        problematic_queries AS (
            SELECT
                *,
                ARRAY_CONSTRUCT_COMPACT(
                    IFF(ELAPSED_SEC > 300, 'Long running (>5 min)', NULL),
                    IFF(QUEUED_SEC > 60, 'High queue time', NULL),
                    IFF(BYTES_SPILLED_TO_REMOTE_STORAGE > 0, 'Remote spilling', NULL),
                    IFF(BYTES_SPILLED_TO_LOCAL_STORAGE > 1073741824, 'Excessive local spilling', NULL),
                    IFF(COMPILATION_SEC / NULLIF(ELAPSED_SEC, 0) > 0.3, 'High compilation overhead', NULL),
                    IFF(EXECUTION_STATUS != 'SUCCESS', 'Query failed', NULL),
                    IFF(BYTES_SCANNED > 10737418240, 'Excessive data scan (>10GB)', NULL),
                    IFF(PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0) > 0.8 AND PARTITIONS_TOTAL > 100, 'Poor partition pruning', NULL)
                ) AS ISSUES,
                ARRAY_SIZE(
                    ARRAY_CONSTRUCT_COMPACT(
                        IFF(ELAPSED_SEC > 300, 1, NULL),
                        IFF(QUEUED_SEC > 60, 1, NULL),
                        IFF(BYTES_SPILLED_TO_REMOTE_STORAGE > 0, 1, NULL),
                        IFF(BYTES_SPILLED_TO_LOCAL_STORAGE > 1073741824, 1, NULL),
                        IFF(COMPILATION_SEC / NULLIF(ELAPSED_SEC, 0) > 0.3, 1, NULL),
                        IFF(EXECUTION_STATUS != 'SUCCESS', 1, NULL),
                        IFF(BYTES_SCANNED > 10737418240, 1, NULL),
                        IFF(PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0) > 0.8 AND PARTITIONS_TOTAL > 100, 1, NULL)
                    )
                ) AS ISSUE_COUNT
            FROM query_metrics
            WHERE ELAPSED_SEC > 60
                OR QUEUED_SEC > 10
                OR BYTES_SPILLED_TO_REMOTE_STORAGE > 0
                OR BYTES_SPILLED_TO_LOCAL_STORAGE > 1073741824
                OR COMPILATION_SEC / NULLIF(ELAPSED_SEC, 0) > 0.3
                OR EXECUTION_STATUS != 'SUCCESS'
                OR BYTES_SCANNED > 10737418240
                OR (PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0) > 0.8 AND PARTITIONS_TOTAL > 100)
        ),
        issue_summary AS (
            SELECT
                f.value::STRING AS ISSUE_TYPE,
                COUNT(*) AS QUERY_COUNT,
                AVG(p.ELAPSED_SEC) AS AVG_ELAPSED_SEC,
                SUM(p.BYTES_SCANNED) AS TOTAL_BYTES_SCANNED
            FROM problematic_queries p,
                 LATERAL FLATTEN(input => p.ISSUES) f
            GROUP BY ISSUE_TYPE
        )
        SELECT *
        FROM issue_summary
        ORDER BY QUERY_COUNT DESC
        """
        return _self.session.sql(query).to_pandas()

    # -------------------------------------------------------------------------
    # AUTOMATIC CLUSTERING & MATERIALIZED VIEWS
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=3600)
    def get_automatic_clustering_history(_self, days):
        """Monitor automatic clustering costs and efficiency"""
        query = f"""
        SELECT
            TABLE_NAME,
            DATABASE_NAME,
            SCHEMA_NAME,
            DATE_TRUNC('DAY', START_TIME) AS CLUSTER_DATE,
            SUM(CREDITS_USED) AS TOTAL_CREDITS,
            SUM(NUM_BYTES_RECLUSTERED) AS BYTES_RECLUSTERED,
            SUM(NUM_ROWS_RECLUSTERED) AS ROWS_RECLUSTERED,
            COUNT(*) AS RECLUSTERING_RUNS
        FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        GROUP BY TABLE_NAME, DATABASE_NAME, SCHEMA_NAME, CLUSTER_DATE
        ORDER BY TOTAL_CREDITS DESC
        LIMIT 100
        """
        return _self.session.sql(query).to_pandas()

    @st.cache_data(ttl=3600)
    def get_materialized_view_refresh_history(_self, days):
        """Monitor materialized view refresh costs and performance"""
        query = f"""
        SELECT
            NAME AS VIEW_NAME,
            DATABASE_NAME,
            SCHEMA_NAME,
            DATE_TRUNC('DAY', START_TIME) AS REFRESH_DATE,
            COUNT(*) AS REFRESH_COUNT,
            SUM(CREDITS_USED) AS TOTAL_CREDITS,
            AVG(CREDITS_USED) AS AVG_CREDITS_PER_REFRESH,
            AVG(DATEDIFF('SECOND', START_TIME, END_TIME)) AS AVG_DURATION_SEC,
            SUM(BYTES_WRITTEN) AS TOTAL_BYTES_WRITTEN
        FROM SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        GROUP BY NAME, DATABASE_NAME, SCHEMA_NAME, REFRESH_DATE
        ORDER BY TOTAL_CREDITS DESC
        LIMIT 100
        """
        return _self.session.sql(query).to_pandas()

    # -------------------------------------------------------------------------
    # DATA LOADING & COPY HISTORY
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=3600)
    def get_copy_history(_self, days):
        """Monitor COPY INTO command history and performance"""
        query = f"""
        SELECT
            DATE_TRUNC('DAY', LAST_LOAD_TIME) AS LOAD_DATE,
            TABLE_NAME,
            TABLE_CATALOG_NAME AS DATABASE_NAME,
            TABLE_SCHEMA_NAME AS SCHEMA_NAME,
            COUNT(DISTINCT FILE_NAME) AS FILES_LOADED,
            SUM(ROW_COUNT) AS TOTAL_ROWS,
            SUM(ROW_PARSED) AS TOTAL_ROWS_PARSED,
            SUM(FILE_SIZE) AS TOTAL_FILE_SIZE_BYTES,
            SUM(CASE WHEN STATUS = 'LOADED' THEN 1 ELSE 0 END) AS SUCCESSFUL_LOADS,
            SUM(CASE WHEN STATUS != 'LOADED' THEN 1 ELSE 0 END) AS FAILED_LOADS
        FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
        WHERE LAST_LOAD_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        GROUP BY LOAD_DATE, TABLE_NAME, DATABASE_NAME, SCHEMA_NAME
        ORDER BY TOTAL_ROWS DESC
        LIMIT 100
        """
        return _self.session.sql(query).to_pandas()

    @st.cache_data(ttl=3600)
    def get_load_history(_self, days):
        """Monitor Snowpipe and bulk load history"""
        query = f"""
        SELECT
            DATE_TRUNC('DAY', LAST_LOAD_TIME) AS LOAD_DATE,
            TABLE_NAME,
            CATALOG_NAME AS DATABASE_NAME,
            SCHEMA_NAME,
            PIPE_NAME,
            COUNT(*) AS LOAD_EVENTS,
            SUM(ROW_COUNT) AS TOTAL_ROWS,
            SUM(FILE_SIZE) AS TOTAL_FILE_SIZE_BYTES,
            SUM(CASE WHEN STATUS = 'LOADED' THEN 1 ELSE 0 END) AS SUCCESSFUL,
            SUM(CASE WHEN STATUS != 'LOADED' THEN 1 ELSE 0 END) AS FAILED
        FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY
        WHERE LAST_LOAD_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        GROUP BY LOAD_DATE, TABLE_NAME, DATABASE_NAME, SCHEMA_NAME, PIPE_NAME
        ORDER BY TOTAL_ROWS DESC
        LIMIT 100
        """
        return _self.session.sql(query).to_pandas()

    # -------------------------------------------------------------------------
    # SEARCH OPTIMIZATION & REPLICATION
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=3600)
    def get_search_optimization_history(_self, days):
        """Monitor search optimization service costs"""
        query = f"""
        SELECT
            DATE_TRUNC('DAY', START_TIME) AS OPTIMIZATION_DATE,
            TABLE_NAME,
            DATABASE_NAME,
            SCHEMA_NAME,
            SUM(CREDITS_USED) AS TOTAL_CREDITS,
            COUNT(*) AS OPTIMIZATION_RUNS,
            AVG(CREDITS_USED) AS AVG_CREDITS_PER_RUN
        FROM SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        GROUP BY OPTIMIZATION_DATE, TABLE_NAME, DATABASE_NAME, SCHEMA_NAME
        ORDER BY TOTAL_CREDITS DESC
        LIMIT 100
        """
        return _self.session.sql(query).to_pandas()

    @st.cache_data(ttl=3600)
    def get_replication_usage_history(_self, days):
        """Monitor replication and failover group usage"""
        query = f"""
        SELECT
            DATE_TRUNC('DAY', START_TIME) AS REPLICATION_DATE,
            REPLICATION_GROUP_NAME,
            DATABASE_NAME,
            SUM(CREDITS_USED) AS TOTAL_CREDITS,
            SUM(BYTES_TRANSFERRED) AS TOTAL_BYTES_TRANSFERRED,
            COUNT(*) AS REPLICATION_RUNS
        FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        GROUP BY REPLICATION_DATE, REPLICATION_GROUP_NAME, DATABASE_NAME
        ORDER BY TOTAL_CREDITS DESC
        LIMIT 100
        """
        return _self.session.sql(query).to_pandas()

    # -------------------------------------------------------------------------
    # DATA GOVERNANCE & METADATA
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=3600)
    def get_tag_references(_self):
        """Get tag usage across objects for governance tracking"""
        query = """
        SELECT
            TAG_DATABASE,
            TAG_SCHEMA,
            TAG_NAME,
            DOMAIN AS OBJECT_TYPE,
            COUNT(*) AS TAGGED_OBJECTS,
            COUNT(DISTINCT OBJECT_DATABASE || '.' || OBJECT_SCHEMA) AS SCHEMAS_WITH_TAGS
        FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        WHERE TAG_DELETED IS NULL
            AND OBJECT_DELETED IS NULL
        GROUP BY TAG_DATABASE, TAG_SCHEMA, TAG_NAME, DOMAIN
        ORDER BY TAGGED_OBJECTS DESC
        LIMIT 100
        """
        return _self.session.sql(query).to_pandas()

    @st.cache_data(ttl=3600)
    def get_object_dependencies(_self):
        """Analyze object dependencies for impact analysis"""
        query = """
        SELECT
            REFERENCED_DATABASE,
            REFERENCED_SCHEMA,
            REFERENCED_OBJECT_NAME,
            REFERENCED_OBJECT_DOMAIN AS REFERENCED_TYPE,
            COUNT(DISTINCT REFERENCING_OBJECT_NAME) AS DEPENDENT_OBJECTS,
            COUNT(DISTINCT REFERENCING_OBJECT_DOMAIN) AS DEPENDENT_TYPES,
            ARRAY_AGG(DISTINCT REFERENCING_OBJECT_DOMAIN) AS DEPENDENT_OBJECT_TYPES
        FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
        WHERE REFERENCED_OBJECT_DELETED IS NULL
            AND REFERENCING_OBJECT_DELETED IS NULL
        GROUP BY REFERENCED_DATABASE, REFERENCED_SCHEMA, REFERENCED_OBJECT_NAME, REFERENCED_TYPE
        HAVING COUNT(DISTINCT REFERENCING_OBJECT_NAME) > 1
        ORDER BY DEPENDENT_OBJECTS DESC
        LIMIT 100
        """
        return _self.session.sql(query).to_pandas()

    @st.cache_data(ttl=3600)
    def get_policy_references(_self):
        """Monitor security policy assignments"""
        query = """
        SELECT
            POLICY_NAME,
            POLICY_KIND,
            POLICY_DB AS POLICY_DATABASE,
            POLICY_SCHEMA,
            REF_ENTITY_DOMAIN AS PROTECTED_OBJECT_TYPE,
            COUNT(*) AS OBJECTS_PROTECTED,
            COUNT(DISTINCT REF_DATABASE_NAME || '.' || REF_SCHEMA_NAME) AS SCHEMAS_PROTECTED
        FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
        WHERE POLICY_STATUS = 'ACTIVE'
        GROUP BY POLICY_NAME, POLICY_KIND, POLICY_DATABASE, POLICY_SCHEMA, PROTECTED_OBJECT_TYPE
        ORDER BY OBJECTS_PROTECTED DESC
        """
        return _self.session.sql(query).to_pandas()

    # -------------------------------------------------------------------------
    # FUNCTIONS & PROCEDURES
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=3600)
    def get_functions_inventory(_self):
        """Get inventory of user-defined functions"""
        query = """
        SELECT
            FUNCTION_CATALOG AS DATABASE_NAME,
            FUNCTION_SCHEMA AS SCHEMA_NAME,
            FUNCTION_NAME,
            FUNCTION_LANGUAGE,
            IS_SECURE,
            IS_EXTERNAL,
            CREATED,
            LAST_ALTERED
        FROM SNOWFLAKE.ACCOUNT_USAGE.FUNCTIONS
        WHERE DELETED IS NULL
            AND FUNCTION_SCHEMA != 'INFORMATION_SCHEMA'
        ORDER BY LAST_ALTERED DESC
        LIMIT 500
        """
        return _self.session.sql(query).to_pandas()

    @st.cache_data(ttl=3600)
    def get_procedures_inventory(_self):
        """Get inventory of stored procedures"""
        query = """
        SELECT
            PROCEDURE_CATALOG AS DATABASE_NAME,
            PROCEDURE_SCHEMA AS SCHEMA_NAME,
            PROCEDURE_NAME,
            PROCEDURE_LANGUAGE,
            IS_SECURE,
            CREATED,
            LAST_ALTERED
        FROM SNOWFLAKE.ACCOUNT_USAGE.PROCEDURES
        WHERE DELETED IS NULL
            AND PROCEDURE_SCHEMA != 'INFORMATION_SCHEMA'
        ORDER BY LAST_ALTERED DESC
        LIMIT 500
        """
        return _self.session.sql(query).to_pandas()

    # -------------------------------------------------------------------------
    # HYBRID TABLES & ADVANCED FEATURES
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=3600)
    def get_hybrid_table_usage(_self, days):
        """Monitor hybrid table usage and costs"""
        query = f"""
        SELECT
            DATABASE_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            DATE_TRUNC('DAY', USAGE_DATE) AS USAGE_DATE,
            SUM(AVERAGE_ROWS_STORED) AS AVG_ROWS,
            SUM(AVERAGE_DATABASE_BYTES) AS AVG_BYTES,
            SUM(CREDITS_USED_COMPUTE) AS COMPUTE_CREDITS,
            SUM(CREDITS_USED_CLOUD_SERVICES) AS CLOUD_SERVICES_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.HYBRID_TABLE_USAGE_HISTORY
        WHERE USAGE_DATE >= DATEADD(DAY, -{days}, CURRENT_DATE())
        GROUP BY DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, USAGE_DATE
        ORDER BY COMPUTE_CREDITS DESC
        LIMIT 100
        """
        return _self.session.sql(query).to_pandas()

    # -------------------------------------------------------------------------
    # AGGREGATE QUERY HISTORY (Performance Optimized)
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=3600)
    def get_aggregate_query_metrics(_self, days):
        """Get pre-aggregated query metrics for faster performance"""
        query = f"""
        SELECT
            DATE_TRUNC('HOUR', START_TIME) AS QUERY_HOUR,
            WAREHOUSE_NAME,
            USER_NAME,
            QUERY_TYPE,
            COUNT(*) AS QUERY_COUNT,
            SUM(EXECUTION_TIME) / 1000 AS TOTAL_EXECUTION_SEC,
            AVG(EXECUTION_TIME) / 1000 AS AVG_EXECUTION_SEC,
            SUM(BYTES_SCANNED) AS TOTAL_BYTES_SCANNED,
            SUM(CREDITS_USED_CLOUD_SERVICES) AS CLOUD_SERVICES_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.AGGREGATE_QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        GROUP BY QUERY_HOUR, WAREHOUSE_NAME, USER_NAME, QUERY_TYPE
        ORDER BY QUERY_HOUR DESC, QUERY_COUNT DESC
        LIMIT 1000
        """
        return _self.session.sql(query).to_pandas()

    # Add more query methods as needed...
    # (Include all other methods from the original SnowflakeQueries class)

# =============================================================================
# AI INSIGHTS GENERATOR - ENHANCED
# =============================================================================

class AIInsightsGenerator:
    """Generate AI-powered insights using Snowflake Cortex Complete - Enhanced"""

    def __init__(self, session):
        self.session = session
        self.default_model = 'mistral-large2'
        self.temperature = 0.3
        self.max_tokens = 1000

    def check_cortex_availability(self):
        """Check if Cortex Complete is available"""
        try:
            # Try to call Cortex Complete with a simple test
            test_query = """
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-7b',
                'Test'
            ) AS test_response
            """
            result = self.session.sql(test_query).collect()
            return True
        except Exception as e:
            # Cortex not available or not authorized
            return False

    def generate_insight(self, context_data, insight_type="summary", custom_prompt=None):
        """Generate AI insights using Cortex Complete"""
        try:
            # Use custom prompt if provided, otherwise use preset
            if custom_prompt:
                prompt = custom_prompt
            elif insight_type == "warehouse_optimization":
                prompt = f"""
                Analyze the following Snowflake warehouse metrics and provide 3-5 actionable optimization recommendations:

                {context_data}

                Focus on: cost savings, performance improvements, and right-sizing opportunities.
                Keep recommendations specific, practical, and prioritized by potential impact.
                """
            elif insight_type == "cost_summary":
                prompt = f"""
                Summarize the following Snowflake cost data and highlight:
                1. Key cost drivers
                2. Unusual spending patterns
                3. Top 3 cost optimization opportunities

                Data: {context_data}

                Be concise and actionable.
                """
            elif insight_type == "performance_analysis":
                prompt = f"""
                Analyze these query performance metrics and identify:
                1. Main performance bottlenecks
                2. Queries that need immediate attention
                3. Recommended optimizations

                Metrics: {context_data}

                Prioritize by impact on user experience and cost.
                """
            else:  # summary
                prompt = f"""
                Provide a concise executive summary of this Snowflake observability data:

                {context_data}

                Highlight: key metrics, trends, and top 3 action items.
                """

            # Escape single quotes for SQL
            prompt_escaped = prompt.replace("'", "''")

            # Call Cortex Complete
            query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{self.default_model}',
                [
                    {{'role': 'system', 'content': 'You are a Snowflake optimization expert providing concise, actionable insights.'}},
                    {{'role': 'user', 'content': '{prompt_escaped}'}}
                ],
                {{
                    'temperature': {self.temperature},
                    'max_tokens': {self.max_tokens}
                }}
            ) AS INSIGHT
            """

            result = self.session.sql(query).collect()
            if result:
                return result[0]['INSIGHT']
            return "Unable to generate AI insight at this time."

        except Exception as e:
            return f"AI insights temporarily unavailable: {str(e)}"

    def generate_custom_insight(self, user_prompt, context_data=None):
        """Generate insights based on user's custom prompt"""
        if context_data:
            full_prompt = f"{user_prompt}\n\nContext data:\n{context_data}"
        else:
            full_prompt = user_prompt

        return self.generate_insight(full_prompt, custom_prompt=full_prompt)

# =============================================================================
# VISUALIZATION HELPER FUNCTIONS
# =============================================================================

def create_metric_card(label, value, delta=None, delta_color="normal"):
    """Create a styled metric card"""
    st.metric(label=label, value=value, delta=delta, delta_color=delta_color)

def create_trend_chart(data, x_col, y_col, title="Trend Analysis", height=300):
    """Create a standardized trend line chart"""
    if data.empty:
        st.info("No data available for trend chart")
        return None

    chart = alt.Chart(data).mark_line(point=True).encode(
        x=alt.X(f'{x_col}:T', title=x_col),
        y=alt.Y(f'{y_col}:Q', title=y_col),
        tooltip=[f'{x_col}:T', alt.Tooltip(f'{y_col}:Q', format=',.2f')]
    ).properties(
        title=title,
        height=height
    ).interactive()

    return chart

def create_bar_chart(data, x_col, y_col, color_col=None, title="Bar Chart", height=300):
    """Create a standardized bar chart"""
    if data.empty:
        st.info("No data available for bar chart")
        return None

    encoding = {
        'y': alt.Y(f'{x_col}:N', sort='-x', title=x_col),
        'x': alt.X(f'{y_col}:Q', title=y_col),
        'tooltip': [x_col, alt.Tooltip(f'{y_col}:Q', format=',.2f')]
    }

    if color_col:
        encoding['color'] = alt.Color(f'{color_col}:Q', scale=alt.Scale(scheme='blues'))

    chart = alt.Chart(data).mark_bar().encode(**encoding).properties(
        title=title,
        height=height
    )

    return chart

def create_alert_badge(message, alert_type="info"):
    """Create alert badges for important notifications"""
    colors = {
        "info": "#D1ECF1",
        "warning": "#FFF3CD",
        "error": "#F8D7DA",
        "success": "#D4EDDA"
    }

    border_colors = {
        "info": "#17A2B8",
        "warning": "#FFC107",
        "error": "#DC3545",
        "success": "#28A745"
    }

    st.markdown(
        f"""
        <div style="background-color: {colors.get(alert_type, colors['info'])};
                    padding: 12px; border-radius: 5px; margin: 8px 0;
                    border-left: 4px solid {border_colors.get(alert_type, border_colors['info'])};">
            {message}
        </div>
        """,
        unsafe_allow_html=True
    )

def apply_custom_css():
    """Apply custom CSS for professional look"""
    st.markdown("""
    <style>
    /* Main theme colors */
    :root {
        --primary-color: #29B5E8;
        --secondary-color: #1E88E5;
        --success-color: #28A745;
        --warning-color: #FFC107;
        --danger-color: #DC3545;
        --info-color: #17A2B8;
    }

    /* Header styling */
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: var(--primary-color);
        margin-bottom: 0.5rem;
        text-align: center;
    }

    .sub-header {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
        text-align: center;
    }

    .page-header {
        font-size: 2rem;
        font-weight: 600;
        color: var(--secondary-color);
        margin-bottom: 1rem;
        border-bottom: 2px solid var(--primary-color);
        padding-bottom: 0.5rem;
    }

    /* Metric cards */
    .metric-container {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        border-radius: 8px;
        padding: 20px;
        margin: 10px 0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        transition: transform 0.2s;
    }

    .metric-container:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(0,0,0,0.15);
    }

    /* Alert boxes */
    .alert-box {
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
        border-left: 4px solid;
        animation: slideIn 0.3s ease-in;
    }

    @keyframes slideIn {
        from {
            opacity: 0;
            transform: translateX(-10px);
        }
        to {
            opacity: 1;
            transform: translateX(0);
        }
    }

    /* Table styling */
    .dataframe {
        font-size: 0.9rem;
    }

    .dataframe thead tr th {
        background-color: var(--primary-color) !important;
        color: white !important;
        font-weight: 600;
    }

    .dataframe tbody tr:hover {
        background-color: #f5f5f5;
    }

    /* Button styling */
    .stButton button {
        background-color: var(--primary-color);
        color: white;
        border-radius: 5px;
        border: none;
        padding: 0.5rem 1rem;
        font-weight: 500;
        transition: all 0.3s;
    }

    .stButton button:hover {
        background-color: var(--secondary-color);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }

    /* Expander styling */
    .streamlit-expanderHeader {
        background-color: #f0f2f6;
        border-radius: 5px;
        font-weight: 600;
    }

    /* Sidebar styling */
    .css-1d391kg {
        background-color: #f8f9fa;
    }

    /* Card containers */
    .insight-card {
        background: white;
        border-radius: 10px;
        padding: 20px;
        margin: 15px 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        border: 1px solid #e0e0e0;
    }

    /* Loading spinner */
    .stSpinner > div {
        border-top-color: var(--primary-color) !important;
    }
    </style>
    """, unsafe_allow_html=True)

def render_page_header(title, subtitle=None, icon=None):
    """Render consistent page headers"""
    if icon:
        st.markdown(f'<p class="page-header">{icon} {title}</p>', unsafe_allow_html=True)
    else:
        st.markdown(f'<p class="page-header">{title}</p>', unsafe_allow_html=True)

    if subtitle:
        st.markdown(f'<p class="sub-header">{subtitle}</p>', unsafe_allow_html=True)
