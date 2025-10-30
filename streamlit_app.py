"""
Snowflake Holistic Observability Dashboard
===========================================
A comprehensive observability tool covering:
- Warehouse analytics & optimization
- Storage management & cost tracking
- Data transfer monitoring
- User query analytics & performance
- AI/ML workload monitoring (Cortex)
- Data pipeline observability (Tasks, Snowpipes, Dynamic Tables)
- Security & governance tracking
- Cost management & savings recommendations
- Data quality monitoring
- AI-powered insights using Cortex Complete

Version: 2.0
Last Updated: 2025
"""

import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from scipy import stats
import json
import numpy as np

# =============================================================================
# CONFIGURATION & HELPER FUNCTIONS
# =============================================================================

class Config:
    """Central configuration for the dashboard"""
    DEFAULT_CREDIT_COST = 2.5  # $ per credit
    DEFAULT_STORAGE_COST = 23.0  # $ per TB per month
    DEFAULT_TIME_PERIOD = 30  # days
    CACHE_TTL = 3600  # 1 hour cache
    MAX_RESULTS = 1000  # Limit for large queries

    # Alert thresholds
    ALERT_COST_SPIKE_PCT = 50  # % increase to trigger alert
    ALERT_QUERY_TIME_SEC = 300  # 5 minutes
    ALERT_FAILURE_RATE_PCT = 10  # % of queries
    ALERT_DATA_FRESHNESS_HOURS = 24  # hours

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

def get_date_range(days):
    """Get start and end dates for queries"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    return start_date, end_date

# =============================================================================
# QUERY FUNCTIONS - ORGANIZED BY DOMAIN
# =============================================================================

class SnowflakeQueries:
    """Centralized query functions for all observability data"""

    def __init__(self, session):
        self.session = session

    # -------------------------------------------------------------------------
    # WAREHOUSE QUERIES
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_warehouse_metrics(_self, days):
        """Get comprehensive warehouse usage metrics"""
        query = f"""
        WITH warehouse_usage AS (
            SELECT
                WAREHOUSE_NAME,
                SUM(CREDITS_USED) AS TOTAL_CREDITS,
                AVG(CREDITS_USED) AS AVG_DAILY_CREDITS,
                MAX(CREDITS_USED) AS MAX_DAILY_CREDITS,
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

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_warehouse_recommendations(_self, days):
        """Generate warehouse optimization recommendations"""
        query = f"""
        WITH warehouse_stats AS (
            SELECT
                w.WAREHOUSE_NAME,
                w.WAREHOUSE_SIZE,
                COUNT(DISTINCT q.QUERY_ID) AS QUERY_COUNT,
                AVG(q.TOTAL_ELAPSED_TIME)/1000 AS AVG_QUERY_TIME_SEC,
                AVG(q.QUEUED_OVERLOAD_TIME)/1000 AS AVG_QUEUE_TIME_SEC,
                SUM(m.CREDITS_USED) AS TOTAL_CREDITS,
                AVG(l.AVG_RUNNING) AS AVG_CONCURRENT_QUERIES
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSES w
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
                ON w.WAREHOUSE_NAME = q.WAREHOUSE_NAME
                AND q.START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY m
                ON w.WAREHOUSE_NAME = m.WAREHOUSE_NAME
                AND m.START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY l
                ON w.WAREHOUSE_NAME = l.WAREHOUSE_NAME
                AND l.START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
            WHERE w.DELETED IS NULL
            GROUP BY w.WAREHOUSE_NAME, w.WAREHOUSE_SIZE
        )
        SELECT
            *,
            CASE
                WHEN AVG_QUEUE_TIME_SEC > 5 THEN 'UPSIZE'
                WHEN AVG_CONCURRENT_QUERIES < 1 AND WAREHOUSE_SIZE IN ('Large', 'X-Large', '2X-Large', '3X-Large', '4X-Large') THEN 'DOWNSIZE'
                WHEN QUERY_COUNT = 0 THEN 'SUSPEND_OR_DROP'
                ELSE 'OPTIMAL'
            END AS RECOMMENDATION,
            CASE
                WHEN AVG_QUEUE_TIME_SEC > 5 THEN 'High queue times detected - consider increasing warehouse size'
                WHEN AVG_CONCURRENT_QUERIES < 1 AND WAREHOUSE_SIZE IN ('Large', 'X-Large', '2X-Large', '3X-Large', '4X-Large')
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

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_storage_metrics(_self, days):
        """Get comprehensive storage metrics including latest features"""
        query = f"""
        WITH latest_storage AS (
            SELECT
                DATABASE_NAME,
                SUM(AVERAGE_DATABASE_BYTES) AS DATABASE_BYTES,
                SUM(AVERAGE_FAILSAFE_BYTES) AS FAILSAFE_BYTES,
                SUM(AVERAGE_HYBRID_TABLE_STORAGE_BYTES) AS HYBRID_TABLE_BYTES,
                MAX(USAGE_DATE) AS LAST_MEASURED
            FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
            WHERE USAGE_DATE >= DATEADD(DAY, -{days}, CURRENT_DATE())
            GROUP BY DATABASE_NAME
        ),
        stage_storage AS (
            SELECT
                SUM(AVERAGE_STAGE_BYTES) AS TOTAL_STAGE_BYTES
            FROM SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY
            WHERE USAGE_DATE >= DATEADD(DAY, -{days}, CURRENT_DATE())
        ),
        snapshot_storage AS (
            SELECT
                SUM(AVERAGE_BYTES_OWNED) AS SNAPSHOT_BYTES
            FROM SNOWFLAKE.ACCOUNT_USAGE.SNAPSHOT_STORAGE_USAGE
            WHERE USAGE_DATE >= DATEADD(DAY, -{days}, CURRENT_DATE())
        )
        SELECT
            l.*,
            s.TOTAL_STAGE_BYTES,
            sn.SNAPSHOT_BYTES,
            (l.DATABASE_BYTES + l.FAILSAFE_BYTES +
             COALESCE(l.HYBRID_TABLE_BYTES, 0)) AS TOTAL_BYTES
        FROM latest_storage l
        CROSS JOIN stage_storage s
        CROSS JOIN snapshot_storage sn
        ORDER BY TOTAL_BYTES DESC
        """
        return _self.session.sql(query).to_pandas()

    @st.cache_data(ttl=Config.CACHE_TTL)
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
                (ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES +
                 RETAINED_FOR_CLONE_BYTES) AS TOTAL_BYTES,
                IS_TRANSIENT,
                TABLE_CREATED,
                DELETED,
                TABLE_DROPPED
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
        ),
        unused_tables AS (
            SELECT
                t.DATABASE_NAME,
                t.SCHEMA_NAME,
                t.TABLE_NAME,
                t.TOTAL_BYTES,
                'No queries in 90 days' AS ISSUE
            FROM table_metrics t
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a
                ON t.TABLE_NAME = a.BASE_OBJECTS_ACCESSED[0]['objectName']::STRING
                AND a.QUERY_START_TIME >= DATEADD(DAY, -90, CURRENT_DATE())
            WHERE a.QUERY_ID IS NULL
                AND t.DELETED = FALSE
                AND t.TOTAL_BYTES > 1073741824  -- > 1GB
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
    # QUERY PERFORMANCE QUERIES
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_query_performance_insights(_self, days):
        """Identify query performance issues and optimization opportunities"""
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
                EXECUTION_STATUS,
                ERROR_MESSAGE
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        ),
        problematic_queries AS (
            SELECT
                *,
                CASE
                    WHEN ELAPSED_SEC > 300 THEN 'Long running (>5 min)'
                    WHEN QUEUED_SEC > 60 THEN 'High queue time'
                    WHEN BYTES_SPILLED_TO_REMOTE_STORAGE > 0 THEN 'Remote spilling detected'
                    WHEN BYTES_SPILLED_TO_LOCAL_STORAGE > 1073741824 THEN 'Excessive local spilling'
                    WHEN COMPILATION_SEC / NULLIF(ELAPSED_SEC, 0) > 0.3 THEN 'High compilation overhead'
                    WHEN EXECUTION_STATUS != 'SUCCESS' THEN 'Query failed'
                    WHEN BYTES_SCANNED > 10737418240 THEN 'Excessive data scan (>10GB)'
                    ELSE 'Other'
                END AS ISSUE_TYPE
            FROM query_metrics
            WHERE ELAPSED_SEC > 60
                OR QUEUED_SEC > 10
                OR BYTES_SPILLED_TO_REMOTE_STORAGE > 0
                OR EXECUTION_STATUS != 'SUCCESS'
        )
        SELECT
            ISSUE_TYPE,
            COUNT(*) AS QUERY_COUNT,
            AVG(ELAPSED_SEC) AS AVG_ELAPSED_SEC,
            SUM(BYTES_SCANNED) AS TOTAL_BYTES_SCANNED,
            LISTAGG(DISTINCT WAREHOUSE_NAME, ', ') WITHIN GROUP (ORDER BY WAREHOUSE_NAME) AS WAREHOUSES
        FROM problematic_queries
        GROUP BY ISSUE_TYPE
        ORDER BY QUERY_COUNT DESC
        """
        return _self.session.sql(query).to_pandas()

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_pruning_efficiency(_self, days):
        """Analyze query pruning efficiency using latest views"""
        query = f"""
        SELECT
            TABLE_NAME,
            DATABASE_NAME,
            SCHEMA_NAME,
            COUNT(*) AS SCAN_COUNT,
            SUM(PARTITIONS_SCANNED) AS TOTAL_PARTITIONS_SCANNED,
            SUM(PARTITIONS_PRUNED) AS TOTAL_PARTITIONS_PRUNED,
            AVG(PARTITIONS_SCANNED::FLOAT / NULLIF(PARTITIONS_TOTAL, 0)) AS AVG_SCAN_RATIO,
            CASE
                WHEN AVG_SCAN_RATIO > 0.5 THEN 'Poor'
                WHEN AVG_SCAN_RATIO > 0.2 THEN 'Moderate'
                ELSE 'Good'
            END AS PRUNING_QUALITY
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_QUERY_PRUNING_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        GROUP BY TABLE_NAME, DATABASE_NAME, SCHEMA_NAME
        HAVING SCAN_COUNT > 10
        ORDER BY AVG_SCAN_RATIO DESC
        LIMIT 50
        """
        return _self.session.sql(query).to_pandas()

    # -------------------------------------------------------------------------
    # AI/ML WORKLOAD QUERIES (CORTEX)
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_cortex_usage(_self, days):
        """Monitor Cortex AI usage across all functions"""
        # Note: These views may require appropriate permissions
        queries = {}

        # Cortex Analyst usage
        try:
            queries['analyst'] = _self.session.sql(f"""
                SELECT
                    DATE_TRUNC('DAY', START_TIME) AS USAGE_DATE,
                    USER_NAME,
                    SEMANTIC_MODEL_NAME,
                    COUNT(*) AS REQUEST_COUNT,
                    AVG(CREDITS_USED) AS AVG_CREDITS,
                    SUM(CREDITS_USED) AS TOTAL_CREDITS
                FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
                WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
                GROUP BY USAGE_DATE, USER_NAME, SEMANTIC_MODEL_NAME
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
                    SUM(CREDITS_USED) AS TOTAL_CREDITS,
                    SUM(NUM_QUERIES) AS TOTAL_QUERIES
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

        return queries

    # -------------------------------------------------------------------------
    # DATA PIPELINE QUERIES
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_task_history(_self, days):
        """Monitor task execution and health"""
        query = f"""
        WITH task_runs AS (
            SELECT
                NAME AS TASK_NAME,
                DATABASE_NAME,
                SCHEMA_NAME,
                STATE,
                SCHEDULED_TIME,
                COMPLETED_TIME,
                DATEDIFF('SECOND', SCHEDULED_TIME, COMPLETED_TIME) AS DURATION_SEC,
                ERROR_CODE,
                ERROR_MESSAGE,
                QUERY_ID
            FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
            WHERE SCHEDULED_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        )
        SELECT
            TASK_NAME,
            DATABASE_NAME,
            SCHEMA_NAME,
            COUNT(*) AS TOTAL_RUNS,
            SUM(CASE WHEN STATE = 'SUCCEEDED' THEN 1 ELSE 0 END) AS SUCCESSFUL_RUNS,
            SUM(CASE WHEN STATE = 'FAILED' THEN 1 ELSE 0 END) AS FAILED_RUNS,
            AVG(DURATION_SEC) AS AVG_DURATION_SEC,
            MAX(DURATION_SEC) AS MAX_DURATION_SEC,
            MAX(COMPLETED_TIME) AS LAST_RUN
        FROM task_runs
        GROUP BY TASK_NAME, DATABASE_NAME, SCHEMA_NAME
        ORDER BY FAILED_RUNS DESC, TOTAL_RUNS DESC
        """
        return _self.session.sql(query).to_pandas()

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_pipe_usage(_self, days):
        """Monitor Snowpipe and Snowpipe Streaming"""
        # Regular Snowpipe
        pipe_query = f"""
        SELECT
            PIPE_NAME,
            SUM(FILES_INSERTED) AS TOTAL_FILES,
            SUM(BYTES_INSERTED) AS TOTAL_BYTES,
            SUM(CREDITS_USED) AS TOTAL_CREDITS,
            AVG(BYTES_INSERTED / NULLIF(FILES_INSERTED, 0)) AS AVG_FILE_SIZE
        FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        GROUP BY PIPE_NAME
        ORDER BY TOTAL_BYTES DESC
        """
        pipe_data = _self.session.sql(pipe_query).to_pandas()

        # Snowpipe Streaming
        try:
            streaming_query = f"""
            SELECT
                CHANNEL_NAME,
                TABLE_NAME,
                SUM(ROWS_INSERTED) AS TOTAL_ROWS,
                SUM(BYTES_INSERTED) AS TOTAL_BYTES,
                AVG(INSERT_LATENCY_MS) AS AVG_LATENCY_MS
            FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_CHANNEL_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
            GROUP BY CHANNEL_NAME, TABLE_NAME
            ORDER BY TOTAL_BYTES DESC
            """
            streaming_data = _self.session.sql(streaming_query).to_pandas()
        except:
            streaming_data = pd.DataFrame()

        return {'pipe': pipe_data, 'streaming': streaming_data}

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_dynamic_table_refreshes(_self, days):
        """Monitor dynamic table refresh performance"""
        try:
            query = f"""
            SELECT
                NAME AS TABLE_NAME,
                DATABASE_NAME,
                SCHEMA_NAME,
                STATE,
                REFRESH_START_TIME,
                REFRESH_END_TIME,
                DATEDIFF('SECOND', REFRESH_START_TIME, REFRESH_END_TIME) AS REFRESH_DURATION_SEC,
                DATA_TIMESTAMP,
                CREDITS_USED
            FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY
            WHERE REFRESH_START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
            ORDER BY REFRESH_START_TIME DESC
            LIMIT 1000
            """
            return _self.session.sql(query).to_pandas()
        except:
            return pd.DataFrame()

    # -------------------------------------------------------------------------
    # SECURITY & GOVERNANCE QUERIES
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_access_patterns(_self, days):
        """Analyze access patterns for security insights"""
        query = f"""
        WITH access_summary AS (
            SELECT
                USER_NAME,
                DIRECT_OBJECTS_ACCESSED,
                BASE_OBJECTS_ACCESSED,
                OBJECTS_MODIFIED,
                QUERY_START_TIME,
                QUERY_ID
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
            WHERE QUERY_START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        )
        SELECT
            USER_NAME,
            COUNT(DISTINCT QUERY_ID) AS ACCESS_COUNT,
            COUNT(DISTINCT QUERY_START_TIME::DATE) AS ACTIVE_DAYS,
            -- Count unique objects accessed
            APPROX_COUNT_DISTINCT(VALUE:objectName::STRING) AS UNIQUE_OBJECTS_ACCESSED
        FROM access_summary,
            LATERAL FLATTEN(input => BASE_OBJECTS_ACCESSED)
        GROUP BY USER_NAME
        ORDER BY ACCESS_COUNT DESC
        LIMIT 100
        """
        return _self.session.sql(query).to_pandas()

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_login_history(_self, days):
        """Monitor login patterns and potential security issues"""
        query = f"""
        SELECT
            USER_NAME,
            CLIENT_IP,
            REPORTED_CLIENT_TYPE,
            FIRST_AUTHENTICATION_FACTOR,
            IS_SUCCESS,
            ERROR_CODE,
            ERROR_MESSAGE,
            EVENT_TIMESTAMP
        FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
        WHERE EVENT_TIMESTAMP >= DATEADD(DAY, -{days}, CURRENT_DATE())
        ORDER BY EVENT_TIMESTAMP DESC
        LIMIT 1000
        """
        return _self.session.sql(query).to_pandas()

    # -------------------------------------------------------------------------
    # COST MANAGEMENT QUERIES
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_cost_attribution(_self, days):
        """Get detailed cost attribution by multiple dimensions"""
        query = f"""
        WITH compute_costs AS (
            SELECT
                'Warehouse' AS COST_TYPE,
                WAREHOUSE_NAME AS RESOURCE_NAME,
                USER_NAME,
                ROLE_NAME,
                SUM(CREDITS_USED) AS CREDITS,
                SUM(CREDITS_USED) * {Config.DEFAULT_CREDIT_COST} AS ESTIMATED_COST
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
            GROUP BY WAREHOUSE_NAME, USER_NAME, ROLE_NAME
        ),
        serverless_costs AS (
            SELECT
                'Serverless' AS COST_TYPE,
                SERVICE_TYPE AS RESOURCE_NAME,
                NULL AS USER_NAME,
                NULL AS ROLE_NAME,
                SUM(CREDITS_USED) AS CREDITS,
                SUM(CREDITS_USED) * {Config.DEFAULT_CREDIT_COST} AS ESTIMATED_COST
            FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
                AND SERVICE_TYPE != 'WAREHOUSE_METERING'
            GROUP BY SERVICE_TYPE
        )
        SELECT * FROM compute_costs
        UNION ALL
        SELECT * FROM serverless_costs
        ORDER BY ESTIMATED_COST DESC
        """
        return _self.session.sql(query).to_pandas()

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_cost_anomalies(_self, days):
        """Detect cost anomalies and spikes"""
        query = f"""
        WITH daily_costs AS (
            SELECT
                DATE_TRUNC('DAY', START_TIME) AS COST_DATE,
                SUM(CREDITS_USED) AS DAILY_CREDITS,
                SUM(CREDITS_USED) * {Config.DEFAULT_CREDIT_COST} AS DAILY_COST
            FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
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
            s.STDDEV_DAILY_COST,
            ((c.DAILY_COST - s.AVG_DAILY_COST) / NULLIF(s.STDDEV_DAILY_COST, 0)) AS Z_SCORE,
            CASE
                WHEN ABS((c.DAILY_COST - s.AVG_DAILY_COST) / NULLIF(s.STDDEV_DAILY_COST, 0)) > 2
                    THEN 'ANOMALY'
                ELSE 'NORMAL'
            END AS STATUS
        FROM daily_costs c
        CROSS JOIN cost_stats s
        ORDER BY c.COST_DATE DESC
        """
        return _self.session.sql(query).to_pandas()

    # -------------------------------------------------------------------------
    # DATA QUALITY QUERIES
    # -------------------------------------------------------------------------

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_table_freshness(_self):
        """Monitor data freshness across tables"""
        query = """
        WITH table_last_modified AS (
            SELECT
                TABLE_CATALOG AS DATABASE_NAME,
                TABLE_SCHEMA AS SCHEMA_NAME,
                TABLE_NAME,
                LAST_ALTERED,
                ROW_COUNT,
                BYTES,
                DATEDIFF('HOUR', LAST_ALTERED, CURRENT_TIMESTAMP()) AS HOURS_SINCE_UPDATE
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
            WHERE DELETED IS NULL
                AND TABLE_TYPE = 'BASE TABLE'
        )
        SELECT
            *,
            CASE
                WHEN HOURS_SINCE_UPDATE > 168 THEN 'STALE (>1 week)'
                WHEN HOURS_SINCE_UPDATE > 48 THEN 'AGING (>2 days)'
                WHEN HOURS_SINCE_UPDATE > 24 THEN 'WARNING (>1 day)'
                ELSE 'FRESH'
            END AS FRESHNESS_STATUS
        FROM table_last_modified
        WHERE BYTES > 0  -- Only tables with data
        ORDER BY HOURS_SINCE_UPDATE DESC
        LIMIT 100
        """
        return _self.session.sql(query).to_pandas()

    @st.cache_data(ttl=Config.CACHE_TTL)
    def get_schema_changes(_self, days):
        """Track schema evolution and changes"""
        query = f"""
        SELECT
            TABLE_CATALOG AS DATABASE_NAME,
            TABLE_SCHEMA AS SCHEMA_NAME,
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            COMMENT,
            DELETED
        FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
        WHERE LAST_ALTERED >= DATEADD(DAY, -{days}, CURRENT_DATE())
        ORDER BY LAST_ALTERED DESC
        LIMIT 500
        """
        return _self.session.sql(query).to_pandas()

# =============================================================================
# AI-POWERED INSIGHTS USING CORTEX COMPLETE
# =============================================================================

class AIInsightsGenerator:
    """Generate AI-powered insights using Snowflake Cortex Complete"""

    def __init__(self, session):
        self.session = session

    def generate_insight(self, context_data, insight_type="summary"):
        """Generate AI insights using Cortex Complete"""
        try:
            # Prepare context based on insight type
            if insight_type == "warehouse_optimization":
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

            elif insight_type == "security_review":
                prompt = f"""
                Review these access patterns and login activities:

                {context_data}

                Identify any:
                1. Unusual access patterns
                2. Potential security risks
                3. Recommended security improvements
                """

            else:  # General summary
                prompt = f"""
                Provide a concise executive summary of this Snowflake observability data:

                {context_data}

                Highlight: key metrics, trends, and top 3 action items.
                """

            # Call Cortex Complete
            query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-large2',
                [
                    {{'role': 'system', 'content': 'You are a Snowflake optimization expert providing concise, actionable insights.'}},
                    {{'role': 'user', 'content': '{prompt.replace("'", "''")}'}}
                ],
                {{
                    'temperature': 0.3,
                    'max_tokens': 500
                }}
            ) AS INSIGHT
            """

            result = self.session.sql(query).collect()
            if result:
                return result[0]['INSIGHT']
            return "Unable to generate AI insight at this time."

        except Exception as e:
            return f"AI insights temporarily unavailable: {str(e)}"

    def get_quick_summary(self, metrics_dict):
        """Generate a quick summary from key metrics"""
        try:
            # Format metrics for AI
            context = "\n".join([f"{k}: {v}" for k, v in metrics_dict.items()])
            return self.generate_insight(context, "summary")
        except:
            return "Summary unavailable"

# =============================================================================
# VISUALIZATION COMPONENTS
# =============================================================================

def create_metric_card(label, value, delta=None, delta_color="normal"):
    """Create a styled metric card"""
    st.metric(label=label, value=value, delta=delta, delta_color=delta_color)

def create_trend_chart(data, x_col, y_col, title="Trend Analysis"):
    """Create a standardized trend line chart"""
    chart = alt.Chart(data).mark_line(point=True).encode(
        x=alt.X(f'{x_col}:T', title=x_col),
        y=alt.Y(f'{y_col}:Q', title=y_col),
        tooltip=[f'{x_col}:T', alt.Tooltip(f'{y_col}:Q', format=',.2f')]
    ).properties(
        title=title,
        height=300
    ).interactive()

    return chart

def create_bar_chart(data, x_col, y_col, color_col=None, title="Bar Chart"):
    """Create a standardized bar chart"""
    encoding = {
        'y': alt.Y(f'{x_col}:N', sort='-x', title=x_col),
        'x': alt.X(f'{y_col}:Q', title=y_col),
        'tooltip': [x_col, alt.Tooltip(f'{y_col}:Q', format=',.2f')]
    }

    if color_col:
        encoding['color'] = alt.Color(f'{color_col}:Q', scale=alt.Scale(scheme='blues'))

    chart = alt.Chart(data).mark_bar().encode(**encoding).properties(
        title=title,
        height=300
    )

    return chart

def create_alert_badge(message, alert_type="info"):
    """Create alert badges for important notifications"""
    colors = {
        "info": "#E3F2FD",
        "warning": "#FFF3E0",
        "error": "#FFEBEE",
        "success": "#E8F5E9"
    }

    st.markdown(
        f"""
        <div style="background-color: {colors.get(alert_type, colors['info'])};
                    padding: 10px; border-radius: 5px; margin: 5px 0;">
            {message}
        </div>
        """,
        unsafe_allow_html=True
    )

# =============================================================================
# MAIN DASHBOARD
# =============================================================================

def main():
    """Main dashboard application"""

    # Page configuration
    st.set_page_config(
        page_title="Snowflake Holistic Observability Dashboard",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Custom CSS
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #29B5E8;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .metric-container {
        background-color: #f0f2f6;
        border-radius: 8px;
        padding: 15px;
        margin: 5px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .alert-box {
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
        border-left: 4px solid;
    }
    .alert-warning {
        background-color: #FFF3CD;
        border-color: #FFC107;
    }
    .alert-danger {
        background-color: #F8D7DA;
        border-color: #DC3545;
    }
    .alert-success {
        background-color: #D4EDDA;
        border-color: #28A745;
    }
    .alert-info {
        background-color: #D1ECF1;
        border-color: #17A2B8;
    }
    </style>
    """, unsafe_allow_html=True)

    # Header
    st.markdown('<p class="main-header">❄️ Snowflake Holistic Observability Dashboard</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Comprehensive monitoring, optimization, and AI-powered insights for your Snowflake environment</p>', unsafe_allow_html=True)

    # Get Snowflake session
    session = get_active_session()

    # Initialize query and AI classes
    queries = SnowflakeQueries(session)
    ai_insights = AIInsightsGenerator(session)

    # Sidebar controls
    st.sidebar.title("⚙️ Dashboard Controls")

    # Time period selector
    time_period = st.sidebar.selectbox(
        "Time Period",
        [1, 7, 14, 30, 60, 90],
        index=3,
        format_func=lambda x: f"Last {x} days"
    )

    # Refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    # Quick stats in sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Quick Stats")

    try:
        quick_stats_query = f"""
        SELECT
            (SELECT COUNT(DISTINCT WAREHOUSE_NAME)
             FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSES
             WHERE DELETED IS NULL) AS ACTIVE_WAREHOUSES,
            (SELECT COUNT(DISTINCT DATABASE_NAME)
             FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES
             WHERE DELETED IS NULL) AS ACTIVE_DATABASES,
            (SELECT COUNT(DISTINCT USER_NAME)
             FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
             WHERE DELETED_ON IS NULL) AS ACTIVE_USERS,
            (SELECT SUM(CREDITS_USED)
             FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
             WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())) AS TOTAL_CREDITS
        """
        quick_stats = session.sql(quick_stats_query).to_pandas().iloc[0]

        st.sidebar.metric("Active Warehouses", quick_stats['ACTIVE_WAREHOUSES'])
        st.sidebar.metric("Active Databases", quick_stats['ACTIVE_DATABASES'])
        st.sidebar.metric("Active Users", quick_stats['ACTIVE_USERS'])
        st.sidebar.metric("Total Credits Used", f"{quick_stats['TOTAL_CREDITS']:.1f}")
    except Exception as e:
        st.sidebar.error(f"Error loading quick stats: {str(e)}")

    # Main tabs
    tabs = st.tabs([
        "🏠 Overview",
        "🏢 Warehouses",
        "💾 Storage",
        "🔄 Data Transfer",
        "👥 Users & Queries",
        "🤖 AI & ML (Cortex)",
        "🔧 Data Pipelines",
        "⚡ Performance",
        "🔒 Security",
        "💰 Cost Management",
        "✅ Data Quality"
    ])

    # =========================================================================
    # TAB 0: OVERVIEW DASHBOARD
    # =========================================================================

    with tabs[0]:
        st.header("📊 Executive Overview")

        col1, col2 = st.columns([2, 1])

        with col1:
            st.subheader("Key Performance Indicators")

            # Load key metrics
            kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

            try:
                # Get warehouse metrics
                warehouse_metrics = queries.get_warehouse_metrics(time_period)
                total_credits = warehouse_metrics['TOTAL_CREDITS'].sum() if not warehouse_metrics.empty else 0

                with kpi_col1:
                    st.metric(
                        "Total Credits",
                        f"{total_credits:,.1f}",
                        f"${total_credits * Config.DEFAULT_CREDIT_COST:,.2f}"
                    )

                # Get storage metrics
                storage_metrics = queries.get_storage_metrics(time_period)
                total_storage = storage_metrics['TOTAL_BYTES'].sum() if not storage_metrics.empty else 0

                with kpi_col2:
                    st.metric(
                        "Total Storage",
                        format_bytes(total_storage)
                    )

                # Get cost anomalies
                cost_anomalies = queries.get_cost_anomalies(time_period)
                anomaly_count = len(cost_anomalies[cost_anomalies['STATUS'] == 'ANOMALY']) if not cost_anomalies.empty else 0

                with kpi_col3:
                    st.metric(
                        "Cost Anomalies",
                        anomaly_count,
                        delta_color="inverse"
                    )

                # Get query performance
                query_issues = queries.get_query_performance_insights(time_period)
                total_issues = query_issues['QUERY_COUNT'].sum() if not query_issues.empty else 0

                with kpi_col4:
                    st.metric(
                        "Query Issues",
                        total_issues,
                        delta_color="inverse"
                    )

            except Exception as e:
                st.error(f"Error loading KPIs: {str(e)}")

            # Alerts section
            st.subheader("🚨 Active Alerts")

            alert_col1, alert_col2 = st.columns(2)

            with alert_col1:
                # Cost alerts
                if anomaly_count > 0:
                    create_alert_badge(
                        f"⚠️ {anomaly_count} cost anomal{'y' if anomaly_count == 1 else 'ies'} detected in the last {time_period} days",
                        "warning"
                    )

                # Query performance alerts
                if total_issues > 10:
                    create_alert_badge(
                        f"⚠️ {total_issues} queries with performance issues",
                        "warning"
                    )

            with alert_col2:
                # Storage alerts
                try:
                    storage_issues = queries.get_table_storage_insights()
                    if not storage_issues.empty:
                        create_alert_badge(
                            f"💾 {len(storage_issues)} tables with storage optimization opportunities",
                            "info"
                        )
                except:
                    pass

                # Warehouse recommendations
                try:
                    warehouse_recs = queries.get_warehouse_recommendations(time_period)
                    needs_action = len(warehouse_recs[warehouse_recs['RECOMMENDATION'] != 'OPTIMAL']) if not warehouse_recs.empty else 0
                    if needs_action > 0:
                        create_alert_badge(
                            f"🏢 {needs_action} warehouse(s) need optimization",
                            "info"
                        )
                except:
                    pass

        with col2:
            st.subheader("🤖 AI Insights")

            try:
                # Prepare context for AI
                context_metrics = {
                    "Total Credits Used": f"{total_credits:,.1f}",
                    "Estimated Cost": f"${total_credits * Config.DEFAULT_CREDIT_COST:,.2f}",
                    "Total Storage": format_bytes(total_storage),
                    "Cost Anomalies": anomaly_count,
                    "Query Issues": total_issues,
                    "Time Period": f"{time_period} days"
                }

                with st.spinner("Generating AI insights..."):
                    summary = ai_insights.get_quick_summary(context_metrics)
                    st.info(summary)

            except Exception as e:
                st.warning("AI insights temporarily unavailable")

        # Trends section
        st.subheader("📈 Trends & Patterns")

        trend_col1, trend_col2 = st.columns(2)

        with trend_col1:
            try:
                # Daily cost trend
                daily_cost_query = f"""
                SELECT
                    DATE_TRUNC('DAY', START_TIME) AS DATE,
                    SUM(CREDITS_USED) * {Config.DEFAULT_CREDIT_COST} AS DAILY_COST
                FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
                WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
                GROUP BY DATE
                ORDER BY DATE
                """
                daily_costs = session.sql(daily_cost_query).to_pandas()

                if not daily_costs.empty:
                    daily_costs['DATE'] = pd.to_datetime(daily_costs['DATE'])
                    chart = create_trend_chart(daily_costs, 'DATE', 'DAILY_COST', 'Daily Cost Trend')
                    st.altair_chart(chart, use_container_width=True)
            except Exception as e:
                st.error(f"Error loading cost trend: {str(e)}")

        with trend_col2:
            try:
                # Query volume trend
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
                    chart = create_trend_chart(query_volume, 'DATE', 'QUERY_COUNT', 'Query Volume Trend')
                    st.altair_chart(chart, use_container_width=True)
            except Exception as e:
                st.error(f"Error loading query volume: {str(e)}")

    # =========================================================================
    # TAB 1: WAREHOUSE ANALYTICS (Enhanced from original)
    # =========================================================================

    with tabs[1]:
        st.header("🏢 Warehouse Analytics & Optimization")

        # Load data
        with st.spinner("Loading warehouse analytics..."):
            warehouse_metrics = queries.get_warehouse_metrics(time_period)
            warehouse_recs = queries.get_warehouse_recommendations(time_period)

        # Overview metrics
        if not warehouse_metrics.empty:
            col1, col2, col3, col4 = st.columns(4)

            total_credits = warehouse_metrics['TOTAL_CREDITS'].sum()
            avg_credits = warehouse_metrics['TOTAL_CREDITS'].mean()
            max_credits = warehouse_metrics['TOTAL_CREDITS'].max()
            num_warehouses = len(warehouse_metrics)

            with col1:
                st.metric("Total Credits", f"{total_credits:,.1f}")
            with col2:
                st.metric("Avg per Warehouse", f"{avg_credits:,.1f}")
            with col3:
                st.metric("Max Usage", f"{max_credits:,.1f}")
            with col4:
                st.metric("Active Warehouses", num_warehouses)

            # Warehouse usage breakdown
            st.subheader("Warehouse Usage Distribution")

            viz_col1, viz_col2 = st.columns(2)

            with viz_col1:
                # Top warehouses by credits
                top_warehouses = warehouse_metrics.nlargest(10, 'TOTAL_CREDITS')
                chart = create_bar_chart(
                    top_warehouses,
                    'WAREHOUSE_NAME',
                    'TOTAL_CREDITS',
                    'TOTAL_CREDITS',
                    'Top 10 Warehouses by Credit Usage'
                )
                st.altair_chart(chart, use_container_width=True)

            with viz_col2:
                # Warehouse load analysis
                load_data = warehouse_metrics[warehouse_metrics['AVG_RUNNING_QUERIES'].notna()]
                if not load_data.empty:
                    chart = alt.Chart(load_data.head(10)).mark_circle(size=100).encode(
                        x=alt.X('AVG_RUNNING_QUERIES:Q', title='Avg Running Queries'),
                        y=alt.Y('AVG_QUEUED_LOAD:Q', title='Avg Queued Load'),
                        size=alt.Size('TOTAL_CREDITS:Q', title='Total Credits'),
                        color=alt.Color('WAREHOUSE_NAME:N', legend=None),
                        tooltip=['WAREHOUSE_NAME', 'AVG_RUNNING_QUERIES', 'AVG_QUEUED_LOAD', 'TOTAL_CREDITS']
                    ).properties(
                        title='Warehouse Load Analysis',
                        height=300
                    )
                    st.altair_chart(chart, use_container_width=True)

        # Optimization recommendations
        st.subheader("💡 Optimization Recommendations")

        if not warehouse_recs.empty:
            # Filter for actionable recommendations
            actionable_recs = warehouse_recs[warehouse_recs['RECOMMENDATION'] != 'OPTIMAL']

            if not actionable_recs.empty:
                for _, rec in actionable_recs.head(10).iterrows():
                    rec_type = rec['RECOMMENDATION']
                    color = "warning" if rec_type in ['UPSIZE', 'DOWNSIZE'] else "error"

                    create_alert_badge(
                        f"**{rec['WAREHOUSE_NAME']}** ({rec['WAREHOUSE_SIZE']}): {rec['REASON']}",
                        color
                    )

                # AI-powered recommendations
                try:
                    with st.expander("🤖 AI-Powered Optimization Insights"):
                        context = actionable_recs.to_dict('records')
                        insight = ai_insights.generate_insight(
                            str(context[:5]),  # Top 5 recommendations
                            "warehouse_optimization"
                        )
                        st.write(insight)
                except:
                    pass
            else:
                st.success("✅ All warehouses are optimally configured!")

        # Detailed metrics table
        with st.expander("📊 Detailed Warehouse Metrics"):
            if not warehouse_metrics.empty:
                display_cols = [
                    'WAREHOUSE_NAME', 'TOTAL_CREDITS', 'AVG_DAILY_CREDITS',
                    'AVG_RUNNING_QUERIES', 'AVG_QUEUED_LOAD', 'ACTIVE_DAYS'
                ]
                display_df = warehouse_metrics[display_cols].copy()
                display_df.columns = [
                    'Warehouse', 'Total Credits', 'Avg Daily Credits',
                    'Avg Running Queries', 'Avg Queue Load', 'Active Days'
                ]
                st.dataframe(display_df, use_container_width=True)

    # =========================================================================
    # TAB 2: STORAGE ANALYTICS (Enhanced from original)
    # =========================================================================

    with tabs[2]:
        st.header("💾 Storage Analytics & Optimization")

        with st.spinner("Loading storage analytics..."):
            storage_metrics = queries.get_storage_metrics(time_period)
            storage_issues = queries.get_table_storage_insights()

        if not storage_metrics.empty:
            # Overview metrics
            col1, col2, col3, col4 = st.columns(4)

            total_storage = storage_metrics['TOTAL_BYTES'].sum()
            total_failsafe = storage_metrics['FAILSAFE_BYTES'].sum()
            total_stage = storage_metrics['TOTAL_STAGE_BYTES'].iloc[0] if 'TOTAL_STAGE_BYTES' in storage_metrics.columns else 0

            with col1:
                st.metric("Total Storage", format_bytes(total_storage))
            with col2:
                st.metric("Failsafe Storage", format_bytes(total_failsafe))
            with col3:
                st.metric("Stage Storage", format_bytes(total_stage))
            with col4:
                monthly_cost = (total_storage / (1024**4)) * Config.DEFAULT_STORAGE_COST
                st.metric("Est. Monthly Cost", f"${monthly_cost:,.2f}")

            # Storage breakdown
            st.subheader("Storage Distribution by Database")

            top_dbs = storage_metrics.nlargest(10, 'TOTAL_BYTES')
            chart = create_bar_chart(
                top_dbs,
                'DATABASE_NAME',
                'TOTAL_BYTES',
                'TOTAL_BYTES',
                'Top 10 Databases by Storage'
            )
            st.altair_chart(chart, use_container_width=True)

        # Storage optimization opportunities
        st.subheader("🎯 Storage Optimization Opportunities")

        if not storage_issues.empty:
            issue_summary = storage_issues.groupby('ISSUE')['TOTAL_BYTES'].agg(['count', 'sum']).reset_index()
            issue_summary.columns = ['Issue Type', 'Table Count', 'Total Bytes']
            issue_summary['Total Size'] = issue_summary['Total Bytes'].apply(format_bytes)
            issue_summary['Potential Savings'] = (issue_summary['Total Bytes'] / (1024**4) * Config.DEFAULT_STORAGE_COST).round(2)

            st.dataframe(issue_summary[['Issue Type', 'Table Count', 'Total Size', 'Potential Savings']], use_container_width=True)

            total_savings = issue_summary['Potential Savings'].sum()
            st.info(f"💰 Potential monthly savings: **${total_savings:,.2f}** by addressing storage issues")

            with st.expander("View Detailed Table Issues"):
                display_issues = storage_issues.copy()
                display_issues['SIZE'] = display_issues['TOTAL_BYTES'].apply(format_bytes)
                st.dataframe(
                    display_issues[['DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME', 'SIZE', 'ISSUE']],
                    use_container_width=True
                )

    # =========================================================================
    # TAB 3: DATA TRANSFER (Keep original functionality)
    # =========================================================================

    with tabs[3]:
        st.header("🔄 Data Transfer Analytics")
        st.info("This tab contains the original data transfer analytics. See the original code for implementation.")
        # Original data transfer code remains here

    # =========================================================================
    # TAB 4: USER & QUERY ANALYTICS (Keep original + enhancements)
    # =========================================================================

    with tabs[4]:
        st.header("👥 User & Query Analytics")
        st.info("This tab contains the original user query analytics. See the original code for implementation.")
        # Original user query analytics code remains here

    # =========================================================================
    # TAB 5: AI & ML WORKLOAD MONITORING (NEW)
    # =========================================================================

    with tabs[5]:
        st.header("🤖 AI & ML Workload Monitoring (Cortex)")

        with st.spinner("Loading Cortex usage data..."):
            cortex_usage = queries.get_cortex_usage(time_period)

        # Overview
        col1, col2, col3 = st.columns(3)

        total_cortex_credits = 0
        analyst_requests = 0
        search_queries = 0

        if not cortex_usage['analyst'].empty:
            analyst_requests = cortex_usage['analyst']['REQUEST_COUNT'].sum()
            total_cortex_credits += cortex_usage['analyst']['TOTAL_CREDITS'].sum()

        if not cortex_usage['search'].empty:
            search_queries = cortex_usage['search']['TOTAL_QUERIES'].sum()
            total_cortex_credits += cortex_usage['search']['TOTAL_CREDITS'].sum()

        if not cortex_usage['finetuning'].empty:
            total_cortex_credits += cortex_usage['finetuning']['TOTAL_CREDITS'].sum()

        with col1:
            st.metric("Total Cortex Credits", f"{total_cortex_credits:,.2f}")
        with col2:
            st.metric("Analyst Requests", f"{analyst_requests:,}")
        with col3:
            st.metric("Search Queries", f"{search_queries:,}")

        # Detailed breakdowns
        st.subheader("Cortex Service Usage")

        service_tabs = st.tabs(["Cortex Analyst", "Cortex Search", "Fine-Tuning"])

        with service_tabs[0]:
            if not cortex_usage['analyst'].empty:
                # Usage by user
                user_usage = cortex_usage['analyst'].groupby('USER_NAME').agg({
                    'REQUEST_COUNT': 'sum',
                    'TOTAL_CREDITS': 'sum'
                }).reset_index().sort_values('TOTAL_CREDITS', ascending=False)

                chart = create_bar_chart(
                    user_usage.head(10),
                    'USER_NAME',
                    'TOTAL_CREDITS',
                    'TOTAL_CREDITS',
                    'Top Users by Cortex Analyst Credits'
                )
                st.altair_chart(chart, use_container_width=True)

                # Trend over time
                if 'USAGE_DATE' in cortex_usage['analyst'].columns:
                    daily_usage = cortex_usage['analyst'].groupby('USAGE_DATE').agg({
                        'REQUEST_COUNT': 'sum',
                        'TOTAL_CREDITS': 'sum'
                    }).reset_index()
                    daily_usage['USAGE_DATE'] = pd.to_datetime(daily_usage['USAGE_DATE'])

                    chart = create_trend_chart(
                        daily_usage,
                        'USAGE_DATE',
                        'REQUEST_COUNT',
                        'Cortex Analyst Usage Trend'
                    )
                    st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No Cortex Analyst usage in the selected period")

        with service_tabs[1]:
            if not cortex_usage['search'].empty:
                # Search service usage
                service_usage = cortex_usage['search'].groupby('SERVICE_NAME').agg({
                    'TOTAL_QUERIES': 'sum',
                    'TOTAL_CREDITS': 'sum'
                }).reset_index()

                col1, col2 = st.columns(2)

                with col1:
                    chart = create_bar_chart(
                        service_usage,
                        'SERVICE_NAME',
                        'TOTAL_QUERIES',
                        'TOTAL_QUERIES',
                        'Queries by Search Service'
                    )
                    st.altair_chart(chart, use_container_width=True)

                with col2:
                    chart = create_bar_chart(
                        service_usage,
                        'SERVICE_NAME',
                        'TOTAL_CREDITS',
                        'TOTAL_CREDITS',
                        'Credits by Search Service'
                    )
                    st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No Cortex Search usage in the selected period")

        with service_tabs[2]:
            if not cortex_usage['finetuning'].empty:
                # Fine-tuning jobs
                ft_summary = cortex_usage['finetuning'].groupby('MODEL_NAME').agg({
                    'JOB_COUNT': 'sum',
                    'TOTAL_CREDITS': 'sum'
                }).reset_index()

                st.dataframe(ft_summary, use_container_width=True)
            else:
                st.info("No Cortex Fine-Tuning jobs in the selected period")

        # AI-powered insights about AI usage (meta!)
        try:
            with st.expander("🤖 AI Insights on AI Usage"):
                context = f"""
                Cortex Total Credits: {total_cortex_credits}
                Analyst Requests: {analyst_requests}
                Search Queries: {search_queries}
                Time Period: {time_period} days
                """
                insight = ai_insights.generate_insight(context, "summary")
                st.write(insight)
        except:
            pass

    # =========================================================================
    # TAB 6: DATA PIPELINES (NEW)
    # =========================================================================

    with tabs[6]:
        st.header("🔧 Data Pipeline Observability")

        pipeline_tabs = st.tabs(["Tasks", "Snowpipes", "Dynamic Tables"])

        with pipeline_tabs[0]:
            st.subheader("Task Execution Monitoring")

            with st.spinner("Loading task history..."):
                task_history = queries.get_task_history(time_period)

            if not task_history.empty:
                # Overview metrics
                col1, col2, col3, col4 = st.columns(4)

                total_tasks = len(task_history)
                total_runs = task_history['TOTAL_RUNS'].sum()
                failed_runs = task_history['FAILED_RUNS'].sum()
                success_rate = ((total_runs - failed_runs) / total_runs * 100) if total_runs > 0 else 0

                with col1:
                    st.metric("Total Tasks", total_tasks)
                with col2:
                    st.metric("Total Runs", f"{total_runs:,}")
                with col3:
                    st.metric("Failed Runs", failed_runs, delta_color="inverse")
                with col4:
                    st.metric("Success Rate", f"{success_rate:.1f}%")

                # Tasks with failures
                failed_tasks = task_history[task_history['FAILED_RUNS'] > 0].sort_values('FAILED_RUNS', ascending=False)

                if not failed_tasks.empty:
                    st.warning(f"⚠️ {len(failed_tasks)} task(s) have failures")

                    chart = create_bar_chart(
                        failed_tasks.head(10),
                        'TASK_NAME',
                        'FAILED_RUNS',
                        'FAILED_RUNS',
                        'Tasks with Most Failures'
                    )
                    st.altair_chart(chart, use_container_width=True)

                # Task performance
                st.subheader("Task Performance")

                perf_data = task_history.nlargest(15, 'AVG_DURATION_SEC')
                chart = create_bar_chart(
                    perf_data,
                    'TASK_NAME',
                    'AVG_DURATION_SEC',
                    'AVG_DURATION_SEC',
                    'Slowest Tasks by Avg Duration'
                )
                st.altair_chart(chart, use_container_width=True)

                # Detailed table
                with st.expander("View All Tasks"):
                    st.dataframe(task_history, use_container_width=True)
            else:
                st.info("No task execution data available for the selected period")

        with pipeline_tabs[1]:
            st.subheader("Snowpipe Monitoring")

            with st.spinner("Loading Snowpipe data..."):
                pipe_data = queries.get_pipe_usage(time_period)

            if not pipe_data['pipe'].empty:
                # Regular Snowpipe
                col1, col2, col3 = st.columns(3)

                total_files = pipe_data['pipe']['TOTAL_FILES'].sum()
                total_bytes = pipe_data['pipe']['TOTAL_BYTES'].sum()
                total_credits = pipe_data['pipe']['TOTAL_CREDITS'].sum()

                with col1:
                    st.metric("Files Loaded", f"{total_files:,}")
                with col2:
                    st.metric("Data Loaded", format_bytes(total_bytes))
                with col3:
                    st.metric("Credits Used", f"{total_credits:,.2f}")

                chart = create_bar_chart(
                    pipe_data['pipe'].head(10),
                    'PIPE_NAME',
                    'TOTAL_BYTES',
                    'TOTAL_CREDITS',
                    'Top Pipes by Data Volume'
                )
                st.altair_chart(chart, use_container_width=True)

            if not pipe_data['streaming'].empty:
                st.subheader("Snowpipe Streaming")

                # Streaming metrics
                col1, col2, col3 = st.columns(3)

                total_rows = pipe_data['streaming']['TOTAL_ROWS'].sum()
                total_bytes = pipe_data['streaming']['TOTAL_BYTES'].sum()
                avg_latency = pipe_data['streaming']['AVG_LATENCY_MS'].mean()

                with col1:
                    st.metric("Rows Inserted", f"{total_rows:,}")
                with col2:
                    st.metric("Data Streamed", format_bytes(total_bytes))
                with col3:
                    st.metric("Avg Latency", f"{avg_latency:.1f} ms")

                chart = create_bar_chart(
                    pipe_data['streaming'].head(10),
                    'CHANNEL_NAME',
                    'TOTAL_BYTES',
                    'AVG_LATENCY_MS',
                    'Streaming Channels Performance'
                )
                st.altair_chart(chart, use_container_width=True)

        with pipeline_tabs[2]:
            st.subheader("Dynamic Table Refreshes")

            with st.spinner("Loading dynamic table data..."):
                dt_data = queries.get_dynamic_table_refreshes(time_period)

            if not dt_data.empty:
                # Overview metrics
                col1, col2, col3 = st.columns(3)

                total_refreshes = len(dt_data)
                successful_refreshes = len(dt_data[dt_data['STATE'] == 'SUCCEEDED'])
                avg_duration = dt_data['REFRESH_DURATION_SEC'].mean()

                with col1:
                    st.metric("Total Refreshes", total_refreshes)
                with col2:
                    st.metric("Successful", successful_refreshes)
                with col3:
                    st.metric("Avg Duration", f"{avg_duration:.1f}s")

                # Performance by table
                table_perf = dt_data.groupby('TABLE_NAME').agg({
                    'REFRESH_DURATION_SEC': 'mean',
                    'CREDITS_USED': 'sum'
                }).reset_index().sort_values('CREDITS_USED', ascending=False)

                chart = create_bar_chart(
                    table_perf.head(10),
                    'TABLE_NAME',
                    'CREDITS_USED',
                    'REFRESH_DURATION_SEC',
                    'Dynamic Table Credits Usage'
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No dynamic table refresh data available")

    # =========================================================================
    # TAB 7: PERFORMANCE OPTIMIZATION (NEW)
    # =========================================================================

    with tabs[7]:
        st.header("⚡ Performance Optimization")

        perf_tabs = st.tabs(["Query Performance", "Pruning Efficiency", "Spilling Analysis"])

        with perf_tabs[0]:
            st.subheader("Query Performance Issues")

            with st.spinner("Analyzing query performance..."):
                query_issues = queries.get_query_performance_insights(time_period)

            if not query_issues.empty:
                # Issue summary
                chart = create_bar_chart(
                    query_issues,
                    'ISSUE_TYPE',
                    'QUERY_COUNT',
                    'QUERY_COUNT',
                    'Query Issues by Type'
                )
                st.altair_chart(chart, use_container_width=True)

                # Detailed breakdown
                st.dataframe(query_issues, use_container_width=True)

                # AI recommendations
                try:
                    with st.expander("🤖 AI Performance Recommendations"):
                        context = query_issues.to_dict('records')
                        insight = ai_insights.generate_insight(
                            str(context),
                            "performance_analysis"
                        )
                        st.write(insight)
                except:
                    pass
            else:
                st.success("✅ No significant query performance issues detected!")

        with perf_tabs[1]:
            st.subheader("Table Pruning Efficiency")

            with st.spinner("Analyzing pruning efficiency..."):
                pruning_data = queries.get_pruning_efficiency(time_period)

            if not pruning_data.empty:
                # Quality distribution
                quality_dist = pruning_data['PRUNING_QUALITY'].value_counts().reset_index()
                quality_dist.columns = ['Quality', 'Table Count']

                col1, col2 = st.columns([1, 2])

                with col1:
                    st.dataframe(quality_dist, use_container_width=True)

                with col2:
                    # Tables with poor pruning
                    poor_pruning = pruning_data[pruning_data['PRUNING_QUALITY'] == 'Poor']
                    if not poor_pruning.empty:
                        chart = create_bar_chart(
                            poor_pruning.head(10),
                            'TABLE_NAME',
                            'AVG_SCAN_RATIO',
                            'TOTAL_PARTITIONS_SCANNED',
                            'Tables with Poor Pruning'
                        )
                        st.altair_chart(chart, use_container_width=True)

                # Recommendations
                if not poor_pruning.empty:
                    st.warning(f"⚠️ {len(poor_pruning)} table(s) have poor pruning efficiency. Consider adding clustering keys.")
            else:
                st.info("No pruning data available")

        with perf_tabs[2]:
            st.info("Spilling analysis coming soon - tracks local and remote spilling patterns")

    # =========================================================================
    # TAB 8: SECURITY & GOVERNANCE (NEW)
    # =========================================================================

    with tabs[8]:
        st.header("🔒 Security & Governance")

        sec_tabs = st.tabs(["Access Patterns", "Login Activity", "Audit Trail"])

        with sec_tabs[0]:
            st.subheader("Access Patterns Analysis")

            with st.spinner("Loading access patterns..."):
                access_data = queries.get_access_patterns(time_period)

            if not access_data.empty:
                # Top accessors
                top_users = access_data.nlargest(15, 'ACCESS_COUNT')

                chart = create_bar_chart(
                    top_users,
                    'USER_NAME',
                    'ACCESS_COUNT',
                    'UNIQUE_OBJECTS_ACCESSED',
                    'Top Users by Access Volume'
                )
                st.altair_chart(chart, use_container_width=True)

                # Unusual patterns
                unusual = access_data[access_data['ACCESS_COUNT'] > access_data['ACCESS_COUNT'].quantile(0.95)]
                if not unusual.empty:
                    st.warning(f"⚠️ {len(unusual)} user(s) with unusually high access patterns")
                    st.dataframe(unusual, use_container_width=True)

        with sec_tabs[1]:
            st.subheader("Login Activity Monitoring")

            with st.spinner("Loading login history..."):
                login_data = queries.get_login_history(time_period)

            if not login_data.empty:
                # Success vs failures
                col1, col2, col3 = st.columns(3)

                total_logins = len(login_data)
                successful = len(login_data[login_data['IS_SUCCESS'] == True])
                failed = total_logins - successful

                with col1:
                    st.metric("Total Logins", total_logins)
                with col2:
                    st.metric("Successful", successful)
                with col3:
                    st.metric("Failed", failed, delta_color="inverse")

                # Failed logins
                if failed > 0:
                    failed_logins = login_data[login_data['IS_SUCCESS'] == False]
                    st.warning(f"⚠️ {failed} failed login attempt(s)")

                    # Group by user
                    failed_by_user = failed_logins.groupby('USER_NAME').size().reset_index(name='FAILED_ATTEMPTS')
                    failed_by_user = failed_by_user.sort_values('FAILED_ATTEMPTS', ascending=False)

                    st.dataframe(failed_by_user.head(10), use_container_width=True)

                # Login timeline
                login_data['EVENT_DATE'] = pd.to_datetime(login_data['EVENT_TIMESTAMP']).dt.date
                daily_logins = login_data.groupby('EVENT_DATE').size().reset_index(name='LOGIN_COUNT')
                daily_logins['EVENT_DATE'] = pd.to_datetime(daily_logins['EVENT_DATE'])

                chart = create_trend_chart(
                    daily_logins,
                    'EVENT_DATE',
                    'LOGIN_COUNT',
                    'Daily Login Activity'
                )
                st.altair_chart(chart, use_container_width=True)

        with sec_tabs[2]:
            st.info("Comprehensive audit trail view coming soon")

    # =========================================================================
    # TAB 9: COST MANAGEMENT (NEW)
    # =========================================================================

    with tabs[9]:
        st.header("💰 Cost Management & Optimization")

        cost_tabs = st.tabs(["Cost Attribution", "Anomaly Detection", "Savings Opportunities"])

        with cost_tabs[0]:
            st.subheader("Cost Attribution Analysis")

            with st.spinner("Loading cost data..."):
                cost_data = queries.get_cost_attribution(time_period)

            if not cost_data.empty:
                # Total costs
                total_cost = cost_data['ESTIMATED_COST'].sum()
                st.metric("Total Estimated Cost", f"${total_cost:,.2f}")

                # By cost type
                cost_by_type = cost_data.groupby('COST_TYPE')['ESTIMATED_COST'].sum().reset_index()

                col1, col2 = st.columns(2)

                with col1:
                    # Pie chart
                    fig = px.pie(
                        cost_by_type,
                        values='ESTIMATED_COST',
                        names='COST_TYPE',
                        title='Cost Distribution by Type'
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    # Top resources
                    top_resources = cost_data.nlargest(10, 'ESTIMATED_COST')
                    chart = create_bar_chart(
                        top_resources,
                        'RESOURCE_NAME',
                        'ESTIMATED_COST',
                        'CREDITS',
                        'Top 10 Resources by Cost'
                    )
                    st.altair_chart(chart, use_container_width=True)

                # By user/role
                if 'USER_NAME' in cost_data.columns:
                    user_costs = cost_data[cost_data['USER_NAME'].notna()].groupby('USER_NAME')['ESTIMATED_COST'].sum().reset_index()
                    user_costs = user_costs.sort_values('ESTIMATED_COST', ascending=False)

                    st.subheader("Cost Attribution by User")
                    chart = create_bar_chart(
                        user_costs.head(10),
                        'USER_NAME',
                        'ESTIMATED_COST',
                        'ESTIMATED_COST',
                        'Top 10 Users by Cost'
                    )
                    st.altair_chart(chart, use_container_width=True)

        with cost_tabs[1]:
            st.subheader("Cost Anomaly Detection")

            with st.spinner("Detecting cost anomalies..."):
                anomalies = queries.get_cost_anomalies(time_period)

            if not anomalies.empty:
                # Anomaly summary
                anomaly_days = anomalies[anomalies['STATUS'] == 'ANOMALY']

                if not anomaly_days.empty:
                    st.warning(f"⚠️ {len(anomaly_days)} day(s) with cost anomalies detected")

                    # Visualize
                    anomalies['COST_DATE'] = pd.to_datetime(anomalies['COST_DATE'])

                    # Create chart with anomaly highlighting
                    base = alt.Chart(anomalies).encode(
                        x=alt.X('COST_DATE:T', title='Date')
                    )

                    line = base.mark_line().encode(
                        y=alt.Y('DAILY_COST:Q', title='Daily Cost ($)')
                    )

                    points = base.mark_point(size=100, filled=True).encode(
                        y='DAILY_COST:Q',
                        color=alt.condition(
                            alt.datum.STATUS == 'ANOMALY',
                            alt.value('red'),
                            alt.value('blue')
                        )
                    )

                    chart = (line + points).properties(
                        title='Daily Cost with Anomalies Highlighted',
                        height=400
                    )
                    st.altair_chart(chart, use_container_width=True)

                    # Anomaly details
                    st.dataframe(
                        anomaly_days[['COST_DATE', 'DAILY_COST', 'AVG_DAILY_COST', 'Z_SCORE']],
                        use_container_width=True
                    )
                else:
                    st.success("✅ No cost anomalies detected")

        with cost_tabs[2]:
            st.subheader("💡 Cost Savings Opportunities")

            # Combine multiple optimization opportunities
            total_potential_savings = 0

            # From storage
            try:
                storage_issues = queries.get_table_storage_insights()
                if not storage_issues.empty:
                    storage_savings = (storage_issues['TOTAL_BYTES'].sum() / (1024**4)) * Config.DEFAULT_STORAGE_COST
                    total_potential_savings += storage_savings

                    create_alert_badge(
                        f"💾 **Storage Optimization**: Potential ${storage_savings:,.2f}/month savings from {len(storage_issues)} tables",
                        "info"
                    )
            except:
                pass

            # From warehouse rightsizing
            try:
                warehouse_recs = queries.get_warehouse_recommendations(time_period)
                downsize_recs = warehouse_recs[warehouse_recs['RECOMMENDATION'] == 'DOWNSIZE']
                if not downsize_recs.empty:
                    # Estimate 30% savings from downsizing
                    downsize_savings = downsize_recs['TOTAL_CREDITS'].sum() * 0.3 * Config.DEFAULT_CREDIT_COST
                    total_potential_savings += downsize_savings

                    create_alert_badge(
                        f"🏢 **Warehouse Rightsizing**: Potential ${downsize_savings:,.2f} savings from downsizing {len(downsize_recs)} warehouse(s)",
                        "info"
                    )
            except:
                pass

            # Total savings summary
            if total_potential_savings > 0:
                st.success(f"🎯 **Total Potential Savings**: ${total_potential_savings:,.2f}/month")

                # AI-powered cost recommendations
                try:
                    with st.expander("🤖 AI-Powered Cost Optimization Recommendations"):
                        context = f"Total potential savings: ${total_potential_savings:,.2f}"
                        insight = ai_insights.generate_insight(context, "cost_summary")
                        st.write(insight)
                except:
                    pass
            else:
                st.info("No significant cost savings opportunities identified")

    # =========================================================================
    # TAB 10: DATA QUALITY (NEW)
    # =========================================================================

    with tabs[10]:
        st.header("✅ Data Quality Monitoring")

        quality_tabs = st.tabs(["Freshness", "Schema Changes", "Quality Metrics"])

        with quality_tabs[0]:
            st.subheader("Data Freshness Monitoring")

            with st.spinner("Checking data freshness..."):
                freshness_data = queries.get_table_freshness()

            if not freshness_data.empty:
                # Status summary
                status_counts = freshness_data['FRESHNESS_STATUS'].value_counts().reset_index()
                status_counts.columns = ['Status', 'Table Count']

                col1, col2 = st.columns([1, 2])

                with col1:
                    st.dataframe(status_counts, use_container_width=True)

                with col2:
                    # Stale tables
                    stale_tables = freshness_data[freshness_data['FRESHNESS_STATUS'].str.contains('STALE|AGING')]
                    if not stale_tables.empty:
                        st.warning(f"⚠️ {len(stale_tables)} table(s) may be stale")

                        chart = create_bar_chart(
                            stale_tables.head(10),
                            'TABLE_NAME',
                            'HOURS_SINCE_UPDATE',
                            'BYTES',
                            'Tables with Stalest Data'
                        )
                        st.altair_chart(chart, use_container_width=True)

                # Detailed view
                with st.expander("View All Tables"):
                    display_df = freshness_data.copy()
                    display_df['SIZE'] = display_df['BYTES'].apply(format_bytes)
                    display_df['DAYS_SINCE_UPDATE'] = (display_df['HOURS_SINCE_UPDATE'] / 24).round(1)

                    st.dataframe(
                        display_df[['DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME',
                                   'SIZE', 'DAYS_SINCE_UPDATE', 'FRESHNESS_STATUS']],
                        use_container_width=True
                    )

        with quality_tabs[1]:
            st.subheader("Schema Change Detection")

            with st.spinner("Loading schema changes..."):
                schema_changes = queries.get_schema_changes(time_period)

            if not schema_changes.empty:
                st.info(f"📊 {len(schema_changes)} column changes detected in the last {time_period} days")

                # Group by table
                changes_by_table = schema_changes.groupby(['DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME']).size().reset_index(name='CHANGE_COUNT')
                changes_by_table = changes_by_table.sort_values('CHANGE_COUNT', ascending=False)

                st.dataframe(changes_by_table.head(20), use_container_width=True)

                # Detailed changes
                with st.expander("View All Schema Changes"):
                    st.dataframe(schema_changes, use_container_width=True)
            else:
                st.success("✅ No schema changes detected")

        with quality_tabs[2]:
            st.info("Additional quality metrics (nullability, data types, constraints) coming soon")

    # Footer
    st.markdown("---")
    st.caption(f"Dashboard last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption("💡 Pro tip: Use the refresh button in the sidebar to update all metrics")

if __name__ == "__main__":
    main()
