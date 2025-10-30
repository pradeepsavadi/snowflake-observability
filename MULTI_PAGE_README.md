

# Snowflake Observability Dashboard - Multi-Page Structure Guide

## 📁 Project Structure

```
snowflake-observability/
├── main.py                          # Home/Overview page (entry point)
├── utils.py                         # Shared utilities, classes, and functions
├── pages/                           # Multi-page directory
│   ├── 1_🏢_Warehouses.py          # Warehouse analytics
│   ├── 2_💾_Storage.py             # Storage management
│   ├── 3_🔄_Data_Transfer.py       # Data transfer monitoring
│   ├── 4_👥_Users_and_Queries.py   # User query analytics
│   ├── 5_🤖_AI_and_ML.py           # AI/ML workload monitoring (✅ CREATED)
│   ├── 6_🔧_Data_Pipelines.py      # Tasks, Snowpipes, Dynamic Tables
│   ├── 7_⚡_Performance.py          # Performance optimization
│   ├── 8_🔒_Security.py             # Security & governance
│   ├── 9_💰_Cost_Management.py     # Cost management
│   ├── 10_✅_Data_Quality.py       # Data quality monitoring
│   └── 11_🧠_AI_Insights.py        # Interactive AI insights (✅ CREATED)
├── CHANGELOG.md
├── README.md
└── MULTI_PAGE_README.md            # This file
```

## 🚀 Getting Started

### Deployment to Snowflake

1. **Upload Files**:
   - Upload `main.py` and `utils.py` to your Streamlit app directory
   - Upload all files in `pages/` to a `pages/` subdirectory

2. **Set Main File**:
   - In Snowsight, set `main.py` as the entry point

3. **Grant Permissions**:
   ```sql
   GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;
   GRANT USAGE ON DATABASE SNOWFLAKE TO ROLE <your_role>;
   ```

4. **Run the App**:
   - Streamlit will automatically detect the `pages/` folder
   - Navigation will appear in the sidebar

## 🎨 Page Naming Convention

Streamlit multi-page apps use filename prefixes for ordering:

- Format: `{order}_{icon}_{name}.py`
- Example: `1_🏢_Warehouses.py`

The sidebar will show: **🏢 Warehouses**

## 📝 Page Template

Use this template for creating new pages:

```python
"""
[Page Name] - [Description]
===============================================
[Detailed description of what this page does]
"""

import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
import sys
sys.path.append('..')  # Important for importing utils

from utils import (
    initialize_session_state,
    render_settings_sidebar,
    get_snowflake_session,
    format_bytes,
    format_number,
    apply_custom_css,
    render_page_header,
    create_metric_card,
    create_trend_chart,
    create_bar_chart,
    create_alert_badge,
    SnowflakeQueries,
    AIInsightsGenerator
)

# Page configuration
st.set_page_config(
    page_title="[Page Title] - Snowflake Observability",
    page_icon="[Icon]",
    layout="wide"
)

# Apply custom styling
apply_custom_css()

# Initialize session state and render settings
initialize_session_state()
render_settings_sidebar()

# Get Snowflake session
session = get_snowflake_session()
if not session:
    st.error("Failed to connect to Snowflake. Please check your connection.")
    st.stop()

# Initialize classes
queries = SnowflakeQueries(session)
ai_insights = AIInsightsGenerator(session)

# Page header
render_page_header(
    "[Page Title]",
    "[Subtitle/description]",
    "[Icon]"
)

# Get configuration from session state
time_period = st.session_state.time_period
credit_cost = st.session_state.credit_cost
storage_cost = st.session_state.storage_cost_per_tb

# ============================================================================
# YOUR PAGE CONTENT HERE
# ============================================================================

st.subheader("Section 1")

# Load data
with st.spinner("Loading data..."):
    try:
        data = queries.get_your_data(time_period)

        # Display metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Metric 1", "Value")

        with col2:
            st.metric("Metric 2", "Value")

        with col3:
            st.metric("Metric 3", "Value")

        # Visualizations
        if not data.empty:
            chart = create_bar_chart(data, 'x_col', 'y_col', title='Chart Title')
            if chart:
                st.altair_chart(chart, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading data: {str(e)}")

# Footer
st.markdown("---")
st.caption(f"📅 Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
st.caption(f"⏱️ Analysis period: {time_period} days")
```

## 🔧 Adding Query Methods to utils.py

When you need new queries, add them to the `SnowflakeQueries` class in `utils.py`:

```python
@st.cache_data(ttl=3600)
def get_your_new_query(_self, days):
    """Description of what this query does"""
    query = f"""
    SELECT
        column1,
        column2,
        COUNT(*) AS metric
    FROM SNOWFLAKE.ACCOUNT_USAGE.YOUR_VIEW
    WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
    GROUP BY column1, column2
    ORDER BY metric DESC
    """
    return _self.session.sql(query).to_pandas()
```

## 📊 Available Helper Functions

### Visualization Functions

1. **create_metric_card(label, value, delta=None, delta_color="normal")**
   - Creates a metric card with optional delta

2. **create_trend_chart(data, x_col, y_col, title, height=300)**
   - Creates a line chart for trends over time

3. **create_bar_chart(data, x_col, y_col, color_col=None, title, height=300)**
   - Creates a horizontal bar chart

4. **create_alert_badge(message, alert_type="info")**
   - Creates colored alert boxes
   - Types: "info", "warning", "error", "success"

### Formatting Functions

1. **format_bytes(bytes_val)**
   - Converts bytes to human-readable format (KB, MB, GB, TB)

2. **format_number(num)**
   - Formats large numbers with K, M, B suffixes

3. **safe_divide(numerator, denominator, default=0)**
   - Safe division with fallback

## 🎯 Page-Specific Implementation Guides

### 1. Warehouses Page (`1_🏢_Warehouses.py`)

**Key Sections:**
- Warehouse usage metrics (credits, queue times, active days)
- Warehouse load analysis (running queries, queued load)
- Optimization recommendations (upsize/downsize/suspend)
- Cost attribution by warehouse
- Performance metrics by warehouse

**Queries Needed:**
- `get_warehouse_metrics(days)` - ✅ Already in utils
- `get_warehouse_recommendations(days)` - ✅ Already in utils
- Daily warehouse trends
- Warehouse-specific query performance

**Layout Suggestion:**
```python
# KPIs
col1, col2, col3, col4 = st.columns(4)
# Overview metrics

# Warehouse usage breakdown
tab1, tab2, tab3 = st.tabs(["Usage", "Performance", "Recommendations"])

with tab1:
    # Usage charts

with tab2:
    # Performance analysis

with tab3:
    # Automated recommendations with AI insights
```

### 2. Storage Page (`2_💾_Storage.py`)

**Key Sections:**
- Storage overview (database, failsafe, stage, hybrid tables)
- Storage growth trends
- Table-level analysis
- Optimization opportunities (unused tables, high overhead)
- Cost projections

**Queries Needed:**
- `get_storage_metrics(days)` - ✅ Already in utils
- `get_table_storage_insights()` - ✅ Already in utils
- Daily storage trends
- Table growth analysis

### 3. Data Transfer Page (`3_🔄_Data_Transfer.py`)

**Key Sections:**
- Transfer volume metrics
- Cross-cloud and cross-region analysis
- User/warehouse attribution
- Transfer type breakdown
- Recent activity timeline

**Queries Needed:**
```python
@st.cache_data(ttl=3600)
def get_data_transfer_metrics(_self, days):
    query = f"""
    SELECT
        DATE_TRUNC('DAY', START_TIME) AS TRANSFER_DATE,
        SOURCE_CLOUD,
        TARGET_CLOUD,
        SOURCE_REGION,
        TARGET_REGION,
        SUM(BYTES_TRANSFERRED) AS DAILY_BYTES
    FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY
    WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
    GROUP BY TRANSFER_DATE, SOURCE_CLOUD, TARGET_CLOUD, SOURCE_REGION, TARGET_REGION
    ORDER BY TRANSFER_DATE DESC
    """
    return _self.session.sql(query).to_pandas()
```

### 4. Users & Queries Page (`4_👥_Users_and_Queries.py`)

**Key Sections:**
- User activity overview
- Query performance by user
- Role-based access patterns
- Session analysis
- Collaboration insights
- Cost attribution by user/role

**Queries Needed:**
```python
@st.cache_data(ttl=3600)
def get_user_activity_summary(_self, days):
    query = f"""
    SELECT
        USER_NAME,
        COUNT(DISTINCT QUERY_ID) AS QUERY_COUNT,
        COUNT(DISTINCT SESSION_ID) AS SESSION_COUNT,
        SUM(CASE WHEN EXECUTION_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCESSFUL_QUERIES,
        AVG(TOTAL_ELAPSED_TIME)/1000 AS AVG_EXECUTION_TIME_SEC,
        SUM(BYTES_SCANNED) AS TOTAL_BYTES_SCANNED
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
    GROUP BY USER_NAME
    ORDER BY QUERY_COUNT DESC
    """
    return _self.session.sql(query).to_pandas()
```

### 6. Data Pipelines Page (`6_🔧_Data_Pipelines.py`)

**Key Sections:**
- Task execution monitoring
- Snowpipe ingestion tracking
- Snowpipe Streaming metrics
- Dynamic table refreshes

**Queries Needed:**
```python
@st.cache_data(ttl=3600)
def get_task_history(_self, days):
    query = f"""
    WITH task_runs AS (
        SELECT
            NAME AS TASK_NAME,
            DATABASE_NAME,
            SCHEMA_NAME,
            STATE,
            SCHEDULED_TIME,
            COMPLETED_TIME,
            DATEDIFF('SECOND', SCHEDULED_TIME, COMPLETED_TIME) AS DURATION_SEC
        FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
        WHERE SCHEDULED_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
    )
    SELECT
        TASK_NAME,
        COUNT(*) AS TOTAL_RUNS,
        SUM(CASE WHEN STATE = 'SUCCEEDED' THEN 1 ELSE 0 END) AS SUCCESSFUL_RUNS,
        SUM(CASE WHEN STATE = 'FAILED' THEN 1 ELSE 0 END) AS FAILED_RUNS,
        AVG(DURATION_SEC) AS AVG_DURATION_SEC
    FROM task_runs
    GROUP BY TASK_NAME
    ORDER BY FAILED_RUNS DESC, TOTAL_RUNS DESC
    """
    return _self.session.sql(query).to_pandas()
```

### 7. Performance Page (`7_⚡_Performance.py`)

**Key Sections:**
- Query performance issues
- Pruning efficiency analysis
- Spilling detection
- Compilation overhead tracking
- Warehouse bottlenecks

**Query Already Available:**
- `get_query_performance_insights(days)` - ✅ In utils

**Additional Queries Needed:**
```python
@st.cache_data(ttl=3600)
def get_pruning_efficiency(_self, days):
    query = f"""
    SELECT
        TABLE_NAME,
        DATABASE_NAME,
        SCHEMA_NAME,
        SUM(PARTITIONS_SCANNED) AS TOTAL_PARTITIONS_SCANNED,
        SUM(PARTITIONS_PRUNED) AS TOTAL_PARTITIONS_PRUNED,
        AVG(PARTITIONS_SCANNED::FLOAT / NULLIF(PARTITIONS_TOTAL, 0)) AS AVG_SCAN_RATIO
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_QUERY_PRUNING_HISTORY
    WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
    GROUP BY TABLE_NAME, DATABASE_NAME, SCHEMA_NAME
    HAVING COUNT(*) > 10
    ORDER BY AVG_SCAN_RATIO DESC
    LIMIT 50
    """
    return _self.session.sql(query).to_pandas()
```

### 8. Security Page (`8_🔒_Security.py`)

**Key Sections:**
- Access pattern analysis
- Login activity monitoring
- Failed login tracking
- User activity distribution
- Audit trail

**Queries Needed:**
```python
@st.cache_data(ttl=3600)
def get_access_patterns(_self, days):
    query = f"""
    SELECT
        USER_NAME,
        COUNT(DISTINCT QUERY_ID) AS ACCESS_COUNT,
        COUNT(DISTINCT DATE_TRUNC('DAY', QUERY_START_TIME)) AS ACTIVE_DAYS
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
    WHERE QUERY_START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
    GROUP BY USER_NAME
    ORDER BY ACCESS_COUNT DESC
    """
    return _self.session.sql(query).to_pandas()

@st.cache_data(ttl=3600)
def get_login_history(_self, days):
    query = f"""
    SELECT
        USER_NAME,
        CLIENT_IP,
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
```

### 9. Cost Management Page (`9_💰_Cost_Management.py`)

**Key Sections:**
- Multi-dimensional cost attribution
- Anomaly detection (Z-score based)
- Savings opportunities
- Cost trends and forecasting
- Budget tracking

**Queries Needed:**
```python
@st.cache_data(ttl=3600)
def get_cost_attribution(_self, days):
    query = f"""
    SELECT
        'Warehouse' AS COST_TYPE,
        WAREHOUSE_NAME AS RESOURCE_NAME,
        SUM(CREDITS_USED) AS CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
    GROUP BY WAREHOUSE_NAME
    ORDER BY CREDITS DESC
    """
    return _self.session.sql(query).to_pandas()

@st.cache_data(ttl=3600)
def get_cost_anomalies(_self, days):
    query = f"""
    WITH daily_costs AS (
        SELECT
            DATE_TRUNC('DAY', START_TIME) AS COST_DATE,
            SUM(CREDITS_USED) AS DAILY_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
        GROUP BY COST_DATE
    ),
    cost_stats AS (
        SELECT
            AVG(DAILY_CREDITS) AS AVG_CREDITS,
            STDDEV(DAILY_CREDITS) AS STDDEV_CREDITS
        FROM daily_costs
    )
    SELECT
        c.COST_DATE,
        c.DAILY_CREDITS,
        s.AVG_CREDITS,
        ABS((c.DAILY_CREDITS - s.AVG_CREDITS) / NULLIF(s.STDDEV_CREDITS, 0)) AS Z_SCORE
    FROM daily_costs c
    CROSS JOIN cost_stats s
    WHERE ABS((c.DAILY_CREDITS - s.AVG_CREDITS) / NULLIF(s.STDDEV_CREDITS, 0)) > 2
    ORDER BY c.COST_DATE DESC
    """
    return _self.session.sql(query).to_pandas()
```

### 10. Data Quality Page (`10_✅_Data_Quality.py`)

**Key Sections:**
- Table freshness monitoring
- Schema change detection
- Stale data identification
- Quality scoring

**Queries Needed:**
```python
@st.cache_data(ttl=3600)
def get_table_freshness(_self):
    query = """
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
        AND BYTES > 0
    ORDER BY HOURS_SINCE_UPDATE DESC
    LIMIT 100
    """
    return _self.session.sql(query).to_pandas()

@st.cache_data(ttl=3600)
def get_schema_changes(_self, days):
    query = f"""
    SELECT
        TABLE_CATALOG AS DATABASE_NAME,
        TABLE_SCHEMA AS SCHEMA_NAME,
        TABLE_NAME,
        COLUMN_NAME,
        DATA_TYPE,
        LAST_ALTERED
    FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
    WHERE LAST_ALTERED >= DATEADD(DAY, -{days}, CURRENT_DATE())
    ORDER BY LAST_ALTERED DESC
    LIMIT 500
    """
    return _self.session.sql(query).to_pandas()
```

## 🎨 UI/UX Best Practices

### Layout Patterns

1. **KPI Row**:
   ```python
   col1, col2, col3, col4 = st.columns(4)
   with col1:
       st.metric("Metric 1", value)
   # ... etc
   ```

2. **Side-by-Side Charts**:
   ```python
   col1, col2 = st.columns(2)
   with col1:
       st.altair_chart(chart1, use_container_width=True)
   with col2:
       st.altair_chart(chart2, use_container_width=True)
   ```

3. **Tabbed Content**:
   ```python
   tab1, tab2, tab3 = st.tabs(["Tab 1", "Tab 2", "Tab 3"])
   with tab1:
       # Content for tab 1
   ```

4. **Expandable Details**:
   ```python
   with st.expander("View Details"):
       st.dataframe(data, use_container_width=True)
   ```

### Alert Patterns

```python
# Success
create_alert_badge("✅ All systems optimal", "success")

# Warning
create_alert_badge("⚠️ High queue times detected", "warning")

# Error
create_alert_badge("❌ Multiple failed queries", "error")

# Info
create_alert_badge("💡 Optimization opportunity detected", "info")
```

### Chart Colors

- **Blues**: Warehouse metrics, performance data
- **Greens**: Storage, optimization successes
- **Reds/Oranges**: Costs, alerts, issues
- **Purples**: AI/ML metrics
- **Grays**: Neutral data

## 🔐 Error Handling

Always wrap data loading in try-except blocks:

```python
try:
    data = queries.get_data(time_period)
    if data.empty:
        st.info("No data available for this period")
    else:
        # Process and display data
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    # Optionally show debug info
    if st.checkbox("Show debug info"):
        st.exception(e)
```

## 📱 Responsive Design

Use Streamlit's column system for responsiveness:

```python
# Desktop: 4 columns, Mobile: 2 columns
col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

# Flexible widths
col1, col2 = st.columns([2, 1])  # 2:1 ratio
```

## 🚀 Performance Optimization

1. **Use caching**: All query functions use `@st.cache_data(ttl=3600)`

2. **Limit results**: Add `LIMIT` clauses to queries

3. **Lazy loading**: Use expanders and tabs to defer expensive operations

4. **Efficient queries**: Use CTEs and appropriate indexes

## 📊 Chart Best Practices

1. **Always check for empty data**:
   ```python
   if not data.empty:
       chart = create_bar_chart(...)
       if chart:
           st.altair_chart(chart, use_container_width=True)
   else:
       st.info("No data available")
   ```

2. **Add helpful tooltips**:
   ```python
   tooltip=['column1', alt.Tooltip('column2:Q', format=',.2f', title='Friendly Name')]
   ```

3. **Use appropriate chart types**:
   - Trends: Line charts
   - Comparisons: Bar charts
   - Distributions: Pie/donut charts
   - Correlations: Scatter plots

## 🧪 Testing Your Pages

1. **Test with no data**: Ensure graceful handling

2. **Test with errors**: Trigger permission errors

3. **Test different time periods**: 1 day vs 90 days

4. **Test cost configurations**: Different credit/storage costs

5. **Test AI insights**: Ensure Cortex Complete works

## 📦 Dependencies

All required packages are included in Streamlit in Snowflake:
- streamlit
- pandas
- altair
- plotly
- numpy
- scipy
- snowflake-snowpark-python

## 🔄 Next Steps

1. **Create remaining pages** using the templates and patterns above

2. **Add new queries** to `utils.py` as needed

3. **Test thoroughly** with different data scenarios

4. **Customize styling** in `apply_custom_css()` in `utils.py`

5. **Enhance AI insights** by adding more specialized prompts

## 💡 Tips

- **Start simple**: Implement basic functionality first, then enhance
- **Reuse patterns**: Copy from existing pages (AI & ML, AI Insights)
- **Test incrementally**: Test each section as you build
- **Use AI**: The AI Insights page can help analyze your data
- **Customize**: Adjust colors, layouts, and metrics to your needs

## 📞 Need Help?

- Check existing pages for patterns
- Review `utils.py` for available functions
- Test with the AI Insights page
- Refer to [Streamlit docs](https://docs.streamlit.io/)
- Check [Snowflake docs](https://docs.snowflake.com/)

---

**Happy Building!** 🎉

This multi-page structure makes the dashboard modular, maintainable, and scalable.
Each page is independent yet shares common utilities and styling.
