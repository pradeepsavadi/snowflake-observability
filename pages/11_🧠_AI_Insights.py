"""
AI-Powered Insights Page - Interactive Custom Analysis
======================================================
Specialized page for interactive AI-powered analysis using Cortex Complete.
Users can ask custom questions and get AI-generated insights.
"""

import streamlit as st
import pandas as pd
import json
from datetime import datetime
import sys
sys.path.append('..')

from utils import (
    initialize_session_state,
    render_settings_sidebar,
    get_snowflake_session,
    format_bytes,
    format_number,
    apply_custom_css,
    render_page_header,
    create_alert_badge,
    SnowflakeQueries,
    AIInsightsGenerator
)

# Page configuration
st.set_page_config(
    page_title="AI Insights - Snowflake Observability",
    page_icon="🧠",
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
    "🧠 AI-Powered Insights",
    "Interactive AI analysis using Snowflake Cortex Complete",
    "🧠"
)

# Get time period from session state
time_period = st.session_state.time_period
credit_cost = st.session_state.credit_cost

# ============================================================================
# CORTEX AVAILABILITY CHECK
# ============================================================================

cortex_available = ai_insights.check_cortex_availability()

if not cortex_available:
    st.error("""
    ⚠️ **Cortex Complete is not available**

    AI insights require Snowflake Cortex Complete. Please ensure:
    - Your account has Cortex enabled
    - Your role has `USAGE` privileges on the SNOWFLAKE database
    - Run: `GRANT USAGE ON DATABASE SNOWFLAKE TO ROLE <your_role>;`
    """)
    st.stop()

st.success("✅ Cortex Complete is available and ready")

# ============================================================================
# INTERACTIVE AI QUERY INTERFACE
# ============================================================================

st.markdown("---")
st.subheader("💬 Ask AI About Your Snowflake Environment")

st.markdown("""
Use the interface below to ask custom questions about your Snowflake environment.
The AI will analyze available data and provide insights, recommendations, and answers.
""")

# Create tabs for different AI interaction modes
ai_tabs = st.tabs(["Custom Query", "Pre-built Analyses", "Data Explorer"])

# ============================================================================
# TAB 1: CUSTOM QUERY
# ============================================================================

with ai_tabs[0]:
    st.markdown("#### Ask a Custom Question")

    st.markdown("""
    **Examples:**
    - "What are the top 3 cost optimization opportunities in my environment?"
    - "Analyze warehouse performance trends and suggest improvements"
    - "Which tables are growing fastest and might cause issues?"
    - "Summarize security risks based on recent login patterns"
    - "What queries are most expensive and how can I optimize them?"
    """)

    # User input
    user_question = st.text_area(
        "Your Question",
        height=100,
        placeholder="Type your question here...",
        help="Ask anything about your Snowflake environment"
    )

    # Data context options
    col1, col2 = st.columns(2)

    with col1:
        include_warehouse_data = st.checkbox("Include warehouse metrics", value=True)
        include_storage_data = st.checkbox("Include storage metrics", value=True)
        include_query_data = st.checkbox("Include query performance", value=True)

    with col2:
        include_cost_data = st.checkbox("Include cost data", value=True)
        include_user_data = st.checkbox("Include user activity", value=False)
        include_security_data = st.checkbox("Include security data", value=False)

    # Advanced options
    with st.expander("⚙️ Advanced Options"):
        col1, col2 = st.columns(2)

        with col1:
            ai_temperature = st.slider(
                "Temperature (creativity)",
                min_value=0.0,
                max_value=1.0,
                value=0.3,
                step=0.1,
                help="Higher = more creative, Lower = more focused"
            )

        with col2:
            max_tokens = st.number_input(
                "Max Response Length (tokens)",
                min_value=100,
                max_value=2000,
                value=1000,
                step=100
            )

    # Generate insights button
    if st.button("🚀 Generate AI Insights", type="primary", use_container_width=True):
        if not user_question:
            st.warning("Please enter a question first")
        else:
            with st.spinner("🤖 AI is analyzing your Snowflake environment..."):
                try:
                    # Gather context data based on selections
                    context_data = {}

                    if include_warehouse_data:
                        wh_metrics = queries.get_warehouse_metrics(time_period)
                        if not wh_metrics.empty:
                            context_data['warehouse_summary'] = {
                                'total_warehouses': len(wh_metrics),
                                'total_credits': float(wh_metrics['TOTAL_CREDITS'].sum()),
                                'top_warehouse': wh_metrics.iloc[0]['WAREHOUSE_NAME'],
                                'top_warehouse_credits': float(wh_metrics.iloc[0]['TOTAL_CREDITS'])
                            }

                    if include_storage_data:
                        storage_metrics = queries.get_storage_metrics(time_period)
                        if not storage_metrics.empty:
                            total_storage = storage_metrics['TOTAL_BYTES'].sum()
                            context_data['storage_summary'] = {
                                'total_storage_gb': float(total_storage / (1024**3)),
                                'top_database': storage_metrics.iloc[0]['DATABASE_NAME'],
                                'num_databases': len(storage_metrics)
                            }

                    if include_query_data:
                        query_issues = queries.get_query_performance_insights(time_period)
                        if not query_issues.empty:
                            context_data['query_performance'] = {
                                'total_issues': int(query_issues['QUERY_COUNT'].sum()),
                                'issue_types': query_issues.to_dict('records')
                            }

                    if include_cost_data:
                        total_credits = context_data.get('warehouse_summary', {}).get('total_credits', 0)
                        total_storage_gb = context_data.get('storage_summary', {}).get('total_storage_gb', 0)
                        total_cost = total_credits * credit_cost + (total_storage_gb / 1024) * st.session_state.storage_cost_per_tb
                        context_data['cost_summary'] = {
                            'total_cost': float(total_cost),
                            'credit_cost': float(total_credits * credit_cost),
                            'storage_cost': float((total_storage_gb / 1024) * st.session_state.storage_cost_per_tb)
                        }

                    # Build the full prompt
                    full_context = f"""
User Question: {user_question}

Available Context Data:
{json.dumps(context_data, indent=2)}

Time Period: Last {time_period} days
Credit Cost: ${credit_cost} per credit
Storage Cost: ${st.session_state.storage_cost_per_tb} per TB/month

Please provide a detailed, actionable response based on the available data.
Include specific recommendations, metrics, and next steps.
                    """

                    # Update AI settings
                    ai_insights.temperature = ai_temperature
                    ai_insights.max_tokens = max_tokens

                    # Generate insight
                    response = ai_insights.generate_custom_insight(user_question, json.dumps(context_data, indent=2))

                    # Display response
                    st.markdown("### 🤖 AI Response")
                    st.markdown(f"""
                    <div class="insight-card">
                        {response}
                    </div>
                    """, unsafe_allow_html=True)

                    # Export options
                    st.markdown("---")
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        if st.button("💾 Save Response"):
                            st.session_state['last_ai_response'] = {
                                'question': user_question,
                                'response': response,
                                'timestamp': datetime.now().isoformat(),
                                'context': context_data
                            }
                            st.success("Response saved to session!")

                    with col2:
                        # Download as text
                        export_text = f"""
AI Insights Export
==================
Question: {user_question}
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Time Period: {time_period} days

Response:
{response}

Context Data:
{json.dumps(context_data, indent=2)}
                        """
                        st.download_button(
                            "📥 Download as Text",
                            export_text,
                            file_name=f"ai_insights_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                        )

                    with col3:
                        # Copy to clipboard (show code)
                        st.code(response, language=None)

                except Exception as e:
                    st.error(f"Error generating insights: {str(e)}")
                    st.exception(e)

# ============================================================================
# TAB 2: PRE-BUILT ANALYSES
# ============================================================================

with ai_tabs[1]:
    st.markdown("#### Quick Pre-built Analyses")

    st.markdown("Select a pre-built analysis to get instant insights:")

    analysis_col1, analysis_col2 = st.columns(2)

    with analysis_col1:
        st.markdown("**💰 Cost Analysis**")

        if st.button("Analyze Cost Trends", use_container_width=True):
            with st.spinner("Analyzing costs..."):
                try:
                    # Get cost data
                    cost_query = f"""
                    SELECT
                        DATE_TRUNC('DAY', START_TIME) AS DATE,
                        SUM(CREDITS_USED) * {credit_cost} AS DAILY_COST
                    FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
                    WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
                    GROUP BY DATE
                    ORDER BY DATE
                    """
                    cost_data = session.sql(cost_query).to_pandas()

                    cost_summary = {
                        'total_cost': float(cost_data['DAILY_COST'].sum()),
                        'avg_daily_cost': float(cost_data['DAILY_COST'].mean()),
                        'max_daily_cost': float(cost_data['DAILY_COST'].max()),
                        'min_daily_cost': float(cost_data['DAILY_COST'].min()),
                        'num_days': len(cost_data)
                    }

                    insight = ai_insights.generate_insight(
                        json.dumps(cost_summary, indent=2),
                        "cost_summary"
                    )

                    st.markdown(f"""
                    <div class="insight-card">
                        {insight}
                    </div>
                    """, unsafe_allow_html=True)

                except Exception as e:
                    st.error(f"Error: {str(e)}")

        if st.button("Find Cost Savings Opportunities", use_container_width=True):
            with st.spinner("Finding savings..."):
                try:
                    # Get warehouse recommendations
                    wh_recs = queries.get_warehouse_recommendations(time_period)
                    storage_issues = queries.get_table_storage_insights()

                    savings_context = {
                        'warehouse_recommendations': wh_recs.to_dict('records')[:5],
                        'storage_optimization_opportunities': storage_issues.to_dict('records')[:5]
                    }

                    insight = ai_insights.generate_custom_insight(
                        "Identify the top 5 cost savings opportunities based on warehouse and storage analysis. Provide specific dollar estimates and implementation steps.",
                        json.dumps(savings_context, indent=2)
                    )

                    st.markdown(f"""
                    <div class="insight-card">
                        {insight}
                    </div>
                    """, unsafe_allow_html=True)

                except Exception as e:
                    st.error(f"Error: {str(e)}")

    with analysis_col2:
        st.markdown("**⚡ Performance Analysis**")

        if st.button("Analyze Query Performance", use_container_width=True):
            with st.spinner("Analyzing performance..."):
                try:
                    query_issues = queries.get_query_performance_insights(time_period)

                    if not query_issues.empty:
                        perf_summary = query_issues.to_dict('records')

                        insight = ai_insights.generate_insight(
                            json.dumps(perf_summary, indent=2),
                            "performance_analysis"
                        )

                        st.markdown(f"""
                        <div class="insight-card">
                            {insight}
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.info("No performance issues detected!")

                except Exception as e:
                    st.error(f"Error: {str(e)}")

        if st.button("Warehouse Optimization Recommendations", use_container_width=True):
            with st.spinner("Generating recommendations..."):
                try:
                    wh_metrics = queries.get_warehouse_metrics(time_period)
                    wh_recs = queries.get_warehouse_recommendations(time_period)

                    wh_context = {
                        'warehouses': wh_metrics.to_dict('records')[:10],
                        'recommendations': wh_recs.to_dict('records')[:10]
                    }

                    insight = ai_insights.generate_insight(
                        json.dumps(wh_context, indent=2),
                        "warehouse_optimization"
                    )

                    st.markdown(f"""
                    <div class="insight-card">
                        {insight}
                    </div>
                    """, unsafe_allow_html=True)

                except Exception as e:
                    st.error(f"Error: {str(e)}")

# ============================================================================
# TAB 3: DATA EXPLORER
# ============================================================================

with ai_tabs[2]:
    st.markdown("#### Explore Your Data with AI")

    st.markdown("""
    Select a data source and let AI analyze it for insights.
    """)

    data_source = st.selectbox(
        "Select Data Source",
        [
            "Warehouse Metrics",
            "Storage Metrics",
            "Query Performance",
            "User Activity",
            "Cost Trends"
        ]
    )

    if st.button("🔍 Analyze Selected Data", use_container_width=True):
        with st.spinner(f"Analyzing {data_source}..."):
            try:
                if data_source == "Warehouse Metrics":
                    data = queries.get_warehouse_metrics(time_period)
                    prompt = "Analyze these warehouse metrics and provide insights on usage patterns, efficiency, and optimization opportunities."

                elif data_source == "Storage Metrics":
                    data = queries.get_storage_metrics(time_period)
                    prompt = "Analyze these storage metrics and identify growth patterns, optimization opportunities, and cost implications."

                elif data_source == "Query Performance":
                    data = queries.get_query_performance_insights(time_period)
                    prompt = "Analyze these query performance issues and recommend specific optimizations."

                elif data_source == "Cost Trends":
                    cost_query = f"""
                    SELECT
                        DATE_TRUNC('DAY', START_TIME) AS DATE,
                        SUM(CREDITS_USED) * {credit_cost} AS COST
                    FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
                    WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
                    GROUP BY DATE
                    ORDER BY DATE
                    """
                    data = session.sql(cost_query).to_pandas()
                    prompt = "Analyze these cost trends, identify anomalies, and suggest cost control measures."

                else:
                    st.warning("Data source analysis coming soon")
                    st.stop()

                # Display the data
                st.markdown("##### Raw Data Preview")
                st.dataframe(data.head(10), use_container_width=True)

                # Generate AI analysis
                st.markdown("##### AI Analysis")

                # Convert data to summary for AI
                data_summary = {
                    'total_rows': len(data),
                    'columns': list(data.columns),
                    'sample_data': data.head(5).to_dict('records'),
                    'summary_stats': data.describe().to_dict() if len(data) > 0 else {}
                }

                insight = ai_insights.generate_custom_insight(
                    prompt,
                    json.dumps(data_summary, indent=2)
                )

                st.markdown(f"""
                <div class="insight-card">
                    {insight}
                </div>
                """, unsafe_allow_html=True)

            except Exception as e:
                st.error(f"Error analyzing data: {str(e)}")

# ============================================================================
# SAVED INSIGHTS HISTORY
# ============================================================================

st.markdown("---")
st.subheader("📚 Saved Insights")

if 'last_ai_response' in st.session_state:
    with st.expander("View Last Saved Response"):
        saved = st.session_state['last_ai_response']

        st.markdown(f"**Question:** {saved['question']}")
        st.markdown(f"**Timestamp:** {saved['timestamp']}")
        st.markdown("**Response:**")
        st.markdown(f"""
        <div class="insight-card">
            {saved['response']}
        </div>
        """, unsafe_allow_html=True)

        if st.button("Clear Saved Response"):
            del st.session_state['last_ai_response']
            st.rerun()
else:
    st.info("No saved insights yet. Use the 'Save Response' button after generating insights.")

# ============================================================================
# TIPS & BEST PRACTICES
# ============================================================================

st.markdown("---")
st.subheader("💡 Tips for Effective AI Insights")

tips_col1, tips_col2 = st.columns(2)

with tips_col1:
    st.markdown("""
    **Ask Specific Questions:**
    - ✅ "Which warehouse has the highest queue time and why?"
    - ❌ "Tell me about warehouses"

    **Include Context:**
    - ✅ "Analyze cost spike in the last week for warehouse X"
    - ❌ "Why did costs increase?"
    """)

with tips_col2:
    st.markdown("""
    **Request Actionable Insights:**
    - ✅ "Suggest 3 ways to reduce storage costs by 20%"
    - ❌ "Storage is high"

    **Use Pre-built Analyses:**
    - Quick insights for common scenarios
    - Template questions for inspiration
    """)

# Footer
st.markdown("---")
st.caption(f"🤖 Powered by Snowflake Cortex Complete | Model: {ai_insights.default_model}")
st.caption(f"📅 Analysis Period: {time_period} days | Temperature: {ai_temperature} | Max Tokens: {max_tokens}")
