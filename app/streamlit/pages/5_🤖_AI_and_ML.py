"""
AI & ML Workload Monitoring Page (Cortex Services)
==================================================
Monitor Snowflake Cortex AI services including Analyst, Search, Fine-tuning, and Complete.
Enhanced with latest 2025 views and token-level metrics.
"""

import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
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
    create_metric_card,
    create_trend_chart,
    create_bar_chart,
    create_alert_badge,
    SnowflakeQueries,
    AIInsightsGenerator
)

# Page configuration
st.set_page_config(
    page_title="AI & ML - Snowflake Observability",
    page_icon="🤖",
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
ai_gen = AIInsightsGenerator(session)

# Page header
render_page_header(
    "🤖 AI & ML Workload Monitoring (Cortex)",
    "Monitor Snowflake Cortex AI services usage, costs, and performance",
    "🤖"
)

# Get configuration
time_period = st.session_state.time_period
credit_cost = st.session_state.credit_cost

# ============================================================================
# LOAD CORTEX USAGE DATA
# ============================================================================

with st.spinner("Loading Cortex AI usage data..."):
    cortex_usage = queries.get_cortex_usage(time_period)

# Calculate totals
total_cortex_credits = 0
analyst_requests = 0
search_queries = 0
finetuning_jobs = 0
complete_requests = 0
total_tokens = 0

if not cortex_usage['analyst'].empty:
    analyst_requests = cortex_usage['analyst']['REQUEST_COUNT'].sum()
    total_cortex_credits += cortex_usage['analyst']['TOTAL_CREDITS'].sum()

if not cortex_usage['search'].empty:
    search_queries = cortex_usage['search']['TOTAL_QUERIES'].sum()
    total_cortex_credits += cortex_usage['search']['TOTAL_CREDITS'].sum()

if not cortex_usage['finetuning'].empty:
    finetuning_jobs = cortex_usage['finetuning']['JOB_COUNT'].sum()
    total_cortex_credits += cortex_usage['finetuning']['TOTAL_CREDITS'].sum()

if not cortex_usage['complete'].empty:
    complete_requests = len(cortex_usage['complete'])
    if 'TOTAL_CREDITS' in cortex_usage['complete'].columns:
        total_cortex_credits += cortex_usage['complete']['TOTAL_CREDITS'].sum()
    if 'TOTAL_TOKENS' in cortex_usage['complete'].columns:
        total_tokens = cortex_usage['complete']['TOTAL_TOKENS'].sum()

# ============================================================================
# OVERVIEW METRICS
# ============================================================================

st.subheader("📊 Cortex Services Overview")

kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

with kpi_col1:
    st.metric(
        "Total Cortex Credits",
        f"{total_cortex_credits:,.2f}",
        help="Total credits used by all Cortex services"
    )

with kpi_col2:
    total_cost = total_cortex_credits * credit_cost
    st.metric(
        "Estimated Cost",
        f"${total_cost:,.2f}",
        help=f"Based on ${credit_cost}/credit"
    )

with kpi_col3:
    total_requests = analyst_requests + search_queries + complete_requests + finetuning_jobs
    st.metric(
        "Total Requests/Jobs",
        f"{total_requests:,}",
        help="All Cortex service calls"
    )

with kpi_col4:
    if total_tokens > 0:
        st.metric(
            "Total Tokens",
            format_number(total_tokens),
            help="Tokens used by Cortex Complete"
        )
    else:
        avg_credits_per_request = total_cortex_credits / total_requests if total_requests > 0 else 0
        st.metric(
            "Avg Credits/Request",
            f"{avg_credits_per_request:.4f}",
            help="Average credits per request"
        )

# Service breakdown
if total_cortex_credits > 0:
    st.markdown("#### Service Usage Distribution")

    service_breakdown = []
    if analyst_requests > 0:
        service_breakdown.append({
            'Service': 'Cortex Analyst',
            'Credits': cortex_usage['analyst']['TOTAL_CREDITS'].sum(),
            'Requests': analyst_requests
        })
    if search_queries > 0:
        service_breakdown.append({
            'Service': 'Cortex Search',
            'Credits': cortex_usage['search']['TOTAL_CREDITS'].sum(),
            'Requests': search_queries
        })
    if finetuning_jobs > 0:
        service_breakdown.append({
            'Service': 'Fine-Tuning',
            'Credits': cortex_usage['finetuning']['TOTAL_CREDITS'].sum(),
            'Requests': finetuning_jobs
        })
    if complete_requests > 0 and 'TOTAL_CREDITS' in cortex_usage['complete'].columns:
        service_breakdown.append({
            'Service': 'Cortex Complete',
            'Credits': cortex_usage['complete']['TOTAL_CREDITS'].sum(),
            'Requests': complete_requests
        })

    if service_breakdown:
        breakdown_df = pd.DataFrame(service_breakdown)

        col1, col2 = st.columns(2)

        with col1:
            # Pie chart for credit distribution
            fig = px.pie(
                breakdown_df,
                values='Credits',
                names='Service',
                title='Credit Distribution by Service',
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Bar chart for request volume
            chart = create_bar_chart(
                breakdown_df,
                'Service',
                'Requests',
                'Credits',
                'Request Volume by Service'
            )
            if chart:
                st.altair_chart(chart, use_container_width=True)

# ============================================================================
# CORTEX ANALYST TAB
# ============================================================================

st.markdown("---")
service_tabs = st.tabs(["Cortex Analyst", "Cortex Search", "Fine-Tuning", "Cortex Complete"])

with service_tabs[0]:
    st.subheader("🔍 Cortex Analyst Usage")

    if not cortex_usage['analyst'].empty:
        # Overview metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Requests", f"{analyst_requests:,}")

        with col2:
            avg_credits = cortex_usage['analyst']['AVG_CREDITS'].mean()
            st.metric("Avg Credits/Request", f"{avg_credits:.4f}")

        with col3:
            total_analyst_cost = cortex_usage['analyst']['TOTAL_CREDITS'].sum() * credit_cost
            st.metric("Total Cost", f"${total_analyst_cost:,.2f}")

        # Usage by user
        st.markdown("#### Usage by User")

        user_usage = cortex_usage['analyst'].groupby('USER_NAME').agg({
            'REQUEST_COUNT': 'sum',
            'TOTAL_CREDITS': 'sum'
        }).reset_index().sort_values('TOTAL_CREDITS', ascending=False)

        chart = create_bar_chart(
            user_usage.head(10),
            'USER_NAME',
            'TOTAL_CREDITS',
            'TOTAL_CREDITS',
            'Top 10 Users by Credits'
        )
        if chart:
            st.altair_chart(chart, use_container_width=True)

        # Trend over time
        if 'USAGE_DATE' in cortex_usage['analyst'].columns:
            st.markdown("#### Usage Trend")

            daily_usage = cortex_usage['analyst'].groupby('USAGE_DATE').agg({
                'REQUEST_COUNT': 'sum',
                'TOTAL_CREDITS': 'sum'
            }).reset_index()
            daily_usage['USAGE_DATE'] = pd.to_datetime(daily_usage['USAGE_DATE'])

            trend_col1, trend_col2 = st.columns(2)

            with trend_col1:
                chart = create_trend_chart(
                    daily_usage,
                    'USAGE_DATE',
                    'REQUEST_COUNT',
                    'Daily Request Volume'
                )
                if chart:
                    st.altair_chart(chart, use_container_width=True)

            with trend_col2:
                chart = create_trend_chart(
                    daily_usage,
                    'USAGE_DATE',
                    'TOTAL_CREDITS',
                    'Daily Credit Usage'
                )
                if chart:
                    st.altair_chart(chart, use_container_width=True)

        # Semantic models
        if 'SEMANTIC_MODEL_NAME' in cortex_usage['analyst'].columns:
            st.markdown("#### Usage by Semantic Model")

            model_usage = cortex_usage['analyst'].groupby('SEMANTIC_MODEL_NAME').agg({
                'REQUEST_COUNT': 'sum',
                'TOTAL_CREDITS': 'sum'
            }).reset_index().sort_values('TOTAL_CREDITS', ascending=False)

            st.dataframe(model_usage, use_container_width=True)

    else:
        st.info("No Cortex Analyst usage detected in the selected period")

# ============================================================================
# CORTEX SEARCH TAB
# ============================================================================

with service_tabs[1]:
    st.subheader("🔎 Cortex Search Usage")

    if not cortex_usage['search'].empty:
        # Overview metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Queries", f"{search_queries:,}")

        with col2:
            total_search_credits = cortex_usage['search']['TOTAL_CREDITS'].sum()
            st.metric("Total Credits", f"{total_search_credits:,.2f}")

        with col3:
            total_search_cost = total_search_credits * credit_cost
            st.metric("Total Cost", f"${total_search_cost:,.2f}")

        # Usage by service
        st.markdown("#### Usage by Search Service")

        service_usage = cortex_usage['search'].groupby('SERVICE_NAME').agg({
            'TOTAL_QUERIES': 'sum',
            'TOTAL_CREDITS': 'sum'
        }).reset_index().sort_values('TOTAL_CREDITS', ascending=False)

        col1, col2 = st.columns(2)

        with col1:
            chart = create_bar_chart(
                service_usage,
                'SERVICE_NAME',
                'TOTAL_QUERIES',
                'TOTAL_QUERIES',
                'Queries by Service'
            )
            if chart:
                st.altair_chart(chart, use_container_width=True)

        with col2:
            chart = create_bar_chart(
                service_usage,
                'SERVICE_NAME',
                'TOTAL_CREDITS',
                'TOTAL_CREDITS',
                'Credits by Service'
            )
            if chart:
                st.altair_chart(chart, use_container_width=True)

        # Daily trend
        if 'USAGE_DATE' in cortex_usage['search'].columns:
            st.markdown("#### Daily Search Activity")

            daily_search = cortex_usage['search'].groupby('USAGE_DATE').agg({
                'TOTAL_QUERIES': 'sum',
                'TOTAL_CREDITS': 'sum'
            }).reset_index()
            daily_search['USAGE_DATE'] = pd.to_datetime(daily_search['USAGE_DATE'])

            chart = create_trend_chart(
                daily_search,
                'USAGE_DATE',
                'TOTAL_QUERIES',
                'Daily Search Queries'
            )
            if chart:
                st.altair_chart(chart, use_container_width=True)

    else:
        st.info("No Cortex Search usage detected in the selected period")

# ============================================================================
# FINE-TUNING TAB
# ============================================================================

with service_tabs[2]:
    st.subheader("🎯 Fine-Tuning Jobs")

    if not cortex_usage['finetuning'].empty:
        # Overview metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Jobs", f"{finetuning_jobs:,}")

        with col2:
            total_ft_credits = cortex_usage['finetuning']['TOTAL_CREDITS'].sum()
            st.metric("Total Credits", f"{total_ft_credits:,.2f}")

        with col3:
            total_ft_cost = total_ft_credits * credit_cost
            st.metric("Total Cost", f"${total_ft_cost:,.2f}")

        # Jobs by model
        st.markdown("#### Jobs by Model")

        model_jobs = cortex_usage['finetuning'].groupby('MODEL_NAME').agg({
            'JOB_COUNT': 'sum',
            'TOTAL_CREDITS': 'sum'
        }).reset_index().sort_values('TOTAL_CREDITS', ascending=False)

        col1, col2 = st.columns(2)

        with col1:
            chart = create_bar_chart(
                model_jobs,
                'MODEL_NAME',
                'JOB_COUNT',
                'JOB_COUNT',
                'Jobs by Model'
            )
            if chart:
                st.altair_chart(chart, use_container_width=True)

        with col2:
            chart = create_bar_chart(
                model_jobs,
                'MODEL_NAME',
                'TOTAL_CREDITS',
                'TOTAL_CREDITS',
                'Credits by Model'
            )
            if chart:
                st.altair_chart(chart, use_container_width=True)

        # Jobs by user
        st.markdown("#### Jobs by User")

        user_jobs = cortex_usage['finetuning'].groupby('USER_NAME').agg({
            'JOB_COUNT': 'sum',
            'TOTAL_CREDITS': 'sum'
        }).reset_index().sort_values('TOTAL_CREDITS', ascending=False)

        st.dataframe(user_jobs, use_container_width=True)

    else:
        st.info("No Fine-Tuning jobs detected in the selected period")

# ============================================================================
# CORTEX COMPLETE TAB (NEW/ENHANCED)
# ============================================================================

with service_tabs[3]:
    st.subheader("💬 Cortex Complete Usage")

    st.markdown("""
    **Cortex Complete** provides LLM inference capabilities. This section monitors:
    - Request volumes and patterns
    - Token usage (prompt + completion)
    - Model-specific metrics
    - Cost attribution
    """)

    if not cortex_usage['complete'].empty:
        # Check if we have detailed token metrics
        has_token_metrics = 'TOTAL_TOKENS' in cortex_usage['complete'].columns

        if has_token_metrics:
            # Overview metrics with tokens
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Total Requests", f"{complete_requests:,}")

            with col2:
                st.metric("Total Tokens", format_number(total_tokens))

            with col3:
                if 'TOTAL_CREDITS' in cortex_usage['complete'].columns:
                    complete_credits = cortex_usage['complete']['TOTAL_CREDITS'].sum()
                    st.metric("Total Credits", f"{complete_credits:,.2f}")
                else:
                    st.metric("Total Credits", "N/A")

            with col4:
                if 'TOTAL_CREDITS' in cortex_usage['complete'].columns:
                    complete_cost = complete_credits * credit_cost
                    st.metric("Total Cost", f"${complete_cost:,.2f}")
                else:
                    st.metric("Total Cost", "N/A")

            # Token breakdown
            if 'PROMPT_TOKENS' in cortex_usage['complete'].columns:
                st.markdown("#### Token Usage Breakdown")

                total_prompt = cortex_usage['complete']['PROMPT_TOKENS'].sum()
                total_completion = cortex_usage['complete']['COMPLETION_TOKENS'].sum()

                token_col1, token_col2 = st.columns(2)

                with token_col1:
                    st.metric(
                        "Prompt Tokens",
                        format_number(total_prompt),
                        help="Tokens in user prompts"
                    )

                with token_col2:
                    st.metric(
                        "Completion Tokens",
                        format_number(total_completion),
                        help="Tokens in AI responses"
                    )

                # Token distribution pie chart
                token_dist_df = pd.DataFrame({
                    'Type': ['Prompt Tokens', 'Completion Tokens'],
                    'Count': [total_prompt, total_completion]
                })

                fig = px.pie(
                    token_dist_df,
                    values='Count',
                    names='Type',
                    title='Token Distribution',
                    color_discrete_sequence=['#3498db', '#e74c3c']
                )
                st.plotly_chart(fig, use_container_width=True)

            # Usage by model
            if 'MODEL_NAME' in cortex_usage['complete'].columns:
                st.markdown("#### Usage by Model")

                model_usage = cortex_usage['complete'].groupby('MODEL_NAME').agg({
                    'REQUEST_COUNT': 'sum',
                    'TOTAL_TOKENS': 'sum' if 'TOTAL_TOKENS' in cortex_usage['complete'].columns else 'count',
                    'TOTAL_CREDITS': 'sum' if 'TOTAL_CREDITS' in cortex_usage['complete'].columns else 'count'
                }).reset_index().sort_values('REQUEST_COUNT', ascending=False)

                st.dataframe(model_usage, use_container_width=True)

            # Daily trend
            if 'USAGE_DATE' in cortex_usage['complete'].columns:
                st.markdown("#### Daily Complete Activity")

                daily_complete = cortex_usage['complete'].groupby('USAGE_DATE').agg({
                    'REQUEST_COUNT': 'sum',
                    'TOTAL_TOKENS': 'sum' if 'TOTAL_TOKENS' in cortex_usage['complete'].columns else 'count'
                }).reset_index()
                daily_complete['USAGE_DATE'] = pd.to_datetime(daily_complete['USAGE_DATE'])

                trend_col1, trend_col2 = st.columns(2)

                with trend_col1:
                    chart = create_trend_chart(
                        daily_complete,
                        'USAGE_DATE',
                        'REQUEST_COUNT',
                        'Daily Requests'
                    )
                    if chart:
                        st.altair_chart(chart, use_container_width=True)

                with trend_col2:
                    if 'TOTAL_TOKENS' in daily_complete.columns:
                        chart = create_trend_chart(
                            daily_complete,
                            'USAGE_DATE',
                            'TOTAL_TOKENS',
                            'Daily Token Usage'
                        )
                        if chart:
                            st.altair_chart(chart, use_container_width=True)

        else:
            # Fallback view from metering history
            st.info("Detailed token metrics not available. Showing aggregate Cortex usage from metering history.")

            if 'SERVICE_TYPE' in cortex_usage['complete'].columns:
                service_usage = cortex_usage['complete'].groupby('SERVICE_TYPE').agg({
                    'TOTAL_CREDITS': 'sum'
                }).reset_index()

                st.dataframe(service_usage, use_container_width=True)

    else:
        st.info("""
        No Cortex Complete usage data available.

        **Note:** Cortex Complete usage tracking may require:
        - Account with Cortex Complete enabled
        - Recent Snowflake version with CORTEX_COMPLETE_USAGE_HISTORY view
        - Appropriate permissions
        """)

# ============================================================================
# AI INSIGHTS ON AI USAGE
# ============================================================================

st.markdown("---")
st.subheader("🤖 AI Insights on AI Usage")

try:
    context = {
        "Total Cortex Credits": float(total_cortex_credits),
        "Analyst Requests": int(analyst_requests),
        "Search Queries": int(search_queries),
        "Fine-Tuning Jobs": int(finetuning_jobs),
        "Complete Requests": int(complete_requests),
        "Total Tokens": int(total_tokens) if total_tokens > 0 else "N/A",
        "Time Period": f"{time_period} days"
    }

    if st.button("🚀 Generate AI Analysis of Cortex Usage", use_container_width=True):
        with st.spinner("Analyzing Cortex usage patterns..."):
            insight = ai_gen.generate_custom_insight(
                "Analyze the Cortex AI services usage patterns, costs, and provide optimization recommendations.",
                str(context)
            )

            st.markdown(f"""
            <div class="insight-card">
                {insight}
            </div>
            """, unsafe_allow_html=True)

except Exception as e:
    st.warning(f"AI insights unavailable: {str(e)}")

# Footer
st.markdown("---")
st.caption(f"📅 Analysis Period: {time_period} days | 💵 Credit Cost: ${credit_cost}/credit")
st.caption("💡 Tip: Use the 'AI Insights' page for deeper analysis of your Cortex usage patterns")
