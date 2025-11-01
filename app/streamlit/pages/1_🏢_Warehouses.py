"""
Warehouse Analytics & Optimization Page
=======================================
Comprehensive warehouse usage analysis, performance monitoring,
and automated optimization recommendations.
"""

import streamlit as st
import pandas as pd
import altair as alt
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
    page_title="Warehouses - Snowflake Observability",
    page_icon="🏢",
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
    "🏢 Warehouse Analytics & Optimization",
    "Monitor warehouse usage, performance, and get AI-powered optimization recommendations",
    "🏢"
)

# Get configuration
time_period = st.session_state.time_period
credit_cost = st.session_state.credit_cost

# ============================================================================
# LOAD DATA
# ============================================================================

with st.spinner("Loading warehouse analytics data..."):
    try:
        warehouse_metrics = queries.get_warehouse_metrics(time_period)
        warehouse_recs = queries.get_warehouse_recommendations(time_period)
    except Exception as e:
        st.error(f"Error loading warehouse data: {str(e)}")
        st.stop()

# ============================================================================
# OVERVIEW METRICS
# ============================================================================

st.subheader("📊 Overview")

if not warehouse_metrics.empty:
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

    total_credits = warehouse_metrics['TOTAL_CREDITS'].sum()
    avg_credits = warehouse_metrics['TOTAL_CREDITS'].mean()
    max_credits = warehouse_metrics['TOTAL_CREDITS'].max()
    num_warehouses = len(warehouse_metrics)

    with kpi_col1:
        st.metric(
            "Total Credits",
            f"{total_credits:,.1f}",
            help=f"Total credits used across all warehouses in {time_period} days"
        )

    with kpi_col2:
        total_cost = total_credits * credit_cost
        st.metric(
            "Estimated Cost",
            f"${total_cost:,.2f}",
            help=f"Based on ${credit_cost} per credit"
        )

    with kpi_col3:
        st.metric(
            "Active Warehouses",
            num_warehouses,
            help="Number of warehouses with activity"
        )

    with kpi_col4:
        avg_daily_cost = total_cost / time_period
        st.metric(
            "Avg Daily Cost",
            f"${avg_daily_cost:,.2f}",
            help="Average daily warehouse cost"
        )

# ============================================================================
# WAREHOUSE USAGE BREAKDOWN
# ============================================================================

st.markdown("---")
st.subheader("📈 Warehouse Usage Breakdown")

if not warehouse_metrics.empty:
    tab1, tab2, tab3 = st.tabs(["💳 Credit Usage", "⚡ Performance", "🎯 Recommendations"])

    # ========================================================================
    # TAB 1: CREDIT USAGE
    # ========================================================================

    with tab1:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### Top 10 Warehouses by Credits")

            top_10 = warehouse_metrics.head(10).copy()
            top_10['COST'] = top_10['TOTAL_CREDITS'] * credit_cost

            chart = alt.Chart(top_10).mark_bar().encode(
                y=alt.Y('WAREHOUSE_NAME:N', sort='-x', title='Warehouse'),
                x=alt.X('TOTAL_CREDITS:Q', title='Credits Used'),
                color=alt.Color('TOTAL_CREDITS:Q', scale=alt.Scale(scheme='blues'), legend=None),
                tooltip=[
                    'WAREHOUSE_NAME',
                    alt.Tooltip('TOTAL_CREDITS:Q', format=',.2f', title='Credits'),
                    alt.Tooltip('COST:Q', format='$,.2f', title='Cost'),
                    alt.Tooltip('ACTIVE_DAYS:Q', title='Active Days')
                ]
            ).properties(height=400)

            st.altair_chart(chart, use_container_width=True)

        with col2:
            st.markdown("#### Credit Distribution")

            # Pie chart for top 5 + others
            top_5 = warehouse_metrics.head(5).copy()
            others_credits = warehouse_metrics.iloc[5:]['TOTAL_CREDITS'].sum() if len(warehouse_metrics) > 5 else 0

            if others_credits > 0:
                others_row = pd.DataFrame([{
                    'WAREHOUSE_NAME': 'Others',
                    'TOTAL_CREDITS': others_credits
                }])
                pie_data = pd.concat([top_5[['WAREHOUSE_NAME', 'TOTAL_CREDITS']], others_row], ignore_index=True)
            else:
                pie_data = top_5[['WAREHOUSE_NAME', 'TOTAL_CREDITS']]

            chart = alt.Chart(pie_data).mark_arc(outerRadius=120).encode(
                theta=alt.Theta(field="TOTAL_CREDITS", type="quantitative"),
                color=alt.Color(field="WAREHOUSE_NAME", type="nominal", scale=alt.Scale(scheme='category10')),
                tooltip=['WAREHOUSE_NAME', alt.Tooltip('TOTAL_CREDITS:Q', format=',.2f')]
            ).properties(height=300)

            st.altair_chart(chart, use_container_width=True)

            # Summary stats
            top_5_pct = (top_5['TOTAL_CREDITS'].sum() / total_credits * 100) if total_credits > 0 else 0
            st.info(f"💡 Top 5 warehouses account for {top_5_pct:.1f}% of total credits")

        # Detailed warehouse table
        with st.expander("📋 View All Warehouse Details"):
            display_df = warehouse_metrics.copy()
            display_df['COST'] = display_df['TOTAL_CREDITS'] * credit_cost
            display_df['AVG_DAILY_COST'] = display_df['COST'] / time_period

            # Select and format columns
            display_cols = [
                'WAREHOUSE_NAME',
                'TOTAL_CREDITS',
                'COST',
                'AVG_DAILY_CREDITS',
                'MAX_DAILY_CREDITS',
                'ACTIVE_DAYS',
                'AVG_RUNNING_QUERIES',
                'AVG_QUEUED_LOAD'
            ]

            # Filter to existing columns
            display_cols = [col for col in display_cols if col in display_df.columns]

            display_table = display_df[display_cols].copy()

            # Rename for clarity
            column_rename = {
                'WAREHOUSE_NAME': 'Warehouse',
                'TOTAL_CREDITS': 'Total Credits',
                'COST': 'Total Cost ($)',
                'AVG_DAILY_CREDITS': 'Avg Daily Credits',
                'MAX_DAILY_CREDITS': 'Max Daily Credits',
                'ACTIVE_DAYS': 'Active Days',
                'AVG_RUNNING_QUERIES': 'Avg Running Queries',
                'AVG_QUEUED_LOAD': 'Avg Queue Load'
            }

            display_table = display_table.rename(columns={k: v for k, v in column_rename.items() if k in display_table.columns})

            st.dataframe(display_table, use_container_width=True)

            # Download button
            csv = display_table.to_csv(index=False)
            st.download_button(
                "📥 Download Warehouse Data",
                csv,
                file_name=f"warehouse_analytics_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

    # ========================================================================
    # TAB 2: PERFORMANCE
    # ========================================================================

    with tab2:
        st.markdown("#### Warehouse Load Analysis")

        # Filter warehouses with load data
        load_data = warehouse_metrics[warehouse_metrics['AVG_RUNNING_QUERIES'].notna()].copy()

        if not load_data.empty:
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("##### Queue Analysis")

                # Sort by queue load
                high_queue = load_data.nlargest(10, 'AVG_QUEUED_LOAD')

                chart = alt.Chart(high_queue).mark_bar(color='#e74c3c').encode(
                    y=alt.Y('WAREHOUSE_NAME:N', sort='-x', title='Warehouse'),
                    x=alt.X('AVG_QUEUED_LOAD:Q', title='Avg Queued Load'),
                    tooltip=[
                        'WAREHOUSE_NAME',
                        alt.Tooltip('AVG_QUEUED_LOAD:Q', format='.2f', title='Avg Queued Load'),
                        alt.Tooltip('AVG_RUNNING_QUERIES:Q', format='.2f', title='Avg Running'),
                        alt.Tooltip('TOTAL_CREDITS:Q', format=',.2f', title='Credits')
                    ]
                ).properties(height=300)

                st.altair_chart(chart, use_container_width=True)

                # Alert for high queue
                high_queue_count = len(load_data[load_data['AVG_QUEUED_LOAD'] > 1])
                if high_queue_count > 0:
                    create_alert_badge(
                        f"⚠️ {high_queue_count} warehouse(s) with high queue load (>1)",
                        "warning"
                    )

            with col2:
                st.markdown("##### Concurrency Analysis")

                # Sort by running queries
                high_concurrent = load_data.nlargest(10, 'AVG_RUNNING_QUERIES')

                chart = alt.Chart(high_concurrent).mark_bar(color='#3498db').encode(
                    y=alt.Y('WAREHOUSE_NAME:N', sort='-x', title='Warehouse'),
                    x=alt.X('AVG_RUNNING_QUERIES:Q', title='Avg Running Queries'),
                    tooltip=[
                        'WAREHOUSE_NAME',
                        alt.Tooltip('AVG_RUNNING_QUERIES:Q', format='.2f', title='Avg Running'),
                        alt.Tooltip('AVG_QUEUED_LOAD:Q', format='.2f', title='Avg Queued'),
                        alt.Tooltip('TOTAL_CREDITS:Q', format=',.2f', title='Credits')
                    ]
                ).properties(height=300)

                st.altair_chart(chart, use_container_width=True)

            # Scatter plot: Queue vs Concurrency
            st.markdown("##### Queue Load vs Concurrency")

            scatter = alt.Chart(load_data).mark_circle(size=100).encode(
                x=alt.X('AVG_RUNNING_QUERIES:Q', title='Avg Running Queries'),
                y=alt.Y('AVG_QUEUED_LOAD:Q', title='Avg Queued Load'),
                size=alt.Size('TOTAL_CREDITS:Q', title='Total Credits'),
                color=alt.Color('WAREHOUSE_NAME:N', legend=None),
                tooltip=[
                    'WAREHOUSE_NAME',
                    alt.Tooltip('AVG_RUNNING_QUERIES:Q', format='.2f'),
                    alt.Tooltip('AVG_QUEUED_LOAD:Q', format='.2f'),
                    alt.Tooltip('TOTAL_CREDITS:Q', format=',.2f')
                ]
            ).properties(height=400).interactive()

            st.altair_chart(scatter, use_container_width=True)

        else:
            st.info("No warehouse load data available for the selected period")

    # ========================================================================
    # TAB 3: RECOMMENDATIONS
    # ========================================================================

    with tab3:
        st.markdown("#### 💡 Automated Optimization Recommendations")

        if not warehouse_recs.empty:
            # Filter for actionable recommendations
            actionable_recs = warehouse_recs[warehouse_recs['RECOMMENDATION'] != 'OPTIMAL']

            if not actionable_recs.empty:
                # Group by recommendation type
                rec_summary = actionable_recs.groupby('RECOMMENDATION').size().reset_index(name='COUNT')

                col1, col2 = st.columns([1, 2])

                with col1:
                    st.markdown("##### Recommendation Summary")
                    for _, row in rec_summary.iterrows():
                        rec_type = row['RECOMMENDATION']
                        count = row['COUNT']

                        if rec_type == 'UPSIZE':
                            st.metric("🔼 Upsize", count, help="Warehouses that need more capacity")
                        elif rec_type == 'DOWNSIZE':
                            st.metric("🔽 Downsize", count, help="Warehouses that can be reduced")
                        elif rec_type == 'SUSPEND_OR_DROP':
                            st.metric("⏸️ Suspend/Drop", count, help="Unused warehouses")

                with col2:
                    st.markdown("##### Top Recommendations")

                    for _, rec in actionable_recs.head(5).iterrows():
                        rec_type = rec['RECOMMENDATION']

                        if rec_type == 'UPSIZE':
                            alert_type = "warning"
                            icon = "🔼"
                        elif rec_type == 'DOWNSIZE':
                            alert_type = "info"
                            icon = "🔽"
                        else:
                            alert_type = "error"
                            icon = "⏸️"

                        # Calculate potential savings for downsize
                        message = f"{icon} **{rec['WAREHOUSE_NAME']}**: {rec['REASON']}"

                        if rec_type == 'DOWNSIZE':
                            # Estimate 30-50% savings from downsizing
                            potential_savings = rec['TOTAL_CREDITS'] * 0.4 * credit_cost
                            message += f" (Potential savings: ~${potential_savings:,.2f})"

                        create_alert_badge(message, alert_type)

                # AI-powered recommendations
                st.markdown("---")
                st.markdown("##### 🤖 AI-Powered Recommendations")

                if st.button("Generate AI Optimization Insights", use_container_width=True):
                    with st.spinner("AI analyzing warehouse configurations..."):
                        try:
                            context = {
                                'total_warehouses': len(warehouse_metrics),
                                'actionable_recommendations': actionable_recs.to_dict('records'),
                                'total_credits': float(total_credits),
                                'time_period': time_period
                            }

                            insight = ai_insights.generate_insight(
                                str(context),
                                "warehouse_optimization"
                            )

                            st.markdown(f"""
                            <div class="insight-card">
                                {insight}
                            </div>
                            """, unsafe_allow_html=True)

                        except Exception as e:
                            st.error(f"AI insights unavailable: {str(e)}")

            else:
                create_alert_badge("✅ All warehouses are optimally configured!", "success")

                st.markdown("""
                **No action needed!** Your warehouses are:
                - Appropriately sized for their workload
                - Not experiencing queue delays
                - Actively being used
                """)

        else:
            st.warning("Unable to generate recommendations. Check data availability.")

# ============================================================================
# DAILY USAGE TRENDS
# ============================================================================

st.markdown("---")
st.subheader("📅 Daily Usage Trends")

try:
    # Get daily usage trend
    daily_usage_query = f"""
    SELECT
        DATE_TRUNC('DAY', START_TIME) AS USAGE_DATE,
        WAREHOUSE_NAME,
        SUM(CREDITS_USED) AS DAILY_CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
    GROUP BY USAGE_DATE, WAREHOUSE_NAME
    ORDER BY USAGE_DATE, DAILY_CREDITS DESC
    """
    daily_usage = session.sql(daily_usage_query).to_pandas()

    if not daily_usage.empty:
        daily_usage['USAGE_DATE'] = pd.to_datetime(daily_usage['USAGE_DATE'])

        # Get top 5 warehouses for trend
        top_5_names = warehouse_metrics.head(5)['WAREHOUSE_NAME'].tolist()
        daily_top_5 = daily_usage[daily_usage['WAREHOUSE_NAME'].isin(top_5_names)]

        # Line chart
        chart = alt.Chart(daily_top_5).mark_line(point=True).encode(
            x=alt.X('USAGE_DATE:T', title='Date'),
            y=alt.Y('DAILY_CREDITS:Q', title='Daily Credits'),
            color=alt.Color('WAREHOUSE_NAME:N', scale=alt.Scale(scheme='category10')),
            tooltip=['USAGE_DATE:T', 'WAREHOUSE_NAME', alt.Tooltip('DAILY_CREDITS:Q', format=',.2f')]
        ).properties(height=400)

        st.altair_chart(chart, use_container_width=True)

        # Insights
        total_by_date = daily_usage.groupby('USAGE_DATE')['DAILY_CREDITS'].sum()
        if len(total_by_date) >= 2:
            recent_avg = total_by_date.tail(7).mean()
            overall_avg = total_by_date.mean()
            trend_pct = ((recent_avg - overall_avg) / overall_avg * 100) if overall_avg > 0 else 0

            if abs(trend_pct) > 10:
                trend_msg = f"Recent 7-day average is {abs(trend_pct):.1f}% {'higher' if trend_pct > 0 else 'lower'} than overall average"
                st.info(f"📊 {trend_msg}")

except Exception as e:
    st.error(f"Error loading daily trends: {str(e)}")

# ============================================================================
# COST PROJECTION
# ============================================================================

st.markdown("---")
st.subheader("💰 Cost Projection")

try:
    # Simple linear projection
    if not daily_usage.empty and len(daily_usage) >= 7:
        from scipy import stats

        # Aggregate daily totals
        daily_totals = daily_usage.groupby('USAGE_DATE')['DAILY_CREDITS'].sum().reset_index()
        daily_totals['DAY_INDEX'] = range(len(daily_totals))

        # Linear regression
        slope, intercept, r_value, p_value, std_err = stats.linregress(
            daily_totals['DAY_INDEX'],
            daily_totals['DAILY_CREDITS']
        )

        # Project next 30 days
        last_date = daily_totals['USAGE_DATE'].max()
        future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=30)

        projection_df = pd.DataFrame({
            'USAGE_DATE': future_dates,
            'DAY_INDEX': range(len(daily_totals), len(daily_totals) + 30),
            'PROJECTED_CREDITS': [max(0, intercept + slope * i) for i in range(len(daily_totals), len(daily_totals) + 30)]
        })
        projection_df['PROJECTED_COST'] = projection_df['PROJECTED_CREDITS'] * credit_cost

        # Combine actual and projected
        actual_df = daily_totals[['USAGE_DATE', 'DAILY_CREDITS']].rename(columns={'DAILY_CREDITS': 'VALUE'})
        actual_df['TYPE'] = 'Actual'
        actual_df['COST'] = actual_df['VALUE'] * credit_cost

        projected_df = projection_df[['USAGE_DATE', 'PROJECTED_CREDITS']].rename(columns={'PROJECTED_CREDITS': 'VALUE'})
        projected_df['TYPE'] = 'Projected'
        projected_df['COST'] = projected_df['VALUE'] * credit_cost

        combined_df = pd.concat([actual_df, projected_df])

        # Chart
        chart = alt.Chart(combined_df).mark_line(point=True).encode(
            x=alt.X('USAGE_DATE:T', title='Date'),
            y=alt.Y('COST:Q', title='Daily Cost ($)'),
            color=alt.Color('TYPE:N', scale=alt.Scale(domain=['Actual', 'Projected'], range=['#3498db', '#e74c3c'])),
            strokeDash=alt.condition(
                alt.datum.TYPE == 'Projected',
                alt.value([5, 5]),
                alt.value([0])
            ),
            tooltip=['USAGE_DATE:T', alt.Tooltip('COST:Q', format='$,.2f'), 'TYPE']
        ).properties(height=300)

        st.altair_chart(chart, use_container_width=True)

        # Summary
        projected_30day_cost = projection_df['PROJECTED_COST'].sum()
        st.info(f"📊 Projected cost for next 30 days: **${projected_30day_cost:,.2f}** (based on current trends)")

except Exception as e:
    st.warning(f"Cost projection unavailable: {str(e)}")

# Footer
st.markdown("---")
st.caption(f"📅 Analysis period: {time_period} days | 💵 Credit cost: ${credit_cost}/credit")
st.caption("💡 **Tip:** Adjust time period and cost settings in the sidebar for customized analysis")
