"""
Snowflake Holistic Observability Dashboard - Cost Management Page
==================================================================
Detailed cost attribution, budgeting, anomaly detection, and savings opportunities
"""

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from scipy import stats
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
    page_title="Cost Management - Snowflake Observability",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom styling
apply_custom_css()

# Initialize session state and render settings
initialize_session_state()
render_settings_sidebar()

# Page header
render_page_header("💰 Cost Management", "Track costs, identify anomalies, and discover savings opportunities")

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
storage_cost = st.session_state.storage_cost_per_tb

# ============================================================================
# COST OVERVIEW
# ============================================================================

st.markdown("---")
st.subheader("💵 Cost Overview")

col1, col2, col3, col4 = st.columns(4)

with st.spinner("Loading cost metrics..."):
    try:
        # Get total credits
        credits_query = f"""
        SELECT
            SUM(CREDITS_USED) AS TOTAL_CREDITS,
            SUM(CREDITS_USED_COMPUTE) AS COMPUTE_CREDITS,
            SUM(CREDITS_USED_CLOUD_SERVICES) AS CLOUD_SERVICES_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        """
        credits_data = session.sql(credits_query).to_pandas()

        total_credits = credits_data['TOTAL_CREDITS'].iloc[0] if not credits_data.empty else 0
        compute_credits = credits_data['COMPUTE_CREDITS'].iloc[0] if not credits_data.empty else 0
        cloud_services_credits = credits_data['CLOUD_SERVICES_CREDITS'].iloc[0] if not credits_data.empty else 0

        # Get storage costs
        storage_metrics = queries.get_storage_metrics(time_period)
        total_storage_tb = storage_metrics['TOTAL_BYTES'].sum() / (1024**4) if not storage_metrics.empty else 0

        # Calculate costs
        compute_cost = compute_credits * credit_cost
        cloud_services_cost = cloud_services_credits * credit_cost
        storage_cost_total = total_storage_tb * storage_cost
        total_cost = compute_cost + cloud_services_cost + storage_cost_total

        # Calculate daily average
        daily_avg_cost = total_cost / time_period if time_period > 0 else 0

        with col1:
            st.metric(
                "Total Cost",
                f"${total_cost:,.2f}",
                help=f"Total cost for last {time_period} days"
            )

        with col2:
            st.metric(
                "Compute Cost",
                f"${compute_cost:,.2f}",
                help=f"{compute_credits:,.1f} credits"
            )

        with col3:
            st.metric(
                "Storage Cost",
                f"${storage_cost_total:,.2f}",
                help=f"{total_storage_tb:.2f} TB"
            )

        with col4:
            st.metric(
                "Daily Average",
                f"${daily_avg_cost:,.2f}",
                help="Average daily cost"
            )

        # Cost breakdown pie chart
        st.markdown("---")

        col1, col2 = st.columns([2, 1])

        with col1:
            st.markdown("#### Cost Distribution")

            cost_breakdown = pd.DataFrame({
                'Category': ['Compute', 'Cloud Services', 'Storage'],
                'Cost': [compute_cost, cloud_services_cost, storage_cost_total]
            })
            cost_breakdown = cost_breakdown[cost_breakdown['Cost'] > 0]

            fig = px.pie(
                cost_breakdown,
                values='Cost',
                names='Category',
                title='Cost Distribution by Category',
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            fig.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>$%{value:,.2f}<br>%{percent}'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("#### Cost Metrics")

            cost_per_credit = credit_cost
            credits_per_day = total_credits / time_period if time_period > 0 else 0

            st.metric("Cost per Credit", f"${cost_per_credit:.2f}")
            st.metric("Credits per Day", f"{credits_per_day:,.1f}")

            # Projected monthly cost
            monthly_projection = daily_avg_cost * 30
            st.metric("30-Day Projection", f"${monthly_projection:,.2f}")

    except Exception as e:
        st.error(f"Error loading cost overview: {str(e)}")

# ============================================================================
# COST TABS
# ============================================================================

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Cost Trends",
    "🏷️ Cost Attribution",
    "🚨 Anomaly Detection",
    "💡 Savings Opportunities",
    "📊 Budget Tracking"
])

# ----------------------------------------------------------------------------
# TAB 1: Cost Trends
# ----------------------------------------------------------------------------

with tab1:
    st.markdown("### 📈 Cost Trends & Forecasting")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Daily Cost Trend")

        try:
            daily_cost_query = f"""
            SELECT
                DATE_TRUNC('DAY', START_TIME) AS COST_DATE,
                SUM(CREDITS_USED) AS DAILY_CREDITS,
                SUM(CREDITS_USED) * {credit_cost} AS DAILY_COST
            FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            GROUP BY COST_DATE
            ORDER BY COST_DATE
            """
            daily_costs = session.sql(daily_cost_query).to_pandas()

            if not daily_costs.empty:
                daily_costs['COST_DATE'] = pd.to_datetime(daily_costs['COST_DATE'])

                # Create line chart with trend
                base = alt.Chart(daily_costs).encode(
                    x=alt.X('COST_DATE:T', title='Date', axis=alt.Axis(format='%Y-%m-%d'))
                )

                line = base.mark_line(strokeWidth=3, color='steelblue').encode(
                    y=alt.Y('DAILY_COST:Q', title='Daily Cost ($)'),
                    tooltip=[
                        alt.Tooltip('COST_DATE:T', title='Date', format='%Y-%m-%d'),
                        alt.Tooltip('DAILY_COST:Q', title='Cost', format='$,.2f'),
                        alt.Tooltip('DAILY_CREDITS:Q', title='Credits', format=',.2f')
                    ]
                )

                # Add 7-day moving average
                daily_costs['MA_7'] = daily_costs['DAILY_COST'].rolling(window=7, min_periods=1).mean()

                ma_line = alt.Chart(daily_costs).mark_line(strokeWidth=2, color='orange', strokeDash=[5, 5]).encode(
                    x='COST_DATE:T',
                    y=alt.Y('MA_7:Q', title=''),
                    tooltip=[alt.Tooltip('MA_7:Q', title='7-Day Avg', format='$,.2f')]
                )

                chart = (line + ma_line).properties(height=300)
                st.altair_chart(chart, use_container_width=True)

                # Cost statistics
                avg_cost = daily_costs['DAILY_COST'].mean()
                max_cost = daily_costs['DAILY_COST'].max()
                min_cost = daily_costs['DAILY_COST'].min()
                std_cost = daily_costs['DAILY_COST'].std()

                st.caption(f"Average: ${avg_cost:,.2f} | Max: ${max_cost:,.2f} | Min: ${min_cost:,.2f} | Std Dev: ${std_cost:,.2f}")

                # Trend analysis
                recent_avg = daily_costs.tail(7)['DAILY_COST'].mean()
                previous_avg = daily_costs.iloc[-14:-7]['DAILY_COST'].mean() if len(daily_costs) >= 14 else avg_cost
                trend_pct = ((recent_avg - previous_avg) / previous_avg * 100) if previous_avg > 0 else 0

                if trend_pct > 20:
                    st.warning(f"⚠️ Cost trending up: {trend_pct:.1f}% increase (last 7 days vs previous 7 days)")
                elif trend_pct < -20:
                    st.success(f"✅ Cost trending down: {abs(trend_pct):.1f}% decrease")
                else:
                    st.info(f"📊 Cost stable: {trend_pct:+.1f}% change")

            else:
                st.info("No daily cost data available")

        except Exception as e:
            st.error(f"Error loading cost trends: {str(e)}")

    with col2:
        st.markdown("#### Cost Forecast (30 Days)")

        try:
            if not daily_costs.empty and len(daily_costs) >= 7:
                # Linear regression forecast
                daily_costs['DAY_NUM'] = range(len(daily_costs))
                X = daily_costs['DAY_NUM'].values.reshape(-1, 1)
                y = daily_costs['DAILY_COST'].values

                slope, intercept, r_value, p_value, std_err = stats.linregress(daily_costs['DAY_NUM'], daily_costs['DAILY_COST'])

                # Generate forecast
                forecast_days = 30
                future_days = range(len(daily_costs), len(daily_costs) + forecast_days)
                forecast_dates = pd.date_range(
                    start=daily_costs['COST_DATE'].max() + timedelta(days=1),
                    periods=forecast_days,
                    freq='D'
                )
                forecast_costs = [slope * day + intercept for day in future_days]

                forecast_df = pd.DataFrame({
                    'COST_DATE': forecast_dates,
                    'FORECAST_COST': forecast_costs,
                    'TYPE': 'Forecast'
                })

                # Combine historical and forecast
                historical_df = daily_costs[['COST_DATE', 'DAILY_COST']].copy()
                historical_df['TYPE'] = 'Historical'
                historical_df = historical_df.rename(columns={'DAILY_COST': 'FORECAST_COST'})

                combined_df = pd.concat([historical_df, forecast_df], ignore_index=True)

                # Create chart
                chart = alt.Chart(combined_df).mark_line(strokeWidth=2).encode(
                    x=alt.X('COST_DATE:T', title='Date'),
                    y=alt.Y('FORECAST_COST:Q', title='Cost ($)'),
                    color=alt.Color('TYPE:N', scale=alt.Scale(domain=['Historical', 'Forecast'], range=['steelblue', 'orange'])),
                    strokeDash=alt.StrokeDash('TYPE:N', scale=alt.Scale(domain=['Historical', 'Forecast'], range=[[1], [5, 5]])),
                    tooltip=[
                        alt.Tooltip('COST_DATE:T', title='Date', format='%Y-%m-%d'),
                        alt.Tooltip('FORECAST_COST:Q', title='Cost', format='$,.2f'),
                        'TYPE'
                    ]
                ).properties(height=300)

                st.altair_chart(chart, use_container_width=True)

                # Forecast summary
                forecast_total = sum(forecast_costs)
                current_30day_total = daily_costs.tail(30)['DAILY_COST'].sum() if len(daily_costs) >= 30 else total_cost

                st.caption(f"**30-Day Forecast:** ${forecast_total:,.2f}")

                forecast_change_pct = ((forecast_total - current_30day_total) / current_30day_total * 100) if current_30day_total > 0 else 0

                if forecast_change_pct > 10:
                    st.warning(f"⚠️ Forecast shows {forecast_change_pct:.1f}% increase in next 30 days")
                elif forecast_change_pct < -10:
                    st.success(f"✅ Forecast shows {abs(forecast_change_pct):.1f}% decrease in next 30 days")
                else:
                    st.info(f"📊 Forecast relatively stable ({forecast_change_pct:+.1f}% change)")

                st.caption(f"R² = {r_value**2:.3f} | Trend: ${slope:.2f}/day")

            else:
                st.info("Insufficient data for forecasting (need at least 7 days)")

        except Exception as e:
            st.error(f"Error generating forecast: {str(e)}")

    # Hourly cost pattern
    st.markdown("---")
    st.markdown("#### Hourly Cost Pattern")

    try:
        hourly_pattern_query = f"""
        SELECT
            HOUR(START_TIME) AS HOUR_OF_DAY,
            AVG(CREDITS_USED) * {credit_cost} AS AVG_HOURLY_COST,
            SUM(CREDITS_USED) * {credit_cost} AS TOTAL_HOURLY_COST
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        GROUP BY HOUR_OF_DAY
        ORDER BY HOUR_OF_DAY
        """
        hourly_pattern = session.sql(hourly_pattern_query).to_pandas()

        if not hourly_pattern.empty:
            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=hourly_pattern['HOUR_OF_DAY'],
                y=hourly_pattern['AVG_HOURLY_COST'],
                marker_color='lightblue',
                name='Average Cost',
                hovertemplate='Hour: %{x}<br>Avg Cost: $%{y:,.2f}<extra></extra>'
            ))

            fig.update_layout(
                title="Average Cost by Hour of Day",
                xaxis_title="Hour of Day",
                yaxis_title="Average Cost ($)",
                height=300
            )

            st.plotly_chart(fig, use_container_width=True)

            # Identify peak hours
            peak_hour = hourly_pattern.loc[hourly_pattern['AVG_HOURLY_COST'].idxmax(), 'HOUR_OF_DAY']
            peak_cost = hourly_pattern['AVG_HOURLY_COST'].max()

            st.caption(f"Peak hour: {int(peak_hour)}:00 with average cost of ${peak_cost:,.2f}")

        else:
            st.info("No hourly pattern data available")

    except Exception as e:
        st.error(f"Error loading hourly pattern: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 2: Cost Attribution
# ----------------------------------------------------------------------------

with tab2:
    st.markdown("### 🏷️ Cost Attribution")

    attribution_tab1, attribution_tab2, attribution_tab3, attribution_tab4 = st.tabs([
        "Warehouse", "Service Type", "User", "Database"
    ])

    with attribution_tab1:
        st.markdown("#### Cost by Warehouse")

        try:
            warehouse_cost_query = f"""
            SELECT
                WAREHOUSE_NAME,
                SUM(CREDITS_USED) AS TOTAL_CREDITS,
                SUM(CREDITS_USED) * {credit_cost} AS TOTAL_COST,
                AVG(CREDITS_USED) AS AVG_CREDITS
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            GROUP BY WAREHOUSE_NAME
            ORDER BY TOTAL_COST DESC
            """
            warehouse_costs = session.sql(warehouse_cost_query).to_pandas()

            if not warehouse_costs.empty:
                total_wh_cost = warehouse_costs['TOTAL_COST'].sum()
                warehouse_costs['COST_PCT'] = (warehouse_costs['TOTAL_COST'] / total_wh_cost * 100).round(1)

                # Display table
                display_df = warehouse_costs[['WAREHOUSE_NAME', 'TOTAL_CREDITS', 'TOTAL_COST', 'COST_PCT']].copy()
                display_df.columns = ['Warehouse', 'Credits', 'Cost ($)', '% of Total']

                st.dataframe(
                    display_df.style.format({
                        'Credits': '{:,.2f}',
                        'Cost ($)': '${:,.2f}',
                        '% of Total': '{:.1f}%'
                    }).background_gradient(subset=['Cost ($)'], cmap='YlOrRd'),
                    use_container_width=True,
                    height=300
                )

                # Pareto chart (80/20 analysis)
                warehouse_costs_sorted = warehouse_costs.sort_values('TOTAL_COST', ascending=False).copy()
                warehouse_costs_sorted['CUMULATIVE_PCT'] = (warehouse_costs_sorted['TOTAL_COST'].cumsum() / total_wh_cost * 100)

                fig = go.Figure()

                fig.add_trace(go.Bar(
                    x=warehouse_costs_sorted['WAREHOUSE_NAME'],
                    y=warehouse_costs_sorted['TOTAL_COST'],
                    name='Cost',
                    marker_color='steelblue',
                    yaxis='y',
                    hovertemplate='%{x}<br>Cost: $%{y:,.2f}<extra></extra>'
                ))

                fig.add_trace(go.Scatter(
                    x=warehouse_costs_sorted['WAREHOUSE_NAME'],
                    y=warehouse_costs_sorted['CUMULATIVE_PCT'],
                    name='Cumulative %',
                    marker_color='red',
                    yaxis='y2',
                    mode='lines+markers',
                    hovertemplate='%{x}<br>Cumulative: %{y:.1f}%<extra></extra>'
                ))

                fig.update_layout(
                    title="Warehouse Cost Pareto Chart",
                    xaxis_title="Warehouse",
                    yaxis=dict(title="Cost ($)"),
                    yaxis2=dict(title="Cumulative %", overlaying='y', side='right', range=[0, 100]),
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)

                # 80/20 analysis
                top_80_count = len(warehouse_costs_sorted[warehouse_costs_sorted['CUMULATIVE_PCT'] <= 80])
                st.caption(f"Top {top_80_count} warehouses account for 80% of costs")

            else:
                st.info("No warehouse cost data available")

        except Exception as e:
            st.error(f"Error loading warehouse costs: {str(e)}")

    with attribution_tab2:
        st.markdown("#### Cost by Service Type")

        try:
            service_cost_query = f"""
            SELECT
                SERVICE_TYPE,
                SUM(CREDITS_USED) AS TOTAL_CREDITS,
                SUM(CREDITS_USED) * {credit_cost} AS TOTAL_COST
            FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            GROUP BY SERVICE_TYPE
            ORDER BY TOTAL_COST DESC
            """
            service_costs = session.sql(service_cost_query).to_pandas()

            if not service_costs.empty:
                col1, col2 = st.columns(2)

                with col1:
                    # Display table
                    display_df = service_costs.copy()
                    display_df.columns = ['Service Type', 'Credits', 'Cost ($)']

                    st.dataframe(
                        display_df.style.format({
                            'Credits': '{:,.2f}',
                            'Cost ($)': '${:,.2f}'
                        }),
                        use_container_width=True
                    )

                with col2:
                    # Pie chart
                    fig = px.pie(
                        service_costs,
                        values='TOTAL_COST',
                        names='SERVICE_TYPE',
                        title='Cost Distribution by Service Type'
                    )
                    fig.update_traces(
                        textposition='inside',
                        textinfo='percent+label'
                    )
                    st.plotly_chart(fig, use_container_width=True)

            else:
                st.info("No service type cost data available")

        except Exception as e:
            st.error(f"Error loading service costs: {str(e)}")

    with attribution_tab3:
        st.markdown("#### Cost by User")

        try:
            user_cost_query = f"""
            SELECT
                USER_NAME,
                COUNT(DISTINCT QUERY_ID) AS QUERY_COUNT,
                SUM(CREDITS_USED_CLOUD_SERVICES) AS TOTAL_CREDITS,
                SUM(CREDITS_USED_CLOUD_SERVICES) * {credit_cost} AS TOTAL_COST,
                AVG(EXECUTION_TIME) / 1000 AS AVG_EXEC_TIME_SEC
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            AND CREDITS_USED_CLOUD_SERVICES > 0
            GROUP BY USER_NAME
            ORDER BY TOTAL_COST DESC
            LIMIT 50
            """
            user_costs = session.sql(user_cost_query).to_pandas()

            if not user_costs.empty:
                # Display table
                display_df = user_costs.copy()
                display_df.columns = ['User', 'Queries', 'Credits', 'Cost ($)', 'Avg Exec Time (sec)']

                st.dataframe(
                    display_df.style.format({
                        'Queries': '{:,}',
                        'Credits': '{:,.2f}',
                        'Cost ($)': '${:,.2f}',
                        'Avg Exec Time (sec)': '{:.2f}'
                    }).background_gradient(subset=['Cost ($)'], cmap='RdYlGn_r'),
                    use_container_width=True,
                    height=400
                )

                # Top 10 users chart
                top_10_users = user_costs.head(10)

                fig = go.Figure()

                fig.add_trace(go.Bar(
                    y=top_10_users['USER_NAME'],
                    x=top_10_users['TOTAL_COST'],
                    orientation='h',
                    marker_color='lightcoral',
                    text=top_10_users['TOTAL_COST'].round(2),
                    textposition='outside',
                    hovertemplate='<b>%{y}</b><br>Cost: $%{x:,.2f}<br>Queries: %{customdata:,}<extra></extra>',
                    customdata=top_10_users['QUERY_COUNT']
                ))

                fig.update_layout(
                    title="Top 10 Users by Cost",
                    xaxis_title="Cost ($)",
                    yaxis_title="User",
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)

            else:
                st.info("No user cost data available")

        except Exception as e:
            st.error(f"Error loading user costs: {str(e)}")

    with attribution_tab4:
        st.markdown("#### Cost by Database (Storage)")

        try:
            storage_metrics = queries.get_storage_metrics(time_period)

            if not storage_metrics.empty:
                storage_metrics['SIZE_TB'] = storage_metrics['TOTAL_BYTES'] / (1024**4)
                storage_metrics['MONTHLY_COST'] = storage_metrics['SIZE_TB'] * storage_cost

                # Display table
                display_df = storage_metrics[['DATABASE_NAME', 'SIZE_TB', 'MONTHLY_COST']].copy()
                display_df.columns = ['Database', 'Storage (TB)', 'Monthly Cost ($)']

                st.dataframe(
                    display_df.style.format({
                        'Storage (TB)': '{:.2f}',
                        'Monthly Cost ($)': '${:,.2f}'
                    }).background_gradient(subset=['Monthly Cost ($)'], cmap='YlOrRd'),
                    use_container_width=True,
                    height=300
                )

                # Treemap
                top_15_db = storage_metrics.head(15)

                fig = px.treemap(
                    top_15_db,
                    path=['DATABASE_NAME'],
                    values='MONTHLY_COST',
                    title='Database Storage Cost Treemap',
                    color='SIZE_TB',
                    color_continuous_scale='Reds'
                )

                st.plotly_chart(fig, use_container_width=True)

            else:
                st.info("No database storage data available")

        except Exception as e:
            st.error(f"Error loading database costs: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 3: Anomaly Detection
# ----------------------------------------------------------------------------

with tab3:
    st.markdown("### 🚨 Cost Anomaly Detection")

    try:
        # Detect cost anomalies using z-score
        anomaly_query = f"""
        WITH daily_costs AS (
            SELECT
                DATE_TRUNC('DAY', START_TIME) AS COST_DATE,
                SUM(CREDITS_USED) * {credit_cost} AS DAILY_COST
            FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            GROUP BY COST_DATE
        ),
        cost_stats AS (
            SELECT
                AVG(DAILY_COST) AS AVG_COST,
                STDDEV(DAILY_COST) AS STDDEV_COST
            FROM daily_costs
        )
        SELECT
            c.COST_DATE,
            c.DAILY_COST,
            s.AVG_COST,
            s.STDDEV_COST,
            ABS((c.DAILY_COST - s.AVG_COST) / NULLIF(s.STDDEV_COST, 0)) AS Z_SCORE,
            CASE
                WHEN ABS((c.DAILY_COST - s.AVG_COST) / NULLIF(s.STDDEV_COST, 0)) > 3 THEN 'CRITICAL'
                WHEN ABS((c.DAILY_COST - s.AVG_COST) / NULLIF(s.STDDEV_COST, 0)) > 2 THEN 'WARNING'
                ELSE 'NORMAL'
            END AS SEVERITY
        FROM daily_costs c
        CROSS JOIN cost_stats s
        ORDER BY c.COST_DATE DESC
        """
        anomalies = session.sql(anomaly_query).to_pandas()

        if not anomalies.empty:
            anomalies['COST_DATE'] = pd.to_datetime(anomalies['COST_DATE'])

            # Count anomalies
            critical_count = len(anomalies[anomalies['SEVERITY'] == 'CRITICAL'])
            warning_count = len(anomalies[anomalies['SEVERITY'] == 'WARNING'])

            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Critical Anomalies", critical_count, help="Z-score > 3")
            with col2:
                st.metric("Warning Anomalies", warning_count, help="Z-score > 2")
            with col3:
                normal_count = len(anomalies[anomalies['SEVERITY'] == 'NORMAL'])
                st.metric("Normal Days", normal_count)

            # Anomaly visualization
            st.markdown("#### Cost Anomaly Timeline")

            fig = go.Figure()

            # Add all data points
            for severity in ['NORMAL', 'WARNING', 'CRITICAL']:
                severity_data = anomalies[anomalies['SEVERITY'] == severity]

                if not severity_data.empty:
                    colors = {'NORMAL': 'green', 'WARNING': 'orange', 'CRITICAL': 'red'}

                    fig.add_trace(go.Scatter(
                        x=severity_data['COST_DATE'],
                        y=severity_data['DAILY_COST'],
                        mode='markers',
                        name=severity,
                        marker=dict(
                            color=colors[severity],
                            size=10 if severity != 'NORMAL' else 6,
                            symbol='diamond' if severity == 'CRITICAL' else 'circle'
                        ),
                        hovertemplate='<b>%{x|%Y-%m-%d}</b><br>Cost: $%{y:,.2f}<br>Z-Score: %{customdata:.2f}<extra></extra>',
                        customdata=severity_data['Z_SCORE']
                    ))

            # Add average line
            avg_cost = anomalies['AVG_COST'].iloc[0]
            fig.add_hline(y=avg_cost, line_dash="dash", line_color="blue", annotation_text=f"Average: ${avg_cost:,.2f}")

            fig.update_layout(
                title="Daily Cost with Anomaly Detection",
                xaxis_title="Date",
                yaxis_title="Cost ($)",
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

            # List anomalies
            if critical_count > 0 or warning_count > 0:
                st.markdown("#### Detected Anomalies")

                anomaly_list = anomalies[anomalies['SEVERITY'].isin(['CRITICAL', 'WARNING'])].copy()
                anomaly_list = anomaly_list.sort_values('COST_DATE', ascending=False)

                display_df = anomaly_list[['COST_DATE', 'DAILY_COST', 'AVG_COST', 'Z_SCORE', 'SEVERITY']].copy()
                display_df.columns = ['Date', 'Cost ($)', 'Average ($)', 'Z-Score', 'Severity']

                st.dataframe(
                    display_df.style.format({
                        'Date': lambda x: x.strftime('%Y-%m-%d'),
                        'Cost ($)': '${:,.2f}',
                        'Average ($)': '${:,.2f}',
                        'Z-Score': '{:.2f}'
                    }),
                    use_container_width=True
                )

                # Investigate anomalies
                if critical_count > 0:
                    latest_critical = anomaly_list[anomaly_list['SEVERITY'] == 'CRITICAL'].iloc[0]

                    create_alert_badge(
                        f"🚨 Critical anomaly on {latest_critical['COST_DATE'].strftime('%Y-%m-%d')}: ${latest_critical['DAILY_COST']:,.2f} (Z-score: {latest_critical['Z_SCORE']:.2f})",
                        "warning"
                    )

                    st.markdown("**Recommended Actions:**")
                    st.markdown("""
                    1. Review warehouse usage on the anomalous date
                    2. Check for unusual query patterns or data loads
                    3. Verify no unauthorized access or runaway queries
                    4. Investigate any scheduled jobs that may have run unexpectedly
                    """)
            else:
                create_alert_badge("✅ No anomalies detected in the selected time period", "success")

        else:
            st.info("Insufficient data for anomaly detection")

    except Exception as e:
        st.error(f"Error detecting anomalies: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 4: Savings Opportunities
# ----------------------------------------------------------------------------

with tab4:
    st.markdown("### 💡 Savings Opportunities")

    col1, col2 = st.columns([2, 1])

    with col1:
        try:
            savings_opportunities = []

            # 1. Idle warehouse savings
            idle_wh_query = f"""
            WITH warehouse_load AS (
                SELECT
                    WAREHOUSE_NAME,
                    AVG(AVG_RUNNING) AS AVG_RUNNING,
                    AVG(AVG_QUEUED_LOAD) AS AVG_QUEUED
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
                WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
                GROUP BY WAREHOUSE_NAME
            ),
            warehouse_credits AS (
                SELECT
                    WAREHOUSE_NAME,
                    SUM(CREDITS_USED) AS TOTAL_CREDITS
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
                GROUP BY WAREHOUSE_NAME
            )
            SELECT
                c.WAREHOUSE_NAME,
                c.TOTAL_CREDITS,
                c.TOTAL_CREDITS * {credit_cost} AS TOTAL_COST,
                l.AVG_RUNNING,
                l.AVG_QUEUED
            FROM warehouse_credits c
            JOIN warehouse_load l ON c.WAREHOUSE_NAME = l.WAREHOUSE_NAME
            WHERE l.AVG_RUNNING < 1 AND c.TOTAL_CREDITS > 1
            ORDER BY TOTAL_COST DESC
            """
            idle_warehouses = session.sql(idle_wh_query).to_pandas()

            if not idle_warehouses.empty:
                idle_savings = idle_warehouses['TOTAL_COST'].sum() * 0.7  # Assume 70% savings

                savings_opportunities.append({
                    'category': 'Idle Warehouses',
                    'description': f"{len(idle_warehouses)} warehouse(s) with low utilization",
                    'potential_savings': idle_savings,
                    'priority': 'HIGH',
                    'action': 'Downsize or suspend idle warehouses. Consider auto-suspend settings.'
                })

            # 2. Storage optimization
            table_insights = queries.get_table_storage_insights()
            if not table_insights.empty:
                storage_savings_tb = table_insights['TOTAL_BYTES'].sum() / (1024**4)
                storage_savings = storage_savings_tb * storage_cost

                savings_opportunities.append({
                    'category': 'Storage Optimization',
                    'description': f"{len(table_insights)} unused/stale tables identified",
                    'potential_savings': storage_savings,
                    'priority': 'MEDIUM',
                    'action': 'Drop unused tables or move to cheaper storage tiers.'
                })

            # 3. Long-running queries
            long_queries_query = f"""
            SELECT
                COUNT(*) AS QUERY_COUNT,
                SUM(CREDITS_USED_CLOUD_SERVICES) * {credit_cost} AS TOTAL_COST
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
            AND EXECUTION_TIME > 300000  -- > 5 minutes
            AND CREDITS_USED_CLOUD_SERVICES > 0
            """
            long_queries = session.sql(long_queries_query).to_pandas()

            if not long_queries.empty and long_queries['QUERY_COUNT'].iloc[0] > 0:
                query_count = long_queries['QUERY_COUNT'].iloc[0]
                query_cost = long_queries['TOTAL_COST'].iloc[0]
                optimization_savings = query_cost * 0.3  # Assume 30% savings from optimization

                savings_opportunities.append({
                    'category': 'Query Optimization',
                    'description': f"{int(query_count)} long-running queries (>5 min)",
                    'potential_savings': optimization_savings,
                    'priority': 'HIGH',
                    'action': 'Optimize query performance through clustering, materialized views, or query rewrites.'
                })

            # 4. Time Travel optimization
            tt_query = """
            SELECT
                SUM(TIME_TRAVEL_BYTES) / POWER(1024, 4) AS TT_TB
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
            WHERE TIME_TRAVEL_BYTES > 1073741824  -- > 1 GB
            AND DELETED IS NULL
            """
            tt_data = session.sql(tt_query).to_pandas()

            if not tt_data.empty and tt_data['TT_TB'].iloc[0] > 1:
                tt_savings = tt_data['TT_TB'].iloc[0] * storage_cost * 0.5

                savings_opportunities.append({
                    'category': 'Time Travel Retention',
                    'description': f"{tt_data['TT_TB'].iloc[0]:.2f} TB in Time Travel storage",
                    'potential_savings': tt_savings,
                    'priority': 'MEDIUM',
                    'action': 'Reduce retention period for non-critical tables from default 1 day to minimum required.'
                })

            # 5. Warehouse scaling
            oversized_wh_query = f"""
            WITH warehouse_load AS (
                SELECT
                    WAREHOUSE_NAME,
                    AVG(AVG_QUEUED_LOAD) AS AVG_QUEUED
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
                WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
                GROUP BY WAREHOUSE_NAME
            ),
            warehouse_credits AS (
                SELECT
                    WAREHOUSE_NAME,
                    SUM(CREDITS_USED) AS TOTAL_CREDITS
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
                GROUP BY WAREHOUSE_NAME
            )
            SELECT
                c.WAREHOUSE_NAME,
                c.TOTAL_CREDITS * {credit_cost} AS TOTAL_COST,
                l.AVG_QUEUED
            FROM warehouse_credits c
            JOIN warehouse_load l ON c.WAREHOUSE_NAME = l.WAREHOUSE_NAME
            WHERE l.AVG_QUEUED < 0.1
            AND c.TOTAL_CREDITS > 10
            ORDER BY TOTAL_COST DESC
            """
            oversized_wh = session.sql(oversized_wh_query).to_pandas()

            if not oversized_wh.empty:
                scaling_savings = oversized_wh['TOTAL_COST'].sum() * 0.25  # Assume 25% savings from right-sizing

                savings_opportunities.append({
                    'category': 'Warehouse Right-Sizing',
                    'description': f"{len(oversized_wh)} warehouse(s) may be oversized",
                    'potential_savings': scaling_savings,
                    'priority': 'MEDIUM',
                    'action': 'Monitor queue depth and consider downsizing warehouses with consistently low load.'
                })

            # Display savings opportunities
            if savings_opportunities:
                # Sort by potential savings
                savings_opportunities.sort(key=lambda x: x['potential_savings'], reverse=True)

                total_savings = sum(opp['potential_savings'] for opp in savings_opportunities)

                st.success(f"**Total Potential Savings: ${total_savings:,.2f}/month (${total_savings * 12:,.2f}/year)**")

                for i, opp in enumerate(savings_opportunities, 1):
                    priority_colors = {
                        'HIGH': '🔴',
                        'MEDIUM': '🟡',
                        'LOW': '🟢'
                    }

                    with st.expander(
                        f"{priority_colors.get(opp['priority'], '🔵')} {opp['category']} - ${opp['potential_savings']:,.2f}/month",
                        expanded=(opp['priority'] == 'HIGH')
                    ):
                        st.markdown(f"**Description:** {opp['description']}")
                        st.markdown(f"**Priority:** {opp['priority']}")
                        st.markdown(f"**Monthly Savings:** ${opp['potential_savings']:,.2f}")
                        st.markdown(f"**Annual Savings:** ${opp['potential_savings'] * 12:,.2f}")
                        st.markdown(f"**Recommended Action:**")
                        st.info(opp['action'])
            else:
                create_alert_badge("✅ No major savings opportunities identified", "success")

        except Exception as e:
            st.error(f"Error identifying savings opportunities: {str(e)}")

    with col2:
        st.markdown("#### 🤖 AI-Powered Insights")

        try:
            if ai_insights.check_cortex_availability():
                with st.spinner("Generating AI cost insights..."):
                    context = {
                        "Total Cost": f"${total_cost:,.2f}",
                        "Daily Average": f"${daily_avg_cost:,.2f}",
                        "Time Period": f"{time_period} days",
                        "Savings Opportunities": len(savings_opportunities) if 'savings_opportunities' in locals() else 0
                    }

                    insight = ai_insights.generate_insight(
                        str(context),
                        "Analyze the cost data and provide 3 specific, actionable recommendations to reduce Snowflake costs."
                    )

                    st.markdown(f'<div class="insight-card">{insight}</div>', unsafe_allow_html=True)
            else:
                st.warning("AI insights require Snowflake Cortex Complete access")

        except Exception as e:
            st.warning(f"AI insights temporarily unavailable: {str(e)}")

        # Quick cost reduction tips
        st.markdown("---")
        st.markdown("#### 💡 Quick Tips")

        st.markdown("""
        **Immediate Actions:**
        - Set AUTO_SUSPEND to 60 seconds
        - Enable AUTO_RESUME
        - Use smallest warehouse that meets SLA
        - Cluster large, filtered tables
        - Reduce Time Travel for dev/test
        - Drop unused tables and databases
        - Monitor and kill runaway queries
        """)

# ----------------------------------------------------------------------------
# TAB 5: Budget Tracking
# ----------------------------------------------------------------------------

with tab5:
    st.markdown("### 📊 Budget Tracking & Alerts")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Set Budget")

        monthly_budget = st.number_input(
            "Monthly Budget ($)",
            min_value=0.0,
            value=st.session_state.get('monthly_budget', 10000.0),
            step=100.0,
            help="Set your monthly Snowflake budget"
        )

        st.session_state.monthly_budget = monthly_budget

        # Calculate current month cost
        current_month_query = f"""
        SELECT
            SUM(CREDITS_USED) * {credit_cost} AS MONTH_COST
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
        WHERE START_TIME >= DATE_TRUNC('MONTH', CURRENT_DATE())
        """
        current_month = session.sql(current_month_query).to_pandas()

        if not current_month.empty:
            current_month_cost = current_month['MONTH_COST'].iloc[0]
            budget_used_pct = (current_month_cost / monthly_budget * 100) if monthly_budget > 0 else 0

            # Budget gauge
            fig = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=current_month_cost,
                title={'text': "Current Month Spend"},
                delta={'reference': monthly_budget, 'valueformat': '$,.2f'},
                gauge={
                    'axis': {'range': [None, monthly_budget * 1.2]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, monthly_budget * 0.7], 'color': "lightgreen"},
                        {'range': [monthly_budget * 0.7, monthly_budget], 'color': "yellow"},
                        {'range': [monthly_budget, monthly_budget * 1.2], 'color': "red"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': monthly_budget
                    }
                }
            ))

            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

            # Budget status
            if budget_used_pct >= 100:
                create_alert_badge(f"🚨 Budget exceeded by ${current_month_cost - monthly_budget:,.2f}", "warning")
            elif budget_used_pct >= 80:
                create_alert_badge(f"⚠️ {budget_used_pct:.1f}% of budget used", "warning")
            else:
                create_alert_badge(f"✅ {budget_used_pct:.1f}% of budget used - On track", "success")

            # Projected end-of-month cost
            days_in_month = pd.Timestamp.now().days_in_month
            current_day = pd.Timestamp.now().day
            daily_avg = current_month_cost / current_day if current_day > 0 else 0
            projected_month_cost = daily_avg * days_in_month

            st.metric(
                "Projected Month-End Cost",
                f"${projected_month_cost:,.2f}",
                delta=f"${projected_month_cost - monthly_budget:,.2f}" if monthly_budget > 0 else None,
                delta_color="inverse"
            )

            if projected_month_cost > monthly_budget:
                st.warning(f"⚠️ Projected to exceed budget by ${projected_month_cost - monthly_budget:,.2f}")

    with col2:
        st.markdown("#### Budget Alert Configuration")

        alert_threshold = st.slider(
            "Alert Threshold (%)",
            min_value=50,
            max_value=100,
            value=80,
            step=5,
            help="Receive alert when budget usage exceeds this percentage"
        )

        st.session_state.budget_alert_threshold = alert_threshold

        if 'current_month_cost' in locals() and 'budget_used_pct' in locals():
            if budget_used_pct >= alert_threshold:
                st.error(f"🚨 ALERT: Budget usage ({budget_used_pct:.1f}%) exceeds threshold ({alert_threshold}%)")

                st.markdown("**Recommended Actions:**")
                st.markdown("""
                1. Review recent cost spikes in Cost Trends tab
                2. Check Cost Attribution for top spenders
                3. Implement savings opportunities identified
                4. Consider temporary warehouse suspension
                5. Set up resource monitors for proactive control
                """)
            else:
                st.success(f"✅ Budget usage ({budget_used_pct:.1f}%) below threshold ({alert_threshold}%)")

        st.markdown("---")
        st.markdown("#### Resource Monitor Setup")

        st.markdown("""
        **Create a Resource Monitor:**

        ```sql
        -- Create resource monitor
        CREATE RESOURCE MONITOR monthly_budget
        WITH CREDIT_QUOTA = 1000  -- Adjust based on budget
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
        TRIGGERS
            ON 80 PERCENT DO NOTIFY
            ON 100 PERCENT DO SUSPEND
            ON 110 PERCENT DO SUSPEND_IMMEDIATE;

        -- Assign to warehouses
        ALTER WAREHOUSE <warehouse_name>
        SET RESOURCE_MONITOR = monthly_budget;
        ```
        """)

    # Historical budget tracking
    st.markdown("---")
    st.markdown("#### Monthly Cost History")

    try:
        monthly_cost_query = f"""
        SELECT
            DATE_TRUNC('MONTH', START_TIME) AS COST_MONTH,
            SUM(CREDITS_USED) * {credit_cost} AS MONTHLY_COST
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
        WHERE START_TIME >= DATEADD(MONTH, -12, CURRENT_DATE())
        GROUP BY COST_MONTH
        ORDER BY COST_MONTH
        """
        monthly_costs = session.sql(monthly_cost_query).to_pandas()

        if not monthly_costs.empty:
            monthly_costs['COST_MONTH'] = pd.to_datetime(monthly_costs['COST_MONTH'])
            monthly_costs['MONTH_NAME'] = monthly_costs['COST_MONTH'].dt.strftime('%Y-%m')

            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=monthly_costs['MONTH_NAME'],
                y=monthly_costs['MONTHLY_COST'],
                marker_color='steelblue',
                text=monthly_costs['MONTHLY_COST'].round(2),
                textposition='outside',
                hovertemplate='%{x}<br>Cost: $%{y:,.2f}<extra></extra>'
            ))

            # Add budget line
            fig.add_hline(
                y=monthly_budget,
                line_dash="dash",
                line_color="red",
                annotation_text=f"Budget: ${monthly_budget:,.2f}"
            )

            fig.update_layout(
                title="Monthly Cost History (Last 12 Months)",
                xaxis_title="Month",
                yaxis_title="Cost ($)",
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

            # Monthly statistics
            avg_monthly = monthly_costs['MONTHLY_COST'].mean()
            max_month = monthly_costs.loc[monthly_costs['MONTHLY_COST'].idxmax()]

            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Average Monthly Cost", f"${avg_monthly:,.2f}")
            with col2:
                st.metric("Highest Month", f"${max_month['MONTHLY_COST']:,.2f}")
                st.caption(max_month['MONTH_NAME'])
            with col3:
                months_over_budget = len(monthly_costs[monthly_costs['MONTHLY_COST'] > monthly_budget])
                st.metric("Months Over Budget", months_over_budget)

        else:
            st.info("No monthly cost history available")

    except Exception as e:
        st.error(f"Error loading monthly cost history: {str(e)}")

# Footer
st.markdown("---")
st.caption(f"📅 Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | ⏱️ Time period: {time_period} days | 💵 Credit cost: ${credit_cost}/credit | 💾 Storage: ${storage_cost}/TB/month")
