"""
Snowflake Holistic Observability Dashboard - Data Transfer Page
================================================================
Monitor cross-cloud and cross-region data transfers and associated costs
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
    page_title="Data Transfer - Snowflake Observability",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom styling
apply_custom_css()

# Initialize session state and render settings
initialize_session_state()
render_settings_sidebar()

# Page header
render_page_header("🔄 Data Transfer Monitoring", "Track cross-cloud and cross-region data transfers")

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

# Data transfer cost (per TB)
# Note: Actual costs vary by cloud provider and region
transfer_cost_per_tb = st.sidebar.number_input(
    "Data Transfer Cost ($/TB)",
    min_value=0.0,
    value=20.0,
    step=5.0,
    help="Typical range: $0-$100/TB depending on provider and direction"
)

# ============================================================================
# TRANSFER OVERVIEW
# ============================================================================

st.markdown("---")
st.subheader("📊 Data Transfer Overview")

with st.spinner("Loading data transfer metrics..."):
    try:
        # Get data transfer history
        transfer_query = f"""
        SELECT
            SOURCE_CLOUD,
            SOURCE_REGION,
            TARGET_CLOUD,
            TARGET_REGION,
            TRANSFER_TYPE,
            SUM(BYTES_TRANSFERRED) AS TOTAL_BYTES,
            COUNT(DISTINCT QUERY_ID) AS TRANSFER_COUNT
        FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        GROUP BY SOURCE_CLOUD, SOURCE_REGION, TARGET_CLOUD, TARGET_REGION, TRANSFER_TYPE
        ORDER BY TOTAL_BYTES DESC
        """

        try:
            transfers = session.sql(transfer_query).to_pandas()
        except:
            transfers = pd.DataFrame()

        if not transfers.empty:
            # Calculate costs
            transfers['TRANSFER_TB'] = transfers['TOTAL_BYTES'] / (1024**4)
            transfers['ESTIMATED_COST'] = transfers['TRANSFER_TB'] * transfer_cost_per_tb

            # Summary metrics
            total_bytes = transfers['TOTAL_BYTES'].sum()
            total_tb = total_bytes / (1024**4)
            total_cost = transfers['ESTIMATED_COST'].sum()
            transfer_count = transfers['TRANSFER_COUNT'].sum()

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    "Total Data Transferred",
                    format_bytes(total_bytes),
                    help=f"{total_tb:.2f} TB"
                )

            with col2:
                st.metric(
                    "Transfer Count",
                    format_number(int(transfer_count)),
                    help="Number of data transfer operations"
                )

            with col3:
                avg_transfer_size = total_bytes / max(transfer_count, 1)
                st.metric(
                    "Avg Transfer Size",
                    format_bytes(avg_transfer_size)
                )

            with col4:
                st.metric(
                    "Estimated Cost",
                    f"${total_cost:,.2f}",
                    help=f"Based on ${transfer_cost_per_tb}/TB"
                )

            # Transfer type breakdown
            st.markdown("---")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Transfer by Type")

                type_breakdown = transfers.groupby('TRANSFER_TYPE').agg({
                    'TOTAL_BYTES': 'sum',
                    'ESTIMATED_COST': 'sum'
                }).reset_index()

                fig = px.pie(
                    type_breakdown,
                    values='TOTAL_BYTES',
                    names='TRANSFER_TYPE',
                    title='Data Volume by Transfer Type',
                    hole=0.4
                )

                fig.update_traces(
                    textposition='inside',
                    textinfo='percent+label'
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("#### Cost by Transfer Type")

                fig = px.bar(
                    type_breakdown,
                    x='TRANSFER_TYPE',
                    y='ESTIMATED_COST',
                    title='Estimated Cost by Type',
                    labels={'ESTIMATED_COST': 'Cost ($)', 'TRANSFER_TYPE': 'Transfer Type'}
                )

                fig.update_traces(marker_color='steelblue')
                st.plotly_chart(fig, use_container_width=True)

        else:
            st.info("No data transfer activity found in the selected period")

            st.markdown("""
            **About Data Transfer Costs:**

            Data transfer costs occur when:
            - Moving data between different cloud providers (AWS ↔ Azure ↔ GCP)
            - Moving data between different regions within the same cloud
            - Exporting data out of Snowflake (unloading to external stage)
            - Replicating data for database/share replication

            **Cost Optimization Tips:**
            - Keep data and compute in the same region when possible
            - Use Snowflake's data sharing instead of physical data copies
            - Consolidate data transfers to reduce frequency
            - Monitor cross-region/cross-cloud queries
            """)

    except Exception as e:
        st.error(f"Error loading data transfer overview: {str(e)}")

# ============================================================================
# TRANSFER TABS
# ============================================================================

tab1, tab2, tab3 = st.tabs([
    "🗺️ Transfer Flows",
    "📈 Transfer Trends",
    "💡 Optimization"
])

# ----------------------------------------------------------------------------
# TAB 1: Transfer Flows
# ----------------------------------------------------------------------------

with tab1:
    st.markdown("### 🗺️ Data Transfer Flows")

    try:
        if not transfers.empty:
            # Transfer flow table
            st.markdown("#### Transfer Details")

            # Create readable transfer paths
            transfers['SOURCE'] = transfers['SOURCE_CLOUD'].fillna('N/A') + ' (' + transfers['SOURCE_REGION'].fillna('N/A') + ')'
            transfers['TARGET'] = transfers['TARGET_CLOUD'].fillna('N/A') + ' (' + transfers['TARGET_REGION'].fillna('N/A') + ')'
            transfers['TRANSFER_PATH'] = transfers['SOURCE'] + ' → ' + transfers['TARGET']

            display_df = transfers[[
                'TRANSFER_PATH', 'TRANSFER_TYPE', 'TRANSFER_COUNT',
                'TOTAL_BYTES', 'TRANSFER_TB', 'ESTIMATED_COST'
            ]].copy()

            display_df.columns = [
                'Transfer Path', 'Type', 'Count',
                'Bytes', 'TB', 'Estimated Cost ($)'
            ]

            st.dataframe(
                display_df.style.format({
                    'Count': '{:,}',
                    'Bytes': lambda x: format_bytes(x),
                    'TB': '{:.4f}',
                    'Estimated Cost ($)': '${:,.2f}'
                }).background_gradient(subset=['Estimated Cost ($)'], cmap='YlOrRd'),
                use_container_width=True,
                height=400
            )

            # Cloud-to-cloud transfers
            st.markdown("---")
            st.markdown("#### Cross-Cloud Transfers")

            cross_cloud = transfers[
                (transfers['SOURCE_CLOUD'].notna()) &
                (transfers['TARGET_CLOUD'].notna()) &
                (transfers['SOURCE_CLOUD'] != transfers['TARGET_CLOUD'])
            ].copy()

            if not cross_cloud.empty:
                cloud_summary = cross_cloud.groupby(['SOURCE_CLOUD', 'TARGET_CLOUD']).agg({
                    'TOTAL_BYTES': 'sum',
                    'ESTIMATED_COST': 'sum',
                    'TRANSFER_COUNT': 'sum'
                }).reset_index()

                create_alert_badge(
                    f"⚠️ Cross-cloud transfers detected: {format_bytes(cross_cloud['TOTAL_BYTES'].sum())} (${cross_cloud['ESTIMATED_COST'].sum():,.2f})",
                    "warning"
                )

                display_df = cloud_summary.copy()
                display_df.columns = ['Source Cloud', 'Target Cloud', 'Bytes', 'Cost ($)', 'Count']

                st.dataframe(
                    display_df.style.format({
                        'Bytes': lambda x: format_bytes(x),
                        'Cost ($)': '${:,.2f}',
                        'Count': '{:,}'
                    }),
                    use_container_width=True
                )

                st.markdown("""
                **Recommendation:** Cross-cloud transfers are typically the most expensive. Consider:
                - Using Snowflake Data Sharing instead of physical transfers
                - Consolidating data in a primary cloud provider
                - Implementing a data hub architecture
                """)

            else:
                create_alert_badge("✅ No cross-cloud transfers detected", "success")

            # Cross-region transfers
            st.markdown("---")
            st.markdown("#### Cross-Region Transfers")

            cross_region = transfers[
                (transfers['SOURCE_REGION'].notna()) &
                (transfers['TARGET_REGION'].notna()) &
                (transfers['SOURCE_REGION'] != transfers['TARGET_REGION'])
            ].copy()

            if not cross_region.empty:
                region_summary = cross_region.groupby(['SOURCE_CLOUD', 'SOURCE_REGION', 'TARGET_REGION']).agg({
                    'TOTAL_BYTES': 'sum',
                    'ESTIMATED_COST': 'sum',
                    'TRANSFER_COUNT': 'sum'
                }).reset_index()

                display_df = region_summary.copy()
                display_df.columns = ['Cloud', 'Source Region', 'Target Region', 'Bytes', 'Cost ($)', 'Count']

                st.dataframe(
                    display_df.style.format({
                        'Bytes': lambda x: format_bytes(x),
                        'Cost ($)': '${:,.2f}',
                        'Count': '{:,}'
                    }),
                    use_container_width=True
                )

                # Visualize top region-to-region flows
                top_10_regions = region_summary.nlargest(10, 'TOTAL_BYTES')
                top_10_regions['FLOW'] = (
                    top_10_regions['SOURCE_REGION'] + ' → ' + top_10_regions['TARGET_REGION']
                )

                fig = px.bar(
                    top_10_regions,
                    x='FLOW',
                    y='TOTAL_BYTES',
                    title='Top 10 Cross-Region Transfer Flows',
                    labels={'TOTAL_BYTES': 'Bytes', 'FLOW': 'Transfer Flow'}
                )

                fig.update_traces(marker_color='orange')
                st.plotly_chart(fig, use_container_width=True)

            else:
                create_alert_badge("✅ No cross-region transfers detected", "success")

        else:
            st.info("No transfer flow data available")

    except Exception as e:
        st.error(f"Error loading transfer flows: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 2: Transfer Trends
# ----------------------------------------------------------------------------

with tab2:
    st.markdown("### 📈 Data Transfer Trends")

    try:
        # Daily transfer history
        daily_transfer_query = f"""
        SELECT
            DATE_TRUNC('DAY', START_TIME) AS TRANSFER_DATE,
            TRANSFER_TYPE,
            SUM(BYTES_TRANSFERRED) AS TOTAL_BYTES,
            COUNT(DISTINCT QUERY_ID) AS TRANSFER_COUNT
        FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        GROUP BY TRANSFER_DATE, TRANSFER_TYPE
        ORDER BY TRANSFER_DATE
        """

        try:
            daily_transfers = session.sql(daily_transfer_query).to_pandas()
        except:
            daily_transfers = pd.DataFrame()

        if not daily_transfers.empty:
            daily_transfers['TRANSFER_DATE'] = pd.to_datetime(daily_transfers['TRANSFER_DATE'])
            daily_transfers['TRANSFER_TB'] = daily_transfers['TOTAL_BYTES'] / (1024**4)
            daily_transfers['COST'] = daily_transfers['TRANSFER_TB'] * transfer_cost_per_tb

            # Overall daily trend
            st.markdown("#### Daily Transfer Volume")

            daily_summary = daily_transfers.groupby('TRANSFER_DATE').agg({
                'TOTAL_BYTES': 'sum',
                'TRANSFER_COUNT': 'sum',
                'COST': 'sum'
            }).reset_index()

            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=daily_summary['TRANSFER_DATE'],
                y=daily_summary['TOTAL_BYTES'] / (1024**3),  # GB
                name='Data (GB)',
                marker_color='lightblue',
                yaxis='y'
            ))

            fig.add_trace(go.Scatter(
                x=daily_summary['TRANSFER_DATE'],
                y=daily_summary['COST'],
                name='Cost ($)',
                marker_color='red',
                yaxis='y2',
                mode='lines+markers'
            ))

            fig.update_layout(
                title="Daily Data Transfer Volume and Cost",
                xaxis_title="Date",
                yaxis=dict(title="Data Volume (GB)"),
                yaxis2=dict(title="Cost ($)", overlaying='y', side='right'),
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

            # Trend by type
            st.markdown("---")
            st.markdown("#### Transfer Trends by Type")

            # Pivot data for stacked area chart
            pivot_transfers = daily_transfers.pivot_table(
                index='TRANSFER_DATE',
                columns='TRANSFER_TYPE',
                values='TOTAL_BYTES',
                fill_value=0
            ).reset_index()

            # Melt for plotting
            melted = pivot_transfers.melt(
                id_vars=['TRANSFER_DATE'],
                var_name='TRANSFER_TYPE',
                value_name='BYTES'
            )
            melted['GB'] = melted['BYTES'] / (1024**3)

            fig = px.area(
                melted,
                x='TRANSFER_DATE',
                y='GB',
                color='TRANSFER_TYPE',
                title='Transfer Volume by Type Over Time',
                labels={'GB': 'Data Volume (GB)', 'TRANSFER_DATE': 'Date', 'TRANSFER_TYPE': 'Transfer Type'}
            )

            st.plotly_chart(fig, use_container_width=True)

            # Transfer statistics
            st.markdown("---")
            st.markdown("#### Transfer Statistics")

            col1, col2, col3 = st.columns(3)

            avg_daily_bytes = daily_summary['TOTAL_BYTES'].mean()
            max_daily_bytes = daily_summary['TOTAL_BYTES'].max()
            total_cost = daily_summary['COST'].sum()

            with col1:
                st.metric("Avg Daily Transfer", format_bytes(avg_daily_bytes))

            with col2:
                st.metric("Peak Daily Transfer", format_bytes(max_daily_bytes))

            with col3:
                st.metric("Total Cost", f"${total_cost:,.2f}")

            # Identify spikes
            threshold = avg_daily_bytes * 2
            spikes = daily_summary[daily_summary['TOTAL_BYTES'] > threshold]

            if not spikes.empty:
                create_alert_badge(
                    f"⚠️ {len(spikes)} day(s) with transfer volumes >2x average",
                    "warning"
                )

                st.markdown("**Days with unusually high transfers:**")
                for _, spike in spikes.iterrows():
                    st.caption(
                        f"  • {spike['TRANSFER_DATE'].strftime('%Y-%m-%d')}: "
                        f"{format_bytes(spike['TOTAL_BYTES'])} (${spike['COST']:.2f})"
                    )

        else:
            st.info("No daily transfer trend data available")

    except Exception as e:
        st.error(f"Error loading transfer trends: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 3: Optimization
# ----------------------------------------------------------------------------

with tab3:
    st.markdown("### 💡 Data Transfer Optimization")

    col1, col2 = st.columns([2, 1])

    with col1:
        try:
            recommendations = []

            # Analyze transfer patterns for recommendations
            if not transfers.empty:
                # 1. Cross-cloud transfer recommendation
                cross_cloud_total = cross_cloud['TOTAL_BYTES'].sum() if 'cross_cloud' in locals() and not cross_cloud.empty else 0
                if cross_cloud_total > 0:
                    cross_cloud_cost = (cross_cloud_total / (1024**4)) * transfer_cost_per_tb

                    recommendations.append({
                        'priority': 'HIGH',
                        'category': 'Cross-Cloud Transfers',
                        'issue': f"{format_bytes(cross_cloud_total)} transferred across clouds",
                        'impact': f"Estimated ${cross_cloud_cost:,.2f} in transfer costs",
                        'action': """
                        **Immediate Actions:**
                        - Review necessity of cross-cloud data movement
                        - Implement Snowflake Secure Data Sharing where possible
                        - Consider data replication vs real-time access needs
                        - Consolidate multi-cloud strategy if possible

                        **Long-term Strategy:**
                        - Establish primary cloud provider
                        - Use read replicas instead of full copies
                        - Implement data mesh architecture
                        """
                    })

                # 2. Cross-region recommendation
                cross_region_total = cross_region['TOTAL_BYTES'].sum() if 'cross_region' in locals() and not cross_region.empty else 0
                if cross_region_total > 0:
                    cross_region_cost = (cross_region_total / (1024**4)) * transfer_cost_per_tb * 0.5  # Lower cost than cross-cloud

                    recommendations.append({
                        'priority': 'MEDIUM',
                        'category': 'Cross-Region Transfers',
                        'issue': f"{format_bytes(cross_region_total)} transferred across regions",
                        'impact': f"Estimated ${cross_region_cost:,.2f} in transfer costs",
                        'action': """
                        **Optimization Options:**
                        - Locate warehouses in same region as data
                        - Use regional data clustering
                        - Implement query result caching
                        - Consider data locality in architecture

                        **Best Practices:**
                        - Run analytics workloads in data's home region
                        - Cache frequently accessed cross-region data
                        - Schedule batch transfers during off-peak hours
                        """
                    })

                # 3. High frequency transfers
                high_freq_transfers = transfers[transfers['TRANSFER_COUNT'] > 100]
                if not high_freq_transfers.empty:
                    recommendations.append({
                        'priority': 'MEDIUM',
                        'category': 'High Frequency Transfers',
                        'issue': f"{len(high_freq_transfers)} transfer path(s) with >100 operations",
                        'impact': "Potential for consolidation and cost reduction",
                        'action': """
                        **Consolidation Strategies:**
                        - Batch small transfers into larger, less frequent operations
                        - Use materialized views or caching for frequently accessed data
                        - Implement incremental data loading
                        - Review ETL pipeline for inefficiencies

                        **Monitoring:**
                        - Set up alerts for unusual transfer patterns
                        - Track transfer costs as percentage of total spend
                        - Regular review of data movement patterns
                        """
                    })

                # 4. Data sharing opportunities
                if total_bytes > 1024**4:  # > 1 TB
                    recommendations.append({
                        'priority': 'LOW',
                        'category': 'Data Sharing Opportunity',
                        'issue': f"{format_bytes(total_bytes)} total data transferred",
                        'impact': "Significant opportunity for Secure Data Sharing",
                        'action': """
                        **Snowflake Secure Data Sharing Benefits:**
                        - Zero-copy data sharing (no transfer costs)
                        - Real-time data access
                        - Fine-grained access control
                        - No data synchronization needed

                        **Implementation:**
                        ```sql
                        -- Create a share
                        CREATE SHARE my_share;
                        GRANT USAGE ON DATABASE my_db TO SHARE my_share;
                        GRANT SELECT ON TABLE my_db.schema.table TO SHARE my_share;

                        -- Add accounts to share
                        ALTER SHARE my_share ADD ACCOUNTS = account_id;
                        ```

                        **Use Cases:**
                        - Sharing with customers/partners
                        - Multi-account architectures
                        - Development/test data provisioning
                        """
                    })

            # Display recommendations
            if recommendations:
                st.markdown("#### Optimization Recommendations")

                for i, rec in enumerate(recommendations, 1):
                    priority_colors = {
                        'HIGH': '🔴',
                        'MEDIUM': '🟡',
                        'LOW': '🟢'
                    }

                    with st.expander(
                        f"{priority_colors.get(rec['priority'], '🔵')} {rec['category']}",
                        expanded=(rec['priority'] == 'HIGH')
                    ):
                        st.markdown(f"**Priority:** {rec['priority']}")
                        st.markdown(f"**Issue:** {rec['issue']}")
                        st.markdown(f"**Impact:** {rec['impact']}")
                        st.markdown(f"**Recommended Actions:**")
                        st.info(rec['action'])

            else:
                create_alert_badge("✅ No data transfer optimization opportunities identified", "success")

        except Exception as e:
            st.error(f"Error generating recommendations: {str(e)}")

    with col2:
        st.markdown("#### 🤖 AI Transfer Insights")

        try:
            if ai_insights.check_cortex_availability():
                with st.spinner("Generating AI insights..."):
                    if 'transfers' in locals() and not transfers.empty:
                        context = {
                            "Total Transfer Volume": format_bytes(total_bytes) if 'total_bytes' in locals() else "N/A",
                            "Total Cost": f"${total_cost:,.2f}" if 'total_cost' in locals() else "N/A",
                            "Transfer Count": int(transfer_count) if 'transfer_count' in locals() else 0,
                            "Cross-Cloud Transfers": format_bytes(cross_cloud_total) if 'cross_cloud_total' in locals() else "0 bytes"
                        }

                        insight = ai_insights.generate_insight(
                            str(context),
                            "Analyze the data transfer patterns and provide specific recommendations to reduce transfer costs."
                        )

                        st.markdown(f'<div class="insight-card">{insight}</div>', unsafe_allow_html=True)
                    else:
                        st.info("No transfer data available for AI analysis")
            else:
                st.warning("AI insights require Snowflake Cortex Complete access")

        except Exception as e:
            st.warning(f"AI insights temporarily unavailable: {str(e)}")

        st.markdown("---")
        st.markdown("#### 📚 Quick Reference")

        st.markdown("""
        **Data Transfer Cost Tiers:**

        1. **Same Region** (Cheapest)
           - Negligible or no cost
           - Optimal for performance

        2. **Cross-Region** (Moderate)
           - $0.01-$0.02/GB typically
           - Depends on cloud provider

        3. **Cross-Cloud** (Expensive)
           - $0.05-$0.15/GB typically
           - Can vary significantly

        4. **Egress** (Most Expensive)
           - $0.08-$0.20/GB typically
           - Unloading data out of Snowflake

        **Best Practices:**
        - Co-locate data and compute
        - Use Secure Data Sharing
        - Batch transfers
        - Monitor regularly
        """)

# Footer
st.markdown("---")
st.caption(f"📅 Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | ⏱️ Time period: {time_period} days | 💸 Transfer cost: ${transfer_cost_per_tb}/TB")
