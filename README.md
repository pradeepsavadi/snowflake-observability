# Snowflake Holistic Observability Dashboard

**The most comprehensive free observability dashboard for Snowflake** - Built with Streamlit in Snowflake, powered by AI insights using Cortex Complete.

## 🌟 Overview

This dashboard provides enterprise-grade observability for Snowflake environments, covering all aspects of monitoring, optimization, and governance. It incorporates features from professional tools like SELECT.dev, Snowflake Trail, Datadog, Monte Carlo, and more - all in a single, free, open-source solution.

## ✨ Key Features

### 📊 Executive Overview Dashboard
- Real-time KPIs for credits, storage, queries, and costs
- Active alerts for cost anomalies, performance issues, and optimization opportunities
- AI-powered executive summaries using Cortex Complete
- Trend analysis for costs and query volumes

### 🏢 Warehouse Analytics & Optimization
- **Comprehensive metrics**: Credits usage, query load, queue times, active days
- **Load analysis**: Running queries, queued loads, blocked queries
- **Automated recommendations**: Warehouse rightsizing (upsize/downsize/suspend)
- **AI-powered insights**: Cortex-generated optimization strategies
- **Cost attribution**: Credits per warehouse, user, and role

### 💾 Storage Analytics
- **Complete storage tracking**: Database, failsafe, stage, hybrid tables, snapshots
- **Optimization opportunities**: Unused tables, high overhead tables
- **Cost projections**: Monthly storage costs and trends
- **Granular breakdown**: Database, schema, and table-level insights
- **Time travel analysis**: Identify tables with excessive retention costs

### 🔄 Data Transfer Monitoring
- Cross-cloud and cross-region transfer tracking
- User and warehouse attribution
- Network bytes vs outbound/inbound analysis
- Transfer type distribution
- Recent activity timeline

### 👥 User & Query Analytics
- User activity patterns and session analysis
- Query performance metrics and bottleneck identification
- Role-based access patterns
- Query pattern analysis with parameterized hash tracking
- Collaboration insights (shared schema/database access)
- Cost attribution by user and role

### 🤖 AI & ML Workload Monitoring (Cortex)
- **Cortex Analyst**: Usage tracking, credit consumption, semantic model insights
- **Cortex Search**: Query volumes, service performance, latency metrics
- **Fine-Tuning**: Job tracking, model usage, credit analysis
- **AI-generated insights**: Meta-analysis of AI usage patterns

### 🔧 Data Pipeline Observability
- **Tasks**: Execution history, success/failure rates, duration analysis
- **Snowpipe**: File ingestion tracking, credit usage, throughput metrics
- **Snowpipe Streaming**: Real-time ingestion, latency monitoring, channel performance
- **Dynamic Tables**: Refresh performance, credit consumption, lag analysis

### ⚡ Performance Optimization
- **Query issues detection**: Long-running queries, spilling, compilation overhead
- **Pruning efficiency**: Table and column-level pruning analysis
- **Warehouse bottlenecks**: Queue times, overload detection
- **AI recommendations**: Cortex-powered performance suggestions

### 🔒 Security & Governance
- **Access pattern analysis**: Object access tracking, unusual activity detection
- **Login monitoring**: Success/failure rates, IP tracking, authentication methods
- **Failed login alerts**: Security incident detection
- **Audit trail**: Comprehensive activity logging

### 💰 Cost Management
- **Multi-dimensional attribution**: By warehouse, user, role, service type
- **Anomaly detection**: Statistical analysis with Z-scores
- **Savings opportunities**: Automated identification of cost reduction areas
- **Serverless cost tracking**: Separate serverless service monitoring
- **AI-powered recommendations**: Cortex-driven cost optimization strategies

### ✅ Data Quality Monitoring
- **Freshness tracking**: Identify stale tables, aging data
- **Schema change detection**: Track column modifications, additions, deletions
- **Quality scoring**: Automated data health metrics
- **Alerting**: Configurable thresholds for data SLAs

## 🆕 Latest Snowflake Features (2024-2025)

This dashboard leverages the newest Snowflake Account Usage views:

- ✅ `CORTEX_ANALYST_USAGE_HISTORY` - AI assistant tracking
- ✅ `CORTEX_SEARCH_DAILY_USAGE_HISTORY` - Vector search monitoring
- ✅ `CORTEX_FINETUNING_HISTORY` - Model customization tracking
- ✅ `SNOWPIPE_STREAMING_CHANNEL_HISTORY` - Real-time streaming metrics
- ✅ `TABLE_QUERY_PRUNING_HISTORY` - Partition pruning analysis
- ✅ `COLUMN_QUERY_PRUNING_HISTORY` - Column-level optimization
- ✅ `DYNAMIC_TABLE_REFRESH_HISTORY` - Automated materialization tracking
- ✅ `SNAPSHOT_STORAGE_USAGE` - Snapshot cost tracking
- ✅ `HYBRID_TABLE_USAGE_HISTORY` - Hybrid table monitoring
- ✅ `SNOWPARK_CONTAINER_SERVICES_HISTORY` - Container workload tracking

## 🚀 Installation & Setup

### Prerequisites
- Snowflake account with appropriate permissions
- Access to `SNOWFLAKE.ACCOUNT_USAGE` schema (ACCOUNTADMIN or granted role)
- Streamlit in Snowflake enabled

### Quick Start

1. **Deploy to Snowflake:**

   Navigate to Streamlit in Snowflake:
   - Open Snowsight UI
   - Go to "Streamlit" section
   - Create new Streamlit app
   - Copy contents of `streamlit_app.py`
   - Run the app

2. **Grant necessary permissions:**
   ```sql
   -- Grant access to account usage
   GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;

   -- Optional: Grant Cortex usage for AI insights
   GRANT USAGE ON DATABASE SNOWFLAKE TO ROLE <your_role>;
   ```

3. **Configure settings** (in sidebar):
   - Select time period (1-90 days)
   - Refresh data as needed

## 🎯 Use Cases

### For Data Engineers
- Monitor pipeline health (tasks, Snowpipes, dynamic tables)
- Optimize warehouse performance and costs
- Track data freshness and quality
- Identify schema drift and breaking changes

### For Data Analysts
- Understand query performance bottlenecks
- Track personal and team usage patterns
- Identify slow-running queries for optimization
- Monitor data availability and freshness

### For FinOps Teams
- Comprehensive cost attribution and tracking
- Identify cost anomalies and spikes
- Discover savings opportunities
- Track ROI on Snowflake investments

### For Security Teams
- Monitor access patterns and detect anomalies
- Track login attempts and failures
- Audit user activity across databases
- Ensure compliance with data governance policies

### For ML/AI Teams
- Track Cortex AI service usage and costs
- Monitor fine-tuning job performance
- Analyze Cortex Search query patterns
- Optimize AI workload efficiency

## 🤖 AI-Powered Insights

The dashboard uses **Snowflake Cortex Complete** (Mistral-Large2 model) to generate:

- **Executive summaries** of key metrics and trends
- **Warehouse optimization** recommendations
- **Cost analysis** insights and saving opportunities
- **Performance bottleneck** identification and solutions
- **Security pattern** analysis and risk assessment

All insights are generated on-demand with configurable temperature and token limits.

## 📊 Key Metrics Tracked

### Compute Metrics
- Credit consumption by warehouse, user, role
- Query counts, execution times, queue times
- Warehouse utilization and concurrency
- Spilling (local and remote)
- Compilation overhead

### Storage Metrics
- Active data, time travel, failsafe, clone storage
- Storage growth trends and projections
- Table-level storage distribution
- Stage and internal storage usage
- Hybrid table and snapshot tracking

### Pipeline Metrics
- Task execution success rates
- Snowpipe ingestion volumes and latency
- Streaming channel throughput
- Dynamic table refresh performance

### AI/ML Metrics
- Cortex function call volumes
- Fine-tuning job status and costs
- Search service query counts
- Model usage patterns

### Cost Metrics
- Total spend with trend analysis
- Cost attribution across dimensions
- Anomaly detection with statistical analysis
- Savings opportunity identification
- Budget tracking and forecasting

## 🔧 Configuration

### Time Periods
Supports 1, 7, 14, 30, 60, and 90-day lookback periods for all metrics.

### Cost Assumptions
- Credit cost: $2.50/credit (configurable in `Config` class)
- Storage cost: $23/TB/month (configurable)
- Customizable per your Snowflake pricing

### Alert Thresholds
Configure in the `Config` class:
```python
ALERT_COST_SPIKE_PCT = 50  # % increase
ALERT_QUERY_TIME_SEC = 300  # 5 minutes
ALERT_FAILURE_RATE_PCT = 10  # % of queries
ALERT_DATA_FRESHNESS_HOURS = 24
```

## 🏗️ Architecture

### Modular Design
- **SnowflakeQueries**: Centralized query functions organized by domain
- **AIInsightsGenerator**: Cortex Complete integration for AI insights
- **Visualization Components**: Reusable chart and metric components
- **Caching**: 1-hour TTL on expensive queries via `@st.cache_data`

### Data Sources
All data sourced from `SNOWFLAKE.ACCOUNT_USAGE`:
- Metering and warehouse history
- Query and task execution logs
- Storage and table metrics
- Access and login history
- Cortex usage logs

### Performance Optimizations
- Efficient SQL with CTEs and aggregations
- Query result caching (1-hour TTL)
- Limit clauses to prevent large result sets
- Parallel data loading where possible

## 🛠️ Customization

### Adding Custom Metrics
```python
@st.cache_data(ttl=Config.CACHE_TTL)
def get_custom_metric(_self, days):
    query = f"""
    SELECT ... FROM SNOWFLAKE.ACCOUNT_USAGE...
    WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE())
    """
    return _self.session.sql(query).to_pandas()
```

### Adding Custom Visualizations
```python
def create_custom_chart(data, ...):
    chart = alt.Chart(data).mark_...().encode(...)
    return chart
```

### Customizing AI Prompts
Modify the `generate_insight()` method in `AIInsightsGenerator` class.

## 📝 Best Practices

1. **Permissions**: Use a dedicated role with minimal required permissions
2. **Caching**: Leverage built-in caching for frequently accessed data
3. **Time periods**: Start with shorter periods (7-14 days) for faster loading
4. **Scheduling**: Set up automated refreshes via Snowflake tasks if needed
5. **Monitoring**: Track dashboard performance using query history

## 🔍 Troubleshooting

### Common Issues

**"Access denied on SNOWFLAKE database"**
- Grant: `GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <role>;`

**"View not found: CORTEX_ANALYST_USAGE_HISTORY"**
- Newer views may not be available on all Snowflake accounts
- The dashboard gracefully handles missing views with try/except blocks

**"Slow loading times"**
- Reduce time period selection
- Ensure warehouse size is appropriate (Medium or Large recommended)
- Check query history for long-running dashboard queries

**"AI insights not generating"**
- Ensure Cortex is enabled: `SELECT SYSTEM$CORTEX_AVAILABLE('COMPLETE');`
- Verify role has USAGE on SNOWFLAKE database

## 🔐 Security Considerations

- Never hardcode credentials
- Use Snowflake secrets management
- Implement role-based access control
- Audit dashboard usage via query history
- Monitor for unusual access patterns

## 🚦 Roadmap

- [ ] Custom alert configurations via UI
- [ ] Export reports to PDF/Excel
- [ ] Integration with external monitoring tools (Datadog, New Relic)
- [ ] Slack/Email notifications for alerts
- [ ] Advanced ML-based anomaly detection
- [ ] Query recommendation engine
- [ ] Cost forecasting with ML
- [ ] DBT project integration
- [ ] Custom metric builder
- [ ] Multi-account aggregation

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

MIT License - see LICENSE file for details

## 🙏 Acknowledgments

Inspired by features from:
- SELECT.dev
- Snowflake Trail
- Datadog Snowflake integration
- Monte Carlo Data
- Metaplane
- Chaos Genius
- CloudZero

## 📞 Support

- GitHub Issues: Report bugs or request features
- Discussions: Ask questions and share ideas

## 📊 Stats

- **Lines of Code**: ~2,500+
- **Query Functions**: 20+
- **Visualizations**: 50+
- **AI Insights**: 5 types
- **Tabs**: 11 comprehensive sections
- **Snowflake Views**: 30+

---

**Built with ❄️ and ❤️ for the Snowflake community**
