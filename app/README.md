# ❄️ Snowflake Holistic Observability Dashboard

**Version:** 1.0.0
**Type:** Snowflake Native Application

## Overview

The Snowflake Holistic Observability Dashboard is a comprehensive, enterprise-grade Native Application that provides deep visibility into your Snowflake environment. Built entirely on Snowflake's native platform, it leverages 40+ Account Usage views and Cortex AI to deliver intelligent insights across compute, storage, security, cost, and data quality.

### 🎯 Key Features

- **📊 Executive Overview** - Real-time KPIs, alerts, trends, and AI-powered executive summaries
- **🏢 Warehouse Analytics** - Credit usage, queue times, and optimization recommendations
- **💾 Storage Management** - Database/table breakdown, costs, and storage optimization
- **🔄 Data Transfer Monitoring** - Cross-region/cloud transfer tracking and cost analysis
- **👥 User & Query Analytics** - Activity patterns, query performance, and user insights
- **🤖 AI & ML Services** - Cortex Analyst, Search, Fine-tuning, and Complete API monitoring
- **🔧 Data Pipeline Observability** - Tasks, Snowpipes, Dynamic Tables, and streaming ingestion
- **⚡ Performance Optimization** - Query spilling, pruning efficiency, and compilation overhead
- **🔒 Security & Governance** - Access patterns, login auditing, and compliance tracking
- **💰 Cost Management** - Multi-dimensional cost attribution and anomaly detection
- **✅ Data Quality** - Freshness monitoring, schema drift detection, and quality scoring
- **🧠 AI Insights** - Interactive AI-powered insights using Snowflake Cortex

### 🏗️ Architecture

- **Multi-page Streamlit Application** - 12 specialized observability pages
- **40+ Account Usage Views** - Comprehensive data coverage
- **Snowflake Cortex Integration** - AI-powered insights using Mistral-Large2
- **Application Roles** - Admin, Analyst, and Viewer roles for access control
- **Configuration Management** - Customizable settings and alert thresholds

---

## 📋 Prerequisites

Before installing this Native App, ensure you have:

1. **Account Privileges:**
   - `CREATE APPLICATION` privilege in your Snowflake account
   - Ability to grant `IMPORTED PRIVILEGES` on the SNOWFLAKE database

2. **Snowflake Edition:**
   - Enterprise Edition or higher (recommended)
   - Standard Edition supported with limited Cortex features

3. **Warehouse:**
   - A warehouse with appropriate size for running the Streamlit app
   - Recommended: X-Small or Small warehouse for typical usage

4. **Data Availability:**
   - Account Usage views should have data (typically requires 1+ day after account setup)
   - Historical data available for trend analysis (30+ days recommended)

---

## 🚀 Installation

### Step 1: Create Application Package (Provider/Developer)

If you're deploying this app from source:

```sql
-- Create the application package
CREATE APPLICATION PACKAGE observability_pkg;

-- Create a stage for the app files
CREATE STAGE observability_pkg.app_stage;

-- Upload the app directory to the stage
-- (Use SnowSQL, Snowflake CLI, or Snowsight UI to upload files)
PUT file:///path/to/app/* @observability_pkg.app_stage/app/ AUTO_COMPRESS=FALSE RECURSIVE=TRUE;

-- Create the application package version
ALTER APPLICATION PACKAGE observability_pkg
  ADD VERSION v1_0 USING @observability_pkg.app_stage/app;

-- Set the default release directive
ALTER APPLICATION PACKAGE observability_pkg
  SET DEFAULT RELEASE DIRECTIVE VERSION = v1_0 PATCH = 0;
```

### Step 2: Install the Application (Consumer)

```sql
-- Create the application instance
CREATE APPLICATION snowflake_observability
  FROM APPLICATION PACKAGE observability_pkg
  USING VERSION v1_0;

-- Grant necessary privileges
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION snowflake_observability;

-- Grant usage on Cortex for AI insights
GRANT USAGE ON DATABASE SNOWFLAKE TO APPLICATION snowflake_observability;
```

### Step 3: Grant Application Roles to Users

```sql
-- Grant admin role to administrators
GRANT APPLICATION ROLE snowflake_observability.app_admin TO ROLE ACCOUNTADMIN;

-- Grant analyst role to data analysts
GRANT APPLICATION ROLE snowflake_observability.app_analyst TO ROLE DATA_ANALYST;

-- Grant viewer role to general users
GRANT APPLICATION ROLE snowflake_observability.app_viewer TO ROLE PUBLIC;
```

### Step 4: Access the Dashboard

1. Navigate to **Data Products** → **Apps** in Snowsight
2. Click on **snowflake_observability**
3. Click on the **Streamlit** tab or access via:
   ```sql
   -- View available Streamlit apps
   SHOW STREAMLITS IN APPLICATION snowflake_observability;
   ```

---

## ⚙️ Configuration

### Application Settings

The dashboard includes customizable settings accessible through the sidebar:

#### 💰 Cost Configuration
- **Credit Cost**: Your Snowflake credit cost in USD (default: $2.50)
- **Storage Cost**: Storage cost per TB per month (default: $23.00)

#### 📅 Time Period
- Analysis period: 1, 7, 14, 30, 60, or 90 days (default: 30 days)

#### 🚨 Alert Thresholds
- **Cost Spike**: Percentage increase to trigger alerts (default: 50%)
- **Query Time**: Long-running query threshold in seconds (default: 300s)
- **Failure Rate**: Query failure rate percentage (default: 10%)
- **Data Freshness**: Data staleness threshold in hours (default: 24h)

### Updating Configuration via SQL

You can also update configuration programmatically:

```sql
-- Update credit cost
CALL snowflake_observability.core.set_config('DEFAULT_CREDIT_COST', 3.0, 'Updated credit cost');

-- Update storage cost
CALL snowflake_observability.core.set_config('DEFAULT_STORAGE_COST', 25.0, 'Updated storage cost');

-- Get current configuration
CALL snowflake_observability.core.get_config('DEFAULT_CREDIT_COST');

-- View all configuration
SELECT * FROM snowflake_observability.core.app_config;
```

---

## 📖 Usage

### Application Roles

The application provides three roles with different access levels:

| Role | Permissions | Use Case |
|------|-------------|----------|
| **app_admin** | Full access to all features, configuration, and data | Administrators managing observability |
| **app_analyst** | Read access to all dashboards and analytics | Data analysts and engineers |
| **app_viewer** | View-only access to the Streamlit dashboard | General users and stakeholders |

### Dashboard Pages

#### 🏠 Home - Executive Overview
- Real-time KPIs (compute, storage, queries, cost)
- Alert dashboard with automatic anomaly detection
- Trend analysis and cost breakdowns
- AI-generated executive summary

#### 🏢 Warehouses
- Credit consumption by warehouse
- Queue times and concurrency analysis
- Warehouse utilization patterns
- AI-powered optimization recommendations

#### 💾 Storage
- Database and table storage breakdown
- Storage cost analysis
- Stage and snapshot usage
- Optimization opportunities

#### 🔄 Data Transfer
- Cross-region and cross-cloud transfers
- Transfer volume and cost trends
- Source/target region analysis

#### 👥 Users and Queries
- User activity patterns
- Query performance metrics
- Top users and longest queries
- Query failure analysis

#### 🤖 AI and ML
- Cortex Analyst usage
- Cortex Search metrics
- Model fine-tuning history
- Cortex Complete API usage

#### 🔧 Data Pipelines
- Task execution monitoring
- Snowpipe ingestion metrics
- Dynamic Table refresh performance
- Streaming channel health

#### ⚡ Performance
- Query spilling analysis
- Partition pruning efficiency
- Automatic clustering performance
- Search optimization impact

#### 🔒 Security
- Login history and failed attempts
- Access patterns and audit trails
- User and role grant analysis
- Security anomaly detection

#### 💰 Cost Management
- Multi-dimensional cost attribution
- Cost anomaly detection
- Cost forecasting and trends
- Service-level cost breakdown

#### ✅ Data Quality
- Data freshness monitoring
- Schema drift detection
- Quality score calculation
- Data health recommendations

#### 🧠 AI Insights
- Interactive AI-powered insights
- Custom query generation
- Natural language data exploration

---

## 🔐 Required Privileges

The application requires the following privileges to function:

### Account Usage Views
- **IMPORTED PRIVILEGES** on SNOWFLAKE database
- Access to SNOWFLAKE.ACCOUNT_USAGE schema

### Snowflake Cortex (AI Features)
- **USAGE** on SNOWFLAKE database
- Access to Cortex Complete API

### Optional Privileges
- **EXECUTE TASK** - For future automated reporting and alerting features

---

## 🐛 Troubleshooting

### Issue: "No data available"

**Cause:** Account Usage views may not have data yet or privileges not granted correctly.

**Solution:**
```sql
-- Verify privileges
SHOW GRANTS TO APPLICATION snowflake_observability;

-- Grant if missing
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION snowflake_observability;
```

### Issue: "Cortex AI features not working"

**Cause:** Missing USAGE privilege on SNOWFLAKE database.

**Solution:**
```sql
GRANT USAGE ON DATABASE SNOWFLAKE TO APPLICATION snowflake_observability;
```

### Issue: "Application role not accessible"

**Cause:** Application roles not granted to user roles.

**Solution:**
```sql
-- Grant appropriate application role
GRANT APPLICATION ROLE snowflake_observability.app_analyst TO ROLE YOUR_ROLE;
```

### Issue: "Data appears stale"

**Cause:** Account Usage views have latency (45 minutes to several hours).

**Solution:**
- Wait for data to propagate
- Use "Refresh Data" button in sidebar to clear cache
- Check Account Usage view latency documentation

### Viewing Application Logs

```sql
-- View application logs
SELECT * FROM TABLE(
  snowflake_observability.information_schema.task_history()
) ORDER BY scheduled_time DESC;
```

---

## 📊 Data Latency

Account Usage views have varying latency:

| View Type | Typical Latency |
|-----------|----------------|
| Query History | 45 minutes - 3 hours |
| Warehouse Metering | 45 minutes - 3 hours |
| Storage Usage | 24 hours |
| Login History | 2 hours |
| Access History | 2 hours |

The dashboard automatically accounts for this latency in its time-based queries.

---

## 🔄 Upgrading

To upgrade the application to a new version:

```sql
-- Upgrade to new version
ALTER APPLICATION snowflake_observability UPGRADE USING VERSION v1_1;

-- Verify upgrade
SELECT * FROM snowflake_observability.core.setup_status;
```

---

## 🗑️ Uninstallation

To remove the application:

```sql
-- Drop the application
DROP APPLICATION snowflake_observability;

-- Optional: Drop the application package (if you're the provider)
DROP APPLICATION PACKAGE observability_pkg;
```

**Note:** Uninstalling will remove all application data, including configuration and custom settings.

---

## 📚 Additional Resources

- [Snowflake Native Apps Documentation](https://docs.snowflake.com/en/developer-guide/native-apps/native-apps-about)
- [Account Usage Views Reference](https://docs.snowflake.com/en/sql-reference/account-usage)
- [Snowflake Cortex Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)

---

## 🤝 Support

For issues, questions, or feature requests:

1. Check the Troubleshooting section above
2. Review Snowflake Native Apps documentation
3. Contact your Snowflake account team

---

## 📝 License

Copyright © 2025. All rights reserved.

This Native Application is provided as-is for use within Snowflake environments. Redistribution, modification, or commercial use requires explicit permission.

---

## 🎯 Version History

### Version 1.0.0 (Initial Release)
- Complete multi-page observability dashboard
- 40+ Account Usage views integration
- Snowflake Cortex AI integration
- 12 specialized monitoring pages
- Application role-based access control
- Customizable configuration and alerts
- Comprehensive cost management and optimization

---

**Built with ❤️ for the Snowflake Community**
