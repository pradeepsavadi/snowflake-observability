# Snowflake Native App Deployment Guide

This guide walks you through deploying the Snowflake Holistic Observability Dashboard as a Snowflake Native Application.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Development Setup](#development-setup)
3. [Packaging the Application](#packaging-the-application)
4. [Testing Locally](#testing-locally)
5. [Publishing to Marketplace](#publishing-to-marketplace)
6. [Deployment Checklist](#deployment-checklist)
7. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Tools

1. **SnowSQL** or **Snowflake CLI**
   ```bash
   # Install Snowflake CLI
   pip install snowflake-cli-labs

   # Or install SnowSQL
   # Download from: https://docs.snowflake.com/en/user-guide/snowsql-install-config.html
   ```

2. **Python 3.8+** (for local development)
   ```bash
   python --version
   ```

3. **Git** (for version control)
   ```bash
   git --version
   ```

### Snowflake Account Requirements

- **Role**: `ACCOUNTADMIN` or equivalent
- **Privileges**:
  - `CREATE APPLICATION PACKAGE`
  - `CREATE DATABASE`
  - `CREATE STAGE`
  - `USAGE` on a warehouse
- **Edition**: Enterprise or higher (recommended)

---

## Development Setup

### 1. Directory Structure

Ensure your Native App has the following structure:

```
app/
├── manifest.yml              # Application manifest
├── setup_script.sql          # Setup script executed on install
├── README.md                 # User-facing documentation
├── streamlit/               # Streamlit application files
│   ├── main.py              # Main entry point
│   ├── utils.py             # Shared utilities
│   └── pages/               # Multi-page Streamlit app
│       ├── 1_🏢_Warehouses.py
│       ├── 2_💾_Storage.py
│       ├── 3_🔄_Data_Transfer.py
│       ├── 4_👥_Users_and_Queries.py
│       ├── 5_🤖_AI_and_ML.py
│       ├── 6_🔧_Data_Pipelines.py
│       ├── 7_⚡_Performance.py
│       ├── 8_🔒_Security.py
│       ├── 9_💰_Cost_Management.py
│       ├── 10_✅_Data_Quality.py
│       └── 11_🧠_AI_Insights.py
└── scripts/                 # Optional: Additional scripts
```

### 2. Validate Files

```bash
# Check that all required files exist
ls -la app/manifest.yml
ls -la app/setup_script.sql
ls -la app/README.md
ls -la app/streamlit/main.py
ls -la app/streamlit/pages/
```

---

## Packaging the Application

### Method 1: Using Snowflake CLI (Recommended)

#### Step 1: Create snowflake.yml

Create a `snowflake.yml` file in your project root:

```yaml
definition_version: 1
native_app:
  name: snowflake_observability
  artifacts:
    - src: app/*
      dest: ./
  application:
    name: snowflake_observability_dev
    role: ACCOUNTADMIN
    warehouse: COMPUTE_WH
    debug: true
  package:
    name: observability_pkg
    role: ACCOUNTADMIN
    scripts:
      - app/setup_script.sql
```

#### Step 2: Deploy with Snowflake CLI

```bash
# Initialize the project
snow app init

# Deploy the application (creates package and installs)
snow app run

# Or deploy step-by-step:
# 1. Create the package
snow app deploy

# 2. Create the application
snow app create

# 3. Grant privileges
snow sql -q "GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION snowflake_observability_dev"
snow sql -q "GRANT USAGE ON DATABASE SNOWFLAKE TO APPLICATION snowflake_observability_dev"
```

### Method 2: Using SnowSQL (Manual)

#### Step 1: Create Application Package

```bash
# Connect to Snowflake
snowsql -a <account> -u <username>
```

```sql
-- Create the application package
CREATE APPLICATION PACKAGE IF NOT EXISTS observability_pkg;

-- Use the package
USE APPLICATION PACKAGE observability_pkg;

-- Create a stage for app files
CREATE STAGE IF NOT EXISTS app_stage
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
  COMMENT = 'Stage for Native App files';
```

#### Step 2: Upload Application Files

```bash
# Navigate to your project directory
cd /path/to/snowflake-observability

# Upload all files from app directory
snowsql -a <account> -u <username> -q "
PUT file://app/manifest.yml @observability_pkg.app_stage/manifest.yml AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://app/setup_script.sql @observability_pkg.app_stage/setup_script.sql AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://app/README.md @observability_pkg.app_stage/README.md AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
"

# Upload Streamlit files (recursive)
find app/streamlit -type f -name "*.py" -exec \
  snowsql -a <account> -u <username> -q \
  "PUT file://{} @observability_pkg.app_stage/{} AUTO_COMPRESS=FALSE OVERWRITE=TRUE" \;
```

Or use the PUT command with recursion:

```bash
# Upload entire app directory recursively
snowsql -a <account> -u <username> -q "
PUT file://app/* @observability_pkg.app_stage/ AUTO_COMPRESS=FALSE RECURSIVE=TRUE OVERWRITE=TRUE;
"
```

#### Step 3: Verify Upload

```sql
-- List uploaded files
LIST @observability_pkg.app_stage;

-- Verify key files exist
LIST @observability_pkg.app_stage PATTERN='.*manifest.yml';
LIST @observability_pkg.app_stage PATTERN='.*setup_script.sql';
LIST @observability_pkg.app_stage PATTERN='.*main.py';
```

#### Step 4: Create Application Package Version

```sql
-- Add version to package
ALTER APPLICATION PACKAGE observability_pkg
  ADD VERSION V1_0 USING @app_stage;

-- Set as default version
ALTER APPLICATION PACKAGE observability_pkg
  SET DEFAULT RELEASE DIRECTIVE VERSION = V1_0 PATCH = 0;

-- Verify version
SHOW VERSIONS IN APPLICATION PACKAGE observability_pkg;
```

---

## Testing Locally

### Step 1: Install the Application

```sql
-- Create application instance for testing
CREATE APPLICATION IF NOT EXISTS snowflake_observability_dev
  FROM APPLICATION PACKAGE observability_pkg
  USING VERSION V1_0
  DEBUG_MODE = TRUE
  COMMENT = 'Development instance of Snowflake Observability Dashboard';

-- Grant required privileges
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE
  TO APPLICATION snowflake_observability_dev;

GRANT USAGE ON DATABASE SNOWFLAKE
  TO APPLICATION snowflake_observability_dev;
```

### Step 2: Grant Application Roles

```sql
-- Grant admin role for testing
GRANT APPLICATION ROLE snowflake_observability_dev.app_admin
  TO ROLE ACCOUNTADMIN;

-- Optional: Grant to other roles
GRANT APPLICATION ROLE snowflake_observability_dev.app_analyst
  TO ROLE SYSADMIN;

GRANT APPLICATION ROLE snowflake_observability_dev.app_viewer
  TO ROLE PUBLIC;
```

### Step 3: Verify Installation

```sql
-- Check application status
SHOW APPLICATIONS LIKE 'snowflake_observability_dev';

-- Verify schemas created
SHOW SCHEMAS IN APPLICATION snowflake_observability_dev;

-- Check configuration
SELECT * FROM snowflake_observability_dev.core.app_config;

-- Verify Streamlit app
SHOW STREAMLITS IN APPLICATION snowflake_observability_dev;

-- View setup status
SELECT * FROM snowflake_observability_dev.core.setup_status;
```

### Step 4: Access the Dashboard

1. Navigate to **Snowsight** → **Data Products** → **Apps**
2. Click on **snowflake_observability_dev**
3. Click the **Streamlit** tab
4. Verify all pages load correctly

### Step 5: Test Functionality

Create a test script to validate key features:

```sql
-- Test configuration procedures
CALL snowflake_observability_dev.core.get_config('DEFAULT_CREDIT_COST');
CALL snowflake_observability_dev.core.set_config('DEFAULT_CREDIT_COST', 3.0, 'Test update');

-- Verify views are accessible
SELECT COUNT(*) FROM snowflake_observability_dev.core.warehouse_metering_v
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP());

SELECT COUNT(*) FROM snowflake_observability_dev.core.query_history_v
WHERE start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP());

-- Test application roles
USE ROLE ACCOUNTADMIN;
SELECT CURRENT_ROLE();
```

### Step 6: Validate Dashboards

Test each dashboard page:

- [ ] **Home** - Executive Overview loads with metrics
- [ ] **Warehouses** - Warehouse analytics display
- [ ] **Storage** - Storage metrics appear
- [ ] **Data Transfer** - Transfer data shows
- [ ] **Users & Queries** - User activity visible
- [ ] **AI & ML** - Cortex metrics display
- [ ] **Data Pipelines** - Pipeline data shows
- [ ] **Performance** - Performance metrics load
- [ ] **Security** - Security audit displays
- [ ] **Cost Management** - Cost data appears
- [ ] **Data Quality** - Quality metrics show
- [ ] **AI Insights** - Cortex insights generate

---

## Publishing to Marketplace

### Step 1: Prepare for Marketplace

1. **Complete Documentation**
   - Finalize README.md with clear instructions
   - Add screenshots and demo videos
   - Include pricing information (if applicable)

2. **Set Visibility**
   ```sql
   -- Set package visibility (internal/account/org/public)
   ALTER APPLICATION PACKAGE observability_pkg
     SET DISTRIBUTION = INTERNAL;  -- Or EXTERNAL for marketplace
   ```

3. **Create Release Version**
   ```sql
   -- Create production version
   ALTER APPLICATION PACKAGE observability_pkg
     ADD VERSION V1_0_PROD USING @app_stage LABEL 'Production Release';

   -- Set as default
   ALTER APPLICATION PACKAGE observability_pkg
     SET DEFAULT RELEASE DIRECTIVE VERSION = V1_0_PROD PATCH = 0;
   ```

### Step 2: Submit to Marketplace

1. Navigate to **Snowsight** → **Data Products** → **Provider Studio**
2. Click **Publish Listing**
3. Select your application package
4. Complete the listing form:
   - **Title**: Snowflake Holistic Observability Dashboard
   - **Category**: Data Observability, Monitoring
   - **Description**: Comprehensive observability solution...
   - **Pricing Model**: Free or Paid
   - **Support Contact**: your-support@example.com
5. Add screenshots and documentation
6. Submit for review

### Step 3: Marketplace Review

- Snowflake reviews typically take 1-2 weeks
- Address any feedback from Snowflake review team
- Once approved, listing will be live

---

## Deployment Checklist

### Pre-Deployment

- [ ] All files are in correct directory structure
- [ ] manifest.yml is properly configured
- [ ] setup_script.sql executes without errors
- [ ] README.md is complete and accurate
- [ ] All Streamlit pages are tested locally
- [ ] Version number is updated
- [ ] Dependencies are documented

### Packaging

- [ ] Application package created successfully
- [ ] All files uploaded to stage
- [ ] Version added to package
- [ ] Default release directive set

### Testing

- [ ] Application installs without errors
- [ ] All privileges granted correctly
- [ ] Application roles created
- [ ] Configuration table populated
- [ ] Streamlit app accessible
- [ ] All 12 pages load correctly
- [ ] Account Usage data displays
- [ ] Cortex AI features work
- [ ] No console errors

### Production

- [ ] Production version created
- [ ] Documentation reviewed
- [ ] Support processes in place
- [ ] Monitoring configured
- [ ] Rollback plan documented

---

## Troubleshooting

### Issue: "Cannot upload files to stage"

**Solution:**
```sql
-- Verify stage exists and you have permissions
SHOW STAGES IN APPLICATION PACKAGE observability_pkg;

-- Recreate stage if needed
CREATE OR REPLACE STAGE observability_pkg.app_stage;

-- Check warehouse is running
SHOW WAREHOUSES;
USE WAREHOUSE COMPUTE_WH;
```

### Issue: "Application install fails"

**Solution:**
```sql
-- Check application package versions
SHOW VERSIONS IN APPLICATION PACKAGE observability_pkg;

-- View detailed error logs
SELECT * FROM TABLE(
  INFORMATION_SCHEMA.TASK_HISTORY(
    APPLICATION_NAME => 'snowflake_observability_dev'
  )
);

-- Drop and recreate application
DROP APPLICATION IF EXISTS snowflake_observability_dev;
CREATE APPLICATION snowflake_observability_dev
  FROM APPLICATION PACKAGE observability_pkg
  DEBUG_MODE = TRUE;
```

### Issue: "Streamlit app not visible"

**Solution:**
```sql
-- Verify Streamlit object exists
SHOW STREAMLITS IN APPLICATION snowflake_observability_dev;

-- Check schema privileges
SHOW GRANTS ON SCHEMA snowflake_observability_dev.streamlit_app;

-- Verify file paths in manifest
-- Ensure default_streamlit points to correct file
```

### Issue: "Account Usage views return no data"

**Solution:**
```sql
-- Verify privileges
SHOW GRANTS TO APPLICATION snowflake_observability_dev;

-- Re-grant if needed
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE
  TO APPLICATION snowflake_observability_dev;

-- Check Account Usage directly
SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(day, -1, CURRENT_TIMESTAMP());
```

### Issue: "Cortex features not working"

**Solution:**
```sql
-- Grant Cortex access
GRANT USAGE ON DATABASE SNOWFLAKE
  TO APPLICATION snowflake_observability_dev;

-- Verify Snowflake edition supports Cortex
SHOW PARAMETERS LIKE 'SNOWFLAKE_EDITION' IN ACCOUNT;

-- Test Cortex directly
SELECT SNOWFLAKE.CORTEX.COMPLETE(
  'mistral-large2',
  'Hello, how are you?'
);
```

---

## Upgrading the Application

### Create New Version

```sql
-- Upload updated files to stage
PUT file://app/* @observability_pkg.app_stage/
  AUTO_COMPRESS=FALSE RECURSIVE=TRUE OVERWRITE=TRUE;

-- Add new version
ALTER APPLICATION PACKAGE observability_pkg
  ADD VERSION V1_1 USING @app_stage;

-- Upgrade existing application
ALTER APPLICATION snowflake_observability_dev
  UPGRADE USING VERSION V1_1;
```

### Rollback

```sql
-- Rollback to previous version
ALTER APPLICATION snowflake_observability_dev
  UPGRADE USING VERSION V1_0;
```

---

## Best Practices

1. **Version Control**
   - Use semantic versioning (e.g., V1_0, V1_1, V2_0)
   - Tag releases in Git
   - Maintain CHANGELOG.md

2. **Testing**
   - Test in development account first
   - Use DEBUG_MODE = TRUE during development
   - Validate all features before production deployment

3. **Security**
   - Follow principle of least privilege
   - Use application roles appropriately
   - Regularly audit access patterns

4. **Documentation**
   - Keep README.md up-to-date
   - Document all configuration options
   - Provide troubleshooting guides

5. **Monitoring**
   - Monitor application usage
   - Track performance metrics
   - Set up alerting for errors

---

## Additional Resources

- [Snowflake Native Apps Framework](https://docs.snowflake.com/en/developer-guide/native-apps/native-apps-about)
- [Snowflake CLI Documentation](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index)
- [Provider Studio Guide](https://docs.snowflake.com/en/developer-guide/native-apps/provider-studio)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)

---

## Support

For deployment issues or questions:
- Review this deployment guide
- Check Snowflake Native Apps documentation
- Contact Snowflake support
- Review application logs in Snowflake

---

**Last Updated:** 2025-11-01
**Version:** 1.0.0
