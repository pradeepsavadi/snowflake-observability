-- Snowflake Holistic Observability Dashboard - Native App Setup Script
-- This script is executed when the application is installed or upgraded

-- ============================================================================
-- 1. CREATE APPLICATION SCHEMAS
-- ============================================================================

-- Core schema for application objects
CREATE SCHEMA IF NOT EXISTS core
    COMMENT = 'Core schema containing application configuration and metadata';

-- Schema for Streamlit application
CREATE SCHEMA IF NOT EXISTS streamlit_app
    COMMENT = 'Schema containing the Streamlit observability dashboard';

-- Schema for shared utilities and procedures
CREATE SCHEMA IF NOT EXISTS utils
    COMMENT = 'Schema for shared utilities, functions, and procedures';

-- ============================================================================
-- 2. CREATE APPLICATION ROLES
-- ============================================================================

-- Admin role with full access to all application features
CREATE APPLICATION ROLE IF NOT EXISTS app_admin
    COMMENT = 'Administrator role with full access to the observability dashboard';

-- Viewer role with read-only access
CREATE APPLICATION ROLE IF NOT EXISTS app_viewer
    COMMENT = 'Read-only role for viewing observability metrics';

-- Analyst role with access to all analytics features
CREATE APPLICATION ROLE IF NOT EXISTS app_analyst
    COMMENT = 'Analyst role with access to all observability and analytics features';

-- ============================================================================
-- 3. CREATE CONFIGURATION TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS core.app_config (
    config_key VARCHAR(255) PRIMARY KEY,
    config_value VARIANT,
    description VARCHAR(1000),
    last_updated TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_by VARCHAR(255)
) COMMENT = 'Application configuration settings';

-- Insert default configuration values
MERGE INTO core.app_config AS target
USING (
    SELECT * FROM (
        VALUES
            ('DEFAULT_CREDIT_COST', 2.5, 'Default cost per credit in USD'),
            ('DEFAULT_STORAGE_COST', 23.0, 'Default storage cost per TB per month in USD'),
            ('DEFAULT_TIME_PERIOD', 30, 'Default time period for analysis in days'),
            ('CACHE_TTL', 3600, 'Cache time-to-live in seconds'),
            ('MAX_RESULTS', 1000, 'Maximum number of results to return in queries'),
            ('ALERT_COST_SPIKE_PCT', 50, 'Alert threshold for cost spike percentage'),
            ('ALERT_QUERY_TIME_SEC', 300, 'Alert threshold for long-running queries in seconds'),
            ('ALERT_FAILURE_RATE_PCT', 10, 'Alert threshold for query failure rate percentage'),
            ('ALERT_DATA_FRESHNESS_HOURS', 24, 'Alert threshold for data freshness in hours'),
            ('APP_VERSION', '1.0.0', 'Current application version'),
            ('CORTEX_MODEL', 'mistral-large2', 'Default Cortex model for AI insights')
    ) AS config_data(config_key, config_value, description)
) AS source
ON target.config_key = source.config_key
WHEN MATCHED THEN
    UPDATE SET
        config_value = source.config_value,
        description = source.description,
        last_updated = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description, last_updated)
    VALUES (source.config_key, source.config_value, source.description, CURRENT_TIMESTAMP());

-- ============================================================================
-- 4. CREATE STREAMLIT APPLICATION
-- ============================================================================

CREATE OR REPLACE STREAMLIT streamlit_app.observability_dashboard
    FROM '/streamlit'
    MAIN_FILE = '/main.py'
    COMMENT = 'Snowflake Holistic Observability Dashboard - Multi-page observability and analytics platform';

-- ============================================================================
-- 5. CREATE HELPER VIEWS FOR ACCOUNT USAGE ACCESS
-- ============================================================================

-- View for warehouse metering with proper access control
CREATE OR REPLACE VIEW core.warehouse_metering_v
    COMMENT = 'Warehouse credit usage metrics'
AS
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY;

-- View for query history
CREATE OR REPLACE VIEW core.query_history_v
    COMMENT = 'Query execution history and performance metrics'
AS
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY;

-- View for storage usage
CREATE OR REPLACE VIEW core.storage_usage_v
    COMMENT = 'Database storage usage metrics'
AS
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY;

-- View for login history
CREATE OR REPLACE VIEW core.login_history_v
    COMMENT = 'User login history and authentication events'
AS
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY;

-- View for access history
CREATE OR REPLACE VIEW core.access_history_v
    COMMENT = 'Object access audit trail'
AS
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY;

-- ============================================================================
-- 6. CREATE CONFIGURATION PROCEDURES
-- ============================================================================

-- Procedure to get configuration value
CREATE OR REPLACE PROCEDURE core.get_config(config_key VARCHAR)
    RETURNS VARIANT
    LANGUAGE SQL
    COMMENT = 'Retrieve a configuration value by key'
AS
$$
BEGIN
    LET result VARIANT;
    SELECT config_value INTO :result
    FROM core.app_config
    WHERE config_key = :config_key;
    RETURN result;
END;
$$;

-- Procedure to set configuration value
CREATE OR REPLACE PROCEDURE core.set_config(
    config_key VARCHAR,
    config_value VARIANT,
    description VARCHAR DEFAULT NULL
)
    RETURNS VARCHAR
    LANGUAGE SQL
    COMMENT = 'Set a configuration value'
AS
$$
BEGIN
    MERGE INTO core.app_config AS target
    USING (SELECT :config_key AS key, :config_value AS value, :description AS desc) AS source
    ON target.config_key = source.key
    WHEN MATCHED THEN
        UPDATE SET
            config_value = source.value,
            description = COALESCE(source.desc, target.description),
            last_updated = CURRENT_TIMESTAMP(),
            updated_by = CURRENT_USER()
    WHEN NOT MATCHED THEN
        INSERT (config_key, config_value, description, last_updated, updated_by)
        VALUES (source.key, source.value, source.desc, CURRENT_TIMESTAMP(), CURRENT_USER());

    RETURN 'Configuration updated successfully';
END;
$$;

-- ============================================================================
-- 7. GRANT PRIVILEGES TO APPLICATION ROLES
-- ============================================================================

-- Grant privileges to app_admin role
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_admin;
GRANT USAGE ON SCHEMA streamlit_app TO APPLICATION ROLE app_admin;
GRANT USAGE ON SCHEMA utils TO APPLICATION ROLE app_admin;
GRANT ALL PRIVILEGES ON TABLE core.app_config TO APPLICATION ROLE app_admin;
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA core TO APPLICATION ROLE app_admin;
GRANT USAGE ON PROCEDURE core.get_config(VARCHAR) TO APPLICATION ROLE app_admin;
GRANT USAGE ON PROCEDURE core.set_config(VARCHAR, VARIANT, VARCHAR) TO APPLICATION ROLE app_admin;
GRANT USAGE ON STREAMLIT streamlit_app.observability_dashboard TO APPLICATION ROLE app_admin;

-- Grant privileges to app_analyst role
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_analyst;
GRANT USAGE ON SCHEMA streamlit_app TO APPLICATION ROLE app_analyst;
GRANT USAGE ON SCHEMA utils TO APPLICATION ROLE app_analyst;
GRANT SELECT ON TABLE core.app_config TO APPLICATION ROLE app_analyst;
GRANT SELECT ON ALL VIEWS IN SCHEMA core TO APPLICATION ROLE app_analyst;
GRANT USAGE ON PROCEDURE core.get_config(VARCHAR) TO APPLICATION ROLE app_analyst;
GRANT USAGE ON STREAMLIT streamlit_app.observability_dashboard TO APPLICATION ROLE app_analyst;

-- Grant privileges to app_viewer role
GRANT USAGE ON SCHEMA streamlit_app TO APPLICATION ROLE app_viewer;
GRANT USAGE ON STREAMLIT streamlit_app.observability_dashboard TO APPLICATION ROLE app_viewer;

-- ============================================================================
-- 8. CREATE APPLICATION METADATA
-- ============================================================================

CREATE TABLE IF NOT EXISTS core.app_metadata (
    metadata_key VARCHAR(255) PRIMARY KEY,
    metadata_value VARIANT,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Application metadata and information';

INSERT INTO core.app_metadata (metadata_key, metadata_value)
VALUES
    ('app_name', 'Snowflake Holistic Observability Dashboard'),
    ('app_version', '1.0.0'),
    ('app_description', 'Comprehensive multi-page observability and analytics platform for Snowflake'),
    ('installation_date', CURRENT_TIMESTAMP()),
    ('pages', ARRAY_CONSTRUCT(
        'Home - Executive Overview',
        'Warehouses - Compute Analytics',
        'Storage - Storage Management',
        'Data Transfer - Network Metrics',
        'Users and Queries - User Analytics',
        'AI and ML - Cortex Services',
        'Data Pipelines - Ingestion Monitoring',
        'Performance - Query Optimization',
        'Security - Governance and Audit',
        'Cost Management - FinOps Analytics',
        'Data Quality - Data Health Monitoring',
        'AI Insights - Interactive AI Generation'
    ))
ON CONFLICT (metadata_key) DO NOTHING;

-- ============================================================================
-- 9. SETUP COMPLETE MESSAGE
-- ============================================================================

-- Create a view to display setup status
CREATE OR REPLACE VIEW core.setup_status AS
SELECT
    'Snowflake Holistic Observability Dashboard' AS app_name,
    '1.0.0' AS version,
    'INSTALLED' AS status,
    CURRENT_TIMESTAMP() AS installation_time,
    'Access the dashboard via the Streamlit app in the streamlit_app schema' AS access_instructions;

-- Grant access to setup status view
GRANT SELECT ON VIEW core.setup_status TO APPLICATION ROLE app_admin;
GRANT SELECT ON VIEW core.setup_status TO APPLICATION ROLE app_analyst;
GRANT SELECT ON VIEW core.setup_status TO APPLICATION ROLE app_viewer;
