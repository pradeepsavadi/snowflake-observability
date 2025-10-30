"""
Snowflake Holistic Observability Dashboard - Security Page
==========================================================
Monitor access patterns, login activity, grants, and security configurations
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
    page_title="Security - Snowflake Observability",
    page_icon="🔒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom styling
apply_custom_css()

# Initialize session state and render settings
initialize_session_state()
render_settings_sidebar()

# Page header
render_page_header("🔒 Security & Governance", "Monitor access patterns, login activity, and security configurations")

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

# ============================================================================
# SECURITY OVERVIEW
# ============================================================================

st.markdown("---")
st.subheader("📊 Security Overview")

col1, col2, col3, col4 = st.columns(4)

with st.spinner("Loading security metrics..."):
    try:
        # Get active users
        active_users_query = f"""
        SELECT COUNT(DISTINCT USER_NAME) AS ACTIVE_USERS
        FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
        WHERE EVENT_TIMESTAMP >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        AND IS_SUCCESS = 'YES'
        """
        active_users = session.sql(active_users_query).to_pandas()['ACTIVE_USERS'].iloc[0]

        # Get failed logins
        failed_logins_query = f"""
        SELECT COUNT(*) AS FAILED_LOGINS
        FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
        WHERE EVENT_TIMESTAMP >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        AND IS_SUCCESS = 'NO'
        """
        failed_logins = session.sql(failed_logins_query).to_pandas()['FAILED_LOGINS'].iloc[0]

        # Get total grants
        grants_query = """
        SELECT COUNT(*) AS TOTAL_GRANTS
        FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
        WHERE DELETED_ON IS NULL
        """
        total_grants = session.sql(grants_query).to_pandas()['TOTAL_GRANTS'].iloc[0]

        # Get active roles
        active_roles_query = """
        SELECT COUNT(DISTINCT NAME) AS ACTIVE_ROLES
        FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES
        WHERE DELETED_ON IS NULL
        """
        active_roles = session.sql(active_roles_query).to_pandas()['ACTIVE_ROLES'].iloc[0]

        with col1:
            st.metric(
                "Active Users",
                format_number(int(active_users)),
                help=f"Users with successful logins in last {time_period} days"
            )

        with col2:
            st.metric(
                "Failed Logins",
                format_number(int(failed_logins)),
                delta_color="inverse",
                help=f"Failed login attempts in last {time_period} days"
            )

        with col3:
            st.metric(
                "Active Grants",
                format_number(int(total_grants)),
                help="Total active privilege grants"
            )

        with col4:
            st.metric(
                "Active Roles",
                format_number(int(active_roles)),
                help="Number of active roles"
            )

        # Security alerts
        st.markdown("---")

        if failed_logins > 100:
            create_alert_badge(f"⚠️ High number of failed logins ({int(failed_logins)})", "warning")

        if failed_logins == 0:
            create_alert_badge("✅ No failed login attempts", "success")

    except Exception as e:
        st.error(f"Error loading security overview: {str(e)}")

# ============================================================================
# SECURITY TABS
# ============================================================================

tab1, tab2, tab3, tab4 = st.tabs([
    "🔐 Login Activity",
    "👥 User & Role Management",
    "🔑 Access Control",
    "🛡️ Security Recommendations"
])

# ----------------------------------------------------------------------------
# TAB 1: Login Activity
# ----------------------------------------------------------------------------

with tab1:
    st.markdown("### 🔐 Login Activity Monitoring")

    try:
        # Login history
        login_history_query = f"""
        SELECT
            USER_NAME,
            EVENT_TYPE,
            IS_SUCCESS,
            ERROR_CODE,
            ERROR_MESSAGE,
            CLIENT_IP,
            REPORTED_CLIENT_TYPE,
            FIRST_AUTHENTICATION_FACTOR,
            SECOND_AUTHENTICATION_FACTOR,
            EVENT_TIMESTAMP
        FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
        WHERE EVENT_TIMESTAMP >= DATEADD(DAY, -{time_period}, CURRENT_DATE())
        ORDER BY EVENT_TIMESTAMP DESC
        LIMIT 1000
        """
        login_history = session.sql(login_history_query).to_pandas()

        if not login_history.empty:
            login_history['EVENT_TIMESTAMP'] = pd.to_datetime(login_history['EVENT_TIMESTAMP'])

            # Summary metrics
            total_logins = len(login_history)
            successful_logins = len(login_history[login_history['IS_SUCCESS'] == 'YES'])
            failed_logins_count = len(login_history[login_history['IS_SUCCESS'] == 'NO'])
            success_rate = (successful_logins / total_logins * 100) if total_logins > 0 else 0
            mfa_enabled = len(login_history[login_history['SECOND_AUTHENTICATION_FACTOR'].notna()])
            mfa_rate = (mfa_enabled / total_logins * 100) if total_logins > 0 else 0

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Total Logins", format_number(total_logins))

            with col2:
                st.metric("Success Rate", f"{success_rate:.1f}%")

            with col3:
                st.metric("Failed Logins", format_number(failed_logins_count), delta_color="inverse")

            with col4:
                st.metric("MFA Usage", f"{mfa_rate:.1f}%")

            # Daily login trend
            st.markdown("---")
            st.markdown("#### Daily Login Activity")

            daily_logins = login_history.groupby(login_history['EVENT_TIMESTAMP'].dt.date).agg({
                'USER_NAME': 'count',
                'IS_SUCCESS': lambda x: (x == 'YES').sum()
            }).reset_index()

            daily_logins.columns = ['DATE', 'TOTAL_LOGINS', 'SUCCESSFUL_LOGINS']
            daily_logins['FAILED_LOGINS'] = daily_logins['TOTAL_LOGINS'] - daily_logins['SUCCESSFUL_LOGINS']
            daily_logins['DATE'] = pd.to_datetime(daily_logins['DATE'])

            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=daily_logins['DATE'],
                y=daily_logins['SUCCESSFUL_LOGINS'],
                name='Successful',
                marker_color='lightgreen'
            ))

            fig.add_trace(go.Bar(
                x=daily_logins['DATE'],
                y=daily_logins['FAILED_LOGINS'],
                name='Failed',
                marker_color='salmon'
            ))

            fig.update_layout(
                title="Daily Login Activity",
                xaxis_title="Date",
                yaxis_title="Login Count",
                barmode='stack',
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

            # Failed login analysis
            if failed_logins_count > 0:
                st.markdown("---")
                st.markdown("#### Failed Login Analysis")

                failed_logins_df = login_history[login_history['IS_SUCCESS'] == 'NO'].copy()

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("**Failed Logins by User**")

                    user_failures = failed_logins_df.groupby('USER_NAME').size().reset_index(name='FAILURE_COUNT')
                    user_failures = user_failures.sort_values('FAILURE_COUNT', ascending=False).head(10)

                    fig = px.bar(
                        user_failures,
                        x='FAILURE_COUNT',
                        y='USER_NAME',
                        orientation='h',
                        title='Top 10 Users with Failed Logins',
                        labels={'FAILURE_COUNT': 'Failed Attempts', 'USER_NAME': 'User'}
                    )

                    fig.update_traces(marker_color='salmon')
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    st.markdown("**Failed Logins by Error Code**")

                    error_counts = failed_logins_df.groupby('ERROR_CODE').size().reset_index(name='COUNT')
                    error_counts = error_counts.sort_values('COUNT', ascending=False)

                    fig = px.pie(
                        error_counts,
                        values='COUNT',
                        names='ERROR_CODE',
                        title='Failed Login Error Distribution'
                    )

                    st.plotly_chart(fig, use_container_width=True)

                # Recent failed logins
                st.markdown("**Recent Failed Login Attempts**")

                display_df = failed_logins_df[[
                    'EVENT_TIMESTAMP', 'USER_NAME', 'ERROR_CODE', 'ERROR_MESSAGE', 'CLIENT_IP'
                ]].head(20).copy()

                display_df.columns = ['Time', 'User', 'Error Code', 'Error Message', 'IP Address']

                st.dataframe(
                    display_df.style.format({
                        'Time': lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
                    }),
                    use_container_width=True,
                    height=300
                )

                # Security recommendations for failed logins
                if failed_logins_count > 50:
                    create_alert_badge(
                        "⚠️ High number of failed logins detected. Consider enabling account lockout policies.",
                        "warning"
                    )

            # MFA adoption
            st.markdown("---")
            st.markdown("#### Multi-Factor Authentication Adoption")

            col1, col2 = st.columns(2)

            with col1:
                mfa_data = pd.DataFrame({
                    'Type': ['MFA Enabled', 'No MFA'],
                    'Count': [mfa_enabled, total_logins - mfa_enabled]
                })

                fig = px.pie(
                    mfa_data,
                    values='Count',
                    names='Type',
                    title='MFA Usage',
                    color_discrete_sequence=['lightgreen', 'salmon']
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Users without MFA
                no_mfa_users = login_history[
                    login_history['SECOND_AUTHENTICATION_FACTOR'].isna()
                ]['USER_NAME'].unique()

                st.metric("Users Without MFA", len(no_mfa_users))

                if len(no_mfa_users) > 0 and mfa_rate < 80:
                    create_alert_badge(
                        f"⚠️ {len(no_mfa_users)} user(s) not using MFA - Enable MFA for all users",
                        "warning"
                    )

                    with st.expander("View users without MFA"):
                        st.write(sorted(no_mfa_users.tolist()))

            # Login by client type
            st.markdown("---")
            st.markdown("#### Login by Client Type")

            client_types = login_history.groupby('REPORTED_CLIENT_TYPE').size().reset_index(name='COUNT')
            client_types = client_types.sort_values('COUNT', ascending=False)

            fig = px.bar(
                client_types,
                x='REPORTED_CLIENT_TYPE',
                y='COUNT',
                title='Logins by Client Type',
                labels={'COUNT': 'Login Count', 'REPORTED_CLIENT_TYPE': 'Client Type'}
            )

            fig.update_traces(marker_color='steelblue')
            st.plotly_chart(fig, use_container_width=True)

        else:
            st.info("No login history available")

    except Exception as e:
        st.error(f"Error loading login activity: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 2: User & Role Management
# ----------------------------------------------------------------------------

with tab2:
    st.markdown("### 👥 User & Role Management")

    try:
        # Users list
        users_query = """
        SELECT
            NAME AS USER_NAME,
            LOGIN_NAME,
            DISPLAY_NAME,
            DISABLED,
            HAS_PASSWORD,
            HAS_RSA_PUBLIC_KEY,
            DEFAULT_WAREHOUSE,
            DEFAULT_ROLE,
            CREATED_ON,
            DELETED_ON,
            LAST_SUCCESS_LOGIN,
            EXPIRES_AT_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
        WHERE DELETED_ON IS NULL
        ORDER BY NAME
        """
        users = session.sql(users_query).to_pandas()

        if not users.empty:
            users['CREATED_ON'] = pd.to_datetime(users['CREATED_ON'])
            users['LAST_SUCCESS_LOGIN'] = pd.to_datetime(users['LAST_SUCCESS_LOGIN'])
            users['EXPIRES_AT_TIME'] = pd.to_datetime(users['EXPIRES_AT_TIME'])

            # User statistics
            total_users = len(users)
            disabled_users = len(users[users['DISABLED'] == True])
            enabled_users = total_users - disabled_users
            users_with_pwd = len(users[users['HAS_PASSWORD'] == True])
            users_with_key = len(users[users['HAS_RSA_PUBLIC_KEY'] == True])

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Total Users", total_users)

            with col2:
                st.metric("Enabled Users", enabled_users)

            with col3:
                st.metric("Password Auth", users_with_pwd)

            with col4:
                st.metric("Key-Pair Auth", users_with_key)

            st.markdown("---")

            # User list
            st.markdown("#### User Inventory")

            display_df = users[[
                'USER_NAME', 'LOGIN_NAME', 'DISABLED', 'DEFAULT_WAREHOUSE',
                'DEFAULT_ROLE', 'LAST_SUCCESS_LOGIN', 'CREATED_ON'
            ]].copy()

            display_df.columns = [
                'User', 'Login', 'Disabled', 'Default Warehouse',
                'Default Role', 'Last Login', 'Created'
            ]

            st.dataframe(
                display_df.style.format({
                    'Last Login': lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) else 'Never',
                    'Created': lambda x: x.strftime('%Y-%m-%d')
                }),
                use_container_width=True,
                height=400
            )

            # Inactive users
            st.markdown("---")
            st.markdown("#### Inactive Users")

            days_inactive_threshold = 90
            inactive_date = pd.Timestamp.now() - pd.Timedelta(days=days_inactive_threshold)

            inactive_users = users[
                (users['LAST_SUCCESS_LOGIN'].isna()) |
                (users['LAST_SUCCESS_LOGIN'] < inactive_date)
            ].copy()

            if not inactive_users.empty:
                create_alert_badge(
                    f"⚠️ {len(inactive_users)} user(s) inactive for >{days_inactive_threshold} days",
                    "warning"
                )

                display_df = inactive_users[['USER_NAME', 'LAST_SUCCESS_LOGIN', 'DISABLED', 'CREATED_ON']].copy()
                display_df.columns = ['User', 'Last Login', 'Disabled', 'Created']

                st.dataframe(
                    display_df.style.format({
                        'Last Login': lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) else 'Never',
                        'Created': lambda x: x.strftime('%Y-%m-%d')
                    }),
                    use_container_width=True
                )

                st.caption("**Recommendation:** Review and disable/delete inactive users to reduce security risk")

            else:
                create_alert_badge(f"✅ No users inactive for >{days_inactive_threshold} days", "success")

        # Roles
        st.markdown("---")
        st.markdown("#### Role Management")

        roles_query = """
        SELECT
            NAME AS ROLE_NAME,
            OWNER,
            COMMENT,
            CREATED_ON,
            DELETED_ON
        FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES
        WHERE DELETED_ON IS NULL
        ORDER BY NAME
        """
        roles = session.sql(roles_query).to_pandas()

        if not roles.empty:
            roles['CREATED_ON'] = pd.to_datetime(roles['CREATED_ON'])

            st.metric("Total Active Roles", len(roles))

            display_df = roles[['ROLE_NAME', 'OWNER', 'COMMENT', 'CREATED_ON']].copy()
            display_df.columns = ['Role', 'Owner', 'Comment', 'Created']

            st.dataframe(
                display_df.style.format({
                    'Created': lambda x: x.strftime('%Y-%m-%d')
                }),
                use_container_width=True,
                height=300
            )

        # Role hierarchy
        st.markdown("---")
        st.markdown("#### Role Hierarchy")

        role_grants_query = """
        SELECT
            GRANTED_TO,
            GRANTEE_NAME,
            ROLE,
            GRANTED_BY
        FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
        WHERE GRANTED_TO = 'ROLE'
        AND DELETED_ON IS NULL
        LIMIT 100
        """
        role_grants = session.sql(role_grants_query).to_pandas()

        if not role_grants.empty:
            display_df = role_grants[['GRANTEE_NAME', 'ROLE', 'GRANTED_BY']].copy()
            display_df.columns = ['Grantee Role', 'Granted Role', 'Granted By']

            st.dataframe(
                display_df,
                use_container_width=True,
                height=300
            )

        else:
            st.info("No role hierarchy data available")

    except Exception as e:
        st.error(f"Error loading user and role data: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 3: Access Control
# ----------------------------------------------------------------------------

with tab3:
    st.markdown("### 🔑 Access Control & Privileges")

    try:
        # Grants to users
        grants_to_users_query = """
        SELECT
            GRANTEE_NAME AS USER_NAME,
            ROLE,
            GRANTED_BY,
            CREATED_ON
        FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
        WHERE DELETED_ON IS NULL
        ORDER BY GRANTEE_NAME, ROLE
        """
        grants_to_users = session.sql(grants_to_users_query).to_pandas()

        if not grants_to_users.empty:
            grants_to_users['CREATED_ON'] = pd.to_datetime(grants_to_users['CREATED_ON'])

            # Summary
            unique_users_with_grants = grants_to_users['USER_NAME'].nunique()
            unique_roles_granted = grants_to_users['ROLE'].nunique()
            total_grant_count = len(grants_to_users)

            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Users with Grants", unique_users_with_grants)

            with col2:
                st.metric("Unique Roles Granted", unique_roles_granted)

            with col3:
                st.metric("Total Grants", total_grant_count)

            st.markdown("---")

            # Grants by user
            st.markdown("#### Grants by User")

            user_grant_counts = grants_to_users.groupby('USER_NAME')['ROLE'].count().reset_index()
            user_grant_counts.columns = ['USER_NAME', 'GRANT_COUNT']
            user_grant_counts = user_grant_counts.sort_values('GRANT_COUNT', ascending=False)

            fig = px.bar(
                user_grant_counts.head(20),
                x='USER_NAME',
                y='GRANT_COUNT',
                title='Top 20 Users by Number of Role Grants',
                labels={'GRANT_COUNT': 'Number of Roles', 'USER_NAME': 'User'}
            )

            fig.update_traces(marker_color='lightblue')
            st.plotly_chart(fig, use_container_width=True)

            # Detailed grants table
            st.markdown("---")
            st.markdown("#### Grant Details")

            # Filter
            selected_user = st.selectbox(
                "Filter by User",
                options=['All'] + sorted(grants_to_users['USER_NAME'].unique().tolist())
            )

            filtered_grants = grants_to_users if selected_user == 'All' else grants_to_users[grants_to_users['USER_NAME'] == selected_user]

            display_df = filtered_grants[['USER_NAME', 'ROLE', 'GRANTED_BY', 'CREATED_ON']].copy()
            display_df.columns = ['User', 'Role', 'Granted By', 'Created']

            st.dataframe(
                display_df.style.format({
                    'Created': lambda x: x.strftime('%Y-%m-%d %H:%M')
                }),
                use_container_width=True,
                height=400
            )

        else:
            st.info("No grant data available")

        # Object privileges
        st.markdown("---")
        st.markdown("#### Object Privileges")

        object_privileges_query = """
        SELECT
            GRANTEE_NAME,
            PRIVILEGE,
            TABLE_CATALOG AS DATABASE_NAME,
            TABLE_SCHEMA AS SCHEMA_NAME,
            TABLE_NAME,
            GRANTED_BY
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_PRIVILEGES
        WHERE DELETED_ON IS NULL
        ORDER BY GRANTEE_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
        LIMIT 500
        """

        try:
            object_privileges = session.sql(object_privileges_query).to_pandas()

            if not object_privileges.empty:
                # Privilege summary
                privilege_counts = object_privileges.groupby('PRIVILEGE').size().reset_index(name='COUNT')
                privilege_counts = privilege_counts.sort_values('COUNT', ascending=False)

                col1, col2 = st.columns(2)

                with col1:
                    fig = px.pie(
                        privilege_counts,
                        values='COUNT',
                        names='PRIVILEGE',
                        title='Privilege Distribution',
                        hole=0.4
                    )

                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    st.markdown("**Privilege Counts:**")

                    for _, row in privilege_counts.iterrows():
                        st.write(f"- **{row['PRIVILEGE']}**: {int(row['COUNT']):,}")

                # Top grantees
                st.markdown("---")
                st.markdown("**Top Grantees by Object Privilege Count:**")

                grantee_counts = object_privileges.groupby('GRANTEE_NAME').size().reset_index(name='PRIVILEGE_COUNT')
                grantee_counts = grantee_counts.sort_values('PRIVILEGE_COUNT', ascending=False).head(15)

                display_df = grantee_counts.copy()
                display_df.columns = ['Grantee', 'Privilege Count']

                st.dataframe(
                    display_df.style.format({'Privilege Count': '{:,}'}),
                    use_container_width=True
                )

            else:
                st.info("No object privilege data available")

        except Exception as e:
            st.warning(f"Object privileges view may not be available: {str(e)}")

    except Exception as e:
        st.error(f"Error loading access control data: {str(e)}")

# ----------------------------------------------------------------------------
# TAB 4: Security Recommendations
# ----------------------------------------------------------------------------

with tab4:
    st.markdown("### 🛡️ Security Recommendations")

    col1, col2 = st.columns([2, 1])

    with col1:
        recommendations = []

        try:
            # 1. MFA Recommendation
            if 'mfa_rate' in locals() and mfa_rate < 100:
                recommendations.append({
                    'priority': 'CRITICAL' if mfa_rate < 50 else 'HIGH',
                    'category': 'Multi-Factor Authentication',
                    'issue': f"Only {mfa_rate:.1f}% of logins use MFA",
                    'impact': "Increased risk of account compromise",
                    'action': """
                    **Enable MFA for all users:**

                    1. Set account-level policy:
                    ```sql
                    ALTER ACCOUNT SET ALLOW_CLIENT_MFA_CACHING = FALSE;
                    ```

                    2. Require MFA for sensitive roles:
                    ```sql
                    ALTER USER <username> SET MINS_TO_BYPASS_MFA = 0;
                    ```

                    3. Educate users on setting up MFA:
                    - Use Snowflake UI: User Preferences > Multi-Factor Authentication
                    - Download authenticator app (Duo Mobile, Google Authenticator)
                    - Scan QR code and save backup codes

                    **Best Practice:** Enforce MFA for ACCOUNTADMIN and SECURITYADMIN roles at minimum
                    """
                })

            # 2. Failed login recommendation
            if 'failed_logins_count' in locals() and failed_logins_count > 50:
                recommendations.append({
                    'priority': 'HIGH',
                    'category': 'Failed Login Attempts',
                    'issue': f"{failed_logins_count} failed login attempts detected",
                    'impact': "Potential brute force attack or credential issues",
                    'action': """
                    **Investigate and Secure:**

                    1. Review failed login patterns for suspicious activity
                    2. Identify users with repeated failures
                    3. Verify IP addresses of failed attempts
                    4. Consider implementing:
                    ```sql
                    -- Set network policy to restrict IP ranges
                    CREATE NETWORK POLICY my_policy
                    ALLOWED_IP_LIST = ('1.2.3.4/32', '5.6.7.8/32')
                    BLOCKED_IP_LIST = ('0.0.0.0/0');

                    ALTER ACCOUNT SET NETWORK_POLICY = my_policy;
                    ```

                    5. Enable account lockout policies
                    6. Review and rotate credentials for affected accounts
                    """
                })

            # 3. Inactive users
            if 'inactive_users' in locals() and not inactive_users.empty:
                recommendations.append({
                    'priority': 'MEDIUM',
                    'category': 'Inactive Users',
                    'issue': f"{len(inactive_users)} inactive users identified",
                    'impact': "Unnecessary attack surface",
                    'action': f"""
                    **User Lifecycle Management:**

                    1. Review inactive users (>{days_inactive_threshold} days)
                    2. Disable or delete accounts:
                    ```sql
                    -- Disable user
                    ALTER USER <username> SET DISABLED = TRUE;

                    -- Or delete user
                    DROP USER <username>;
                    ```

                    3. Implement automated user lifecycle process
                    4. Regular access reviews (quarterly recommended)
                    5. Document offboarding procedures
                    """
                })

            # 4. Role hygiene
            if 'roles' in locals() and len(roles) > 50:
                recommendations.append({
                    'priority': 'LOW',
                    'category': 'Role Proliferation',
                    'issue': f"{len(roles)} roles defined in account",
                    'impact': "Complex access management",
                    'action': """
                    **Role Governance Best Practices:**

                    1. Review and consolidate similar roles
                    2. Follow role hierarchy principles:
                    - Functional roles (read, write, admin)
                    - Business unit roles
                    - Custom roles only when necessary

                    3. Document role purposes and owners
                    4. Regular role access reviews
                    5. Use SHOW GRANTS to audit permissions:
                    ```sql
                    SHOW GRANTS TO ROLE <role_name>;
                    SHOW GRANTS ON DATABASE <db_name>;
                    ```

                    6. Implement principle of least privilege
                    """
                })

            # 5. Password vs Key authentication
            if 'users_with_pwd' in locals() and 'users_with_key' in locals():
                if users_with_pwd > users_with_key and users_with_pwd > 10:
                    recommendations.append({
                        'priority': 'MEDIUM',
                        'category': 'Authentication Methods',
                        'issue': f"{users_with_pwd} users using password auth vs {users_with_key} using key-pair",
                        'impact': "Key-pair authentication is more secure for service accounts",
                        'action': """
                        **Enhance Authentication Security:**

                        1. Use key-pair authentication for service accounts:
                        ```sql
                        -- Set RSA public key for user
                        ALTER USER <username> SET RSA_PUBLIC_KEY='MIIBIjANBg...';
                        ```

                        2. Use OAuth/SAML for human users
                        3. Disable password auth for automated processes
                        4. Implement password policies:
                        ```sql
                        ALTER ACCOUNT SET
                        MIN_PASSWORD_LENGTH = 12
                        MIN_PASSWORD_SPECIAL_CHARS = 1
                        MIN_PASSWORD_NUMERIC_CHARS = 1
                        PASSWORD_HISTORY = 5
                        PASSWORD_MAX_AGE_DAYS = 90;
                        ```
                        """
                    })

            # Display recommendations
            if recommendations:
                for i, rec in enumerate(recommendations, 1):
                    priority_colors = {
                        'CRITICAL': '🔴',
                        'HIGH': '🟠',
                        'MEDIUM': '🟡',
                        'LOW': '🟢'
                    }

                    with st.expander(
                        f"{priority_colors.get(rec['priority'], '🔵')} {rec['category']} - {rec['issue']}",
                        expanded=(rec['priority'] in ['CRITICAL', 'HIGH'])
                    ):
                        st.markdown(f"**Priority:** {rec['priority']}")
                        st.markdown(f"**Issue:** {rec['issue']}")
                        st.markdown(f"**Impact:** {rec['impact']}")
                        st.markdown(f"**Recommended Actions:**")
                        st.info(rec['action'])

            else:
                create_alert_badge("✅ No critical security issues identified", "success")

        except Exception as e:
            st.error(f"Error generating recommendations: {str(e)}")

    with col2:
        st.markdown("#### 🛡️ Security Checklist")

        st.markdown("""
        **Essential Security Controls:**

        - ✅ MFA enabled for all users
        - ✅ Network policies configured
        - ✅ Inactive users disabled
        - ✅ Regular access reviews
        - ✅ Password policies enforced
        - ✅ Key rotation schedules
        - ✅ Audit logging enabled
        - ✅ Role-based access control
        - ✅ Least privilege principle
        - ✅ Service account management

        **Compliance & Governance:**

        - 📋 Document access policies
        - 📋 Regular security audits
        - 📋 Incident response plan
        - 📋 Data classification
        - 📋 Privacy controls
        - 📋 Change management
        """)

        st.markdown("---")
        st.markdown("#### 📚 Resources")

        st.markdown("""
        **Snowflake Security:**

        - [Security Best Practices](https://docs.snowflake.com/en/user-guide/admin-security-best-practices.html)
        - [Access Control](https://docs.snowflake.com/en/user-guide/security-access-control.html)
        - [Network Policies](https://docs.snowflake.com/en/user-guide/network-policies.html)
        - [Key-Pair Authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth.html)
        """)

# Footer
st.markdown("---")
st.caption(f"📅 Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | ⏱️ Time period: {time_period} days")
