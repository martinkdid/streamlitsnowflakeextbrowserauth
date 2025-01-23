from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import streamlit as st

@st.cache_resource(show_spinner="Connecting to Snowflake...")
def get_session():
    """
    Get a Snowflake session. 
    If an active session is available, return it. Otherwise, create a new session.

    Returns:
        Session: The Snowflake session object.
    """
    try:
        return get_active_session()
    except:
        # Create a new Snowflake session with externalbrowser authentication
        return Session.builder.configs({
            "account": st.secrets.snowflake["account"],
            "user": st.secrets.snowflake["user"],
            "authenticator": "externalbrowser",  # Use external browser for SSO
            "database": st.secrets.snowflake["database"],
            "schema": st.secrets.snowflake["schema"],
        }).create()

# Get Snowflake session
session = get_session()

# Test the session
try:
    st.success("Connected to Snowflake successfully!")
    result = session.sql("SELECT CURRENT_USER(), CURRENT_ROLE()").collect()
    st.write(f"Current User: {result[0][0]}")
    st.write(f"Current Role: {result[0][1]}")
except Exception as e:
    st.error(f"Failed to connect: {e}")
