"""
Password protection with multi-user support.
Users are defined in st.secrets["users"] as email = password pairs.
"""

import hmac
import streamlit as st


def check_password():
    """Block access unless user provides valid credentials."""

    if "users" not in st.secrets:
        return True

    if st.session_state.get("authenticated"):
        return True

    st.set_page_config(page_title="Login", page_icon="🔒")

    st.title("🔒 Data Dictionary")
    st.text_input("Email", placeholder="you@example.com", key="_email_input")
    st.text_input("Password", type="password", key="_pw_input")

    if st.button("Login"):
        email = st.session_state.get("_email_input", "").strip().lower()
        pw = st.session_state.get("_pw_input", "")
        # Normalize keys to lowercase for case-insensitive email match
        users = {str(k).lower(): str(v) for k, v in st.secrets["users"].items()}
        expected_pw = users.get(email, "")

        if expected_pw and hmac.compare_digest(pw, expected_pw):
            st.session_state["authenticated"] = True
            st.session_state["username"] = email
            st.rerun()
        else:
            st.error("Invalid email or password.")

    st.stop()
