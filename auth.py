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
    email = st.text_input("Email", placeholder="you@example.com")
    pw = st.text_input("Password", type="password")

    if st.button("Login"):
        email = email.strip().lower()
        users = dict(st.secrets["users"])
        # Ensure string comparison
        expected_pw = str(users.get(email, ""))

        if expected_pw and hmac.compare_digest(pw, expected_pw):
            st.session_state["authenticated"] = True
            st.session_state["username"] = email
            st.rerun()
        else:
            st.error("Invalid email or password.")

    st.stop()
