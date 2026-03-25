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
        # Normalize keys to lowercase for case-insensitive email match
        users = {str(k).lower(): str(v) for k, v in st.secrets["users"].items()}
        expected_pw = users.get(email, "")

        # Debug: show parsed keys (remove after testing)
        st.write("Parsed users keys:", list(users.keys()))
        st.write("Input email:", repr(email))
        st.write("Match found:", repr(expected_pw))

        if expected_pw and hmac.compare_digest(pw, expected_pw):
            st.session_state["authenticated"] = True
            st.session_state["username"] = email
            st.rerun()
        else:
            st.error("Invalid email or password.")

    st.stop()
