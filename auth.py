"""
Simple password protection.
Uses st.secrets["username"] and st.secrets["password"].
"""

import hmac
import streamlit as st

_LS_KEY = "dd_auth"


def check_password():
    if "password" not in st.secrets:
        return True

    if st.session_state.get("authenticated"):
        return True

    st.set_page_config(page_title="Login", page_icon="🔒")
    st.title("🔒 Data Dictionary")

    with st.form("login_form"):
        user = st.text_input("Username")
        pw = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")

    if submitted:
        if (
            hmac.compare_digest(user, st.secrets["username"])
            and hmac.compare_digest(pw, st.secrets["password"])
        ):
            st.session_state["authenticated"] = True
            st.session_state["_just_logged_in"] = True
            st.rerun()
        else:
            st.error("Invalid username or password.")

    st.stop()


def restore_auth(ls):
    """Call after LocalStorage is created in main app to restore/save auth."""
    if st.session_state.get("authenticated"):
        # Just logged in — save to LocalStorage
        if st.session_state.pop("_just_logged_in", False):
            ls.setItem(_LS_KEY, "1")
        return

    # Try restore from LocalStorage
    saved = ls.getItem(_LS_KEY)
    if saved == "1":
        st.session_state["authenticated"] = True
