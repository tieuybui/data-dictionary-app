"""
Password protection with multi-user support.
Users are defined in st.secrets["users"] as email = password pairs.
Login persisted to browser LocalStorage.
"""

import hashlib
import hmac
import json
import streamlit as st
from streamlit_local_storage import LocalStorage

_LS_KEY = "dd_auth"


def _make_token(email: str, password: str) -> str:
    return hashlib.sha256(f"{email}:{password}".encode()).hexdigest()


def _get_users() -> dict:
    return {str(k).lower(): str(v) for k, v in st.secrets["users"].items()}


def check_password():
    """Block access unless user provides valid credentials."""

    if "users" not in st.secrets:
        return True

    if st.session_state.get("authenticated"):
        # Save pending token to LocalStorage after login
        if "_pending_token" in st.session_state:
            _ls = LocalStorage()
            _ls.setItem(_LS_KEY, st.session_state.pop("_pending_token"))
        return True

    # Try auto-login from LocalStorage
    _ls = LocalStorage()
    saved = _ls.getItem(_LS_KEY)
    if saved:
        try:
            data = json.loads(saved) if isinstance(saved, str) else saved
            saved_email = data.get("user", "")
            saved_token = data.get("token", "")
            users = _get_users()
            expected_pw = users.get(saved_email, "")
            if expected_pw and saved_token == _make_token(saved_email, expected_pw):
                st.session_state["authenticated"] = True
                st.session_state["username"] = saved_email
                return True
        except (json.JSONDecodeError, AttributeError):
            pass

    # Show login page
    st.set_page_config(page_title="Login", page_icon="🔒")
    st.title("🔒 Data Dictionary")

    st.text_input("Email", placeholder="you@example.com", key="_email_input")
    st.text_input("Password", type="password", key="_pw_input")

    if st.button("Login"):
        email = st.session_state.get("_email_input", "").strip().lower()
        pw = st.session_state.get("_pw_input", "")
        users = _get_users()
        expected_pw = users.get(email, "")

        if expected_pw and hmac.compare_digest(pw, expected_pw):
            st.session_state["authenticated"] = True
            st.session_state["username"] = email
            # Queue token to be saved on next rerun (after authenticated)
            st.session_state["_pending_token"] = json.dumps({
                "user": email,
                "token": _make_token(email, pw),
            })
            st.rerun()
        else:
            st.error("Invalid email or password.")

    st.stop()
