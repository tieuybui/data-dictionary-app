"""
Password protection with multi-user support.
Users are defined in st.secrets["users"] as email = password pairs.
Login is persisted to browser LocalStorage.
"""

import hashlib
import hmac
import json
import streamlit as st
from streamlit_local_storage import LocalStorage

_ls = LocalStorage()
_LS_KEY = "dd_auth"


def _make_token(username: str, password: str) -> str:
    return hashlib.sha256(f"{username}:{password}".encode()).hexdigest()


def check_password():
    """Block access unless user provides valid credentials."""

    if "users" not in st.secrets:
        return True

    users: dict = dict(st.secrets["users"])

    if st.session_state.get("authenticated"):
        return True

    # Check LocalStorage for saved token
    saved = _ls.getItem(_LS_KEY)
    if saved:
        try:
            data = json.loads(saved) if isinstance(saved, str) else saved
            saved_user = data.get("user", "")
            saved_token = data.get("token", "")
            expected_pw = users.get(saved_user)
            if expected_pw and saved_token == _make_token(saved_user, expected_pw):
                st.session_state["authenticated"] = True
                st.session_state["username"] = saved_user
                return True
        except (json.JSONDecodeError, AttributeError):
            pass

    def _on_submit():
        email = st.session_state.get("_email_input", "").strip().lower()
        pw = st.session_state.get("_password_input", "")
        expected_pw = users.get(email)

        if expected_pw and hmac.compare_digest(pw, expected_pw):
            st.session_state["authenticated"] = True
            st.session_state["username"] = email
            _ls.setItem(_LS_KEY, json.dumps({
                "user": email,
                "token": _make_token(email, pw),
            }))
        else:
            st.session_state["_login_error"] = True

    st.set_page_config(page_title="Login", page_icon="🔒")

    st.title("🔒 Data Dictionary")
    st.text_input("Email", key="_email_input", placeholder="you@example.com")
    st.text_input("Password", type="password", key="_password_input")
    st.button("Login", on_click=_on_submit)

    if st.session_state.get("_login_error"):
        st.error("Invalid email or password.")

    st.stop()


def logout():
    """Clear auth state and LocalStorage."""
    st.session_state.pop("authenticated", None)
    st.session_state.pop("username", None)
    # Overwrite with empty string to invalidate token
    # (deleteItem fails because server-side dict resets on rerun)
    _ls.setItem(_LS_KEY, "")
