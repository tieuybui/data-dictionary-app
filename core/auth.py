"""
Simple password protection with signed token persistence.
Uses st.secrets["username"] and st.secrets["password"].
"""

import hashlib
import hmac
import streamlit as st

_LS_KEY = "dd_auth"


def _sign(username: str) -> str:
    """Create HMAC-SHA256 signature for the username using the app password as secret."""
    secret = st.secrets["password"].encode()
    return hmac.new(secret, username.encode(), hashlib.sha256).hexdigest()


def _make_token(username: str) -> str:
    """Build a signed token: 'username|signature'."""
    return f"{username}|{_sign(username)}"


def _verify_token(token: str) -> bool:
    """Verify a signed token from localStorage."""
    if not token or "|" not in token:
        return False
    username, sig = token.rsplit("|", 1)
    return hmac.compare_digest(sig, _sign(username))


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
        # Just logged in — save signed token to LocalStorage
        if st.session_state.pop("_just_logged_in", False):
            ls.setItem(_LS_KEY, _make_token(st.secrets["username"]))
        return

    # Try restore from LocalStorage — only accept valid signed tokens
    saved = ls.getItem(_LS_KEY)
    if saved and _verify_token(saved):
        st.session_state["authenticated"] = True
