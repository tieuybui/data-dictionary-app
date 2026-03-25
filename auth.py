"""
Password protection with multi-user support.
Users are defined in st.secrets["users"] as email = password pairs.
Login persisted to browser LocalStorage.
"""

import hashlib
import hmac
import json
import streamlit as st
import streamlit.components.v1 as components

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
        return True

    # Try auto-login from LocalStorage (set by JS on previous page load)
    ls_value = st.session_state.get("_ls_auth", "")
    if ls_value:
        try:
            data = json.loads(ls_value) if isinstance(ls_value, str) else ls_value
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

    # JS: read LocalStorage → put into hidden input → triggers rerun with value
    components.html(
        f"""<script>
        const data = localStorage.getItem("{_LS_KEY}");
        if (data) {{
            const doc = window.parent.document;
            const el = doc.querySelector('input[aria-label="_ls_auth"]');
            if (el && !el.value) {{
                const set = Object.getOwnPropertyDescriptor(
                    HTMLInputElement.prototype, 'value').set;
                set.call(el, data);
                el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                setTimeout(() => {{
                    el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                }}, 100);
            }}
        }}
        </script>""",
        height=0,
    )

    # Hidden input to receive LocalStorage data
    st.text_input("_ls_auth", key="_ls_auth", label_visibility="collapsed")

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
            # Save token to LocalStorage via JS
            token_data = json.dumps({"user": email, "token": _make_token(email, pw)})
            components.html(
                f"""<script>
                localStorage.setItem("{_LS_KEY}", {json.dumps(token_data)});
                </script>""",
                height=0,
            )
            st.rerun()
        else:
            st.error("Invalid email or password.")

    st.stop()
