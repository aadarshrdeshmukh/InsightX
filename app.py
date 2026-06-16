"""
InsightX — Real-Time Analytics Dashboard
=========================================
Brutalist B&W edition.
"""

import os
import streamlit as st

from config import APP_NAME, VERSION


# ── Navigation ────────────────────────────────────────────────────────────────
_NAV = [
    ("Home",           "home"),
    ("Simulation",     "simulation"),
    ("Results",        "results"),
    ("Architecture",   "architecture"),
]


def _inject_css():
    css_path = os.path.join(os.path.dirname(__file__), "assets", "style.css")
    if os.path.exists(css_path):
        with open(css_path) as fh:
            st.markdown(f"<style>{fh.read()}</style>", unsafe_allow_html=True)


def _init_session_state():
    defaults = {
        "current_page": "home",
        "sim_engine": None,
        "sim_running": False,
        "sim_complete": False,
        "sim_results": None,
        "sim_config": {
            "window_size": 5,
            "duration": 30,
            "interval_ms": 50,
        },
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _render_sidebar():
    current = st.session_state.get("current_page", "home")

    with st.sidebar:
        # ── Branding ──────────────────────────────────────────────────────
        st.markdown(
            f"""
            <div style="padding: 0.5rem 0 0.4rem; border-bottom: 3px solid #000;
                        margin-bottom: 1rem;">
                <div style="font-family: 'Space Mono', monospace; font-weight: 700;
                            font-size: 1.1rem; color: #000; text-transform: uppercase;
                            letter-spacing: 2px;">
                    {APP_NAME}
                </div>
                <div style="font-family: 'Space Mono', monospace; font-size: 0.65rem;
                            color: #888; text-transform: uppercase; letter-spacing: 1.5px;
                            margin-top: 2px;">
                    Real-Time Analytics
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # ── Navigation ────────────────────────────────────────────────────
        for label, page_key in _NAV:
            is_active = current == page_key
            btn_type = "primary" if is_active else "secondary"
            if st.button(
                label,
                use_container_width=True,
                key=f"nav_{page_key}",
                type=btn_type,
            ):
                st.session_state.current_page = page_key
                st.rerun()

        st.divider()

        # ── Simulation Status ─────────────────────────────────────────────
        if st.session_state.get("sim_complete"):
            results = st.session_state.get("sim_results", {})
            st.markdown(
                f"""
                <div class="br-badge">
                    <div class="br-badge-label">Last Run</div>
                    <div class="br-badge-value">
                        {results.get('total_consumed', 0):,}
                    </div>
                    <div class="br-badge-sub">
                        {results.get('windows_captured', 0)} windows
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.divider()

        # ── Version ───────────────────────────────────────────────────────
        st.markdown(
            f'<div style="font-family: Space Mono, monospace; font-size: 0.6rem; '
            f'color: #ccc; text-transform: uppercase; letter-spacing: 1px; '
            f'text-align: center; padding: 0 0 0.5rem;">'
            f'v{VERSION}</div>',
            unsafe_allow_html=True,
        )


def _render_page():
    page = st.session_state.get("current_page", "home")

    if page == "home":
        from ui.home_page import show_home_page
        show_home_page()
    elif page == "simulation":
        from ui.simulation_page import show_simulation_page
        show_simulation_page()
    elif page == "results":
        from ui.results_page import show_results_page
        show_results_page()
    elif page == "architecture":
        from ui.architecture_page import show_architecture_page
        show_architecture_page()
    else:
        from ui.home_page import show_home_page
        show_home_page()


def main():
    st.set_page_config(
        page_title=f"{APP_NAME} — Real-Time Analytics",
        page_icon="■",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    _inject_css()
    _init_session_state()
    _render_sidebar()
    _render_page()


if __name__ == "__main__":
    main()
