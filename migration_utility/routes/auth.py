"""Auth blueprint — Databricks App identity (proxy-authenticated).

All 14 blueprints import `login_required` from this module.  The new
decorator extracts user identity from Databricks proxy headers / SDK
and populates `session["user"]`, `session["role"]`, `session["display_name"]`
for full backward compatibility with existing route code.
"""
from flask import Blueprint, request, jsonify, session, redirect, g
from functools import wraps

from identity import get_current_user, clear_user_cache
from log_config import get_logger

logger = get_logger(__name__)

auth_bp = Blueprint("auth", __name__)


def login_required(f):
    """Require authenticated Databricks user.  Populates session for backward compat."""
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            if request.path.startswith("/api/"):
                return jsonify({"success": False, "error": "Authentication required — access this app through Databricks."}), 401
            return jsonify({"success": False, "error": "Not authenticated"}), 401

        # Backward compat: populate session keys used by admin.py, databricks.py, datamodel.py, etc.
        session["user"] = user["email"]
        session["role"] = user["role"]
        session["display_name"] = user["display_name"]
        g.user = user
        return f(*args, **kwargs)
    return decorated


def _admin_required(f):
    """Require Admin role.  Must be used after @login_required."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get("role") != "Admin":
            return jsonify({"success": False, "error": "Admin access required"}), 403
        return f(*args, **kwargs)
    return decorated


# ── Routes ────────────────────────────────────────────────────────────────────

@auth_bp.route("/login", methods=["GET", "POST"])
def login_page():
    """No login page needed — redirect to main app (proxy handles auth)."""
    return redirect("/")


@auth_bp.route("/logout")
def logout():
    """Clear local session and redirect to Databricks workspace."""
    user_email = session.get("user")
    session.clear()
    if user_email:
        clear_user_cache(user_email)
    return redirect("/")


@auth_bp.route("/api/v1/auth/me")
@login_required
def auth_me():
    """Return current user identity from Databricks proxy."""
    user = get_current_user()
    return jsonify({
        "user": user["email"],
        "role": user["role"],
        "display_name": user["display_name"],
        "groups": user.get("groups", []),
        "user_id": user.get("user_id", ""),
    })
