"""Admin blueprint — user role management via Delta tables.

Replaces the file-based users.json with a Delta table (user_roles)
for role assignments. Authentication is handled by Databricks proxy;
this module manages app-level role assignments (Admin/Developer/Viewer).
"""
from flask import Blueprint, request, jsonify, session
import re
from functools import wraps

from .auth import login_required
from log_config import get_logger
from dbsql_client import execute_query, execute_write, get_catalog_schema
from audit import log_action

logger = get_logger(__name__)
admin_bp = Blueprint("admin", __name__, url_prefix="/api/v1/admin")

VALID_ROLES = ("Admin", "Developer", "Viewer")
_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


def _fqn(table: str) -> str:
    catalog, schema = get_catalog_schema()
    return f"{catalog}.{schema}.{table}"


def _admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get("role") != "Admin":
            return jsonify({"success": False, "error": "Admin access required"}), 403
        return f(*args, **kwargs)
    return decorated


@admin_bp.route("/users", methods=["GET"])
@login_required
@_admin_required
def list_users():
    rows = execute_query(
        f"SELECT user_email, display_name, role, updated_at FROM {_fqn('user_roles')} ORDER BY user_email"
    )
    users = [
        {
            "username": r["user_email"],
            "display_name": r["display_name"] or r["user_email"].split("@")[0].title(),
            "role": r["role"],
            "updated_at": str(r["updated_at"]) if r["updated_at"] else None,
        }
        for r in rows
    ]
    return jsonify({"success": True, "users": users})


@admin_bp.route("/users", methods=["POST"])
@login_required
@_admin_required
def create_user():
    data = request.get_json(silent=True) or {}
    email = data.get("username", "").strip().lower()
    display_name = data.get("display_name", "").strip()
    role = data.get("role", "Viewer")

    if not email or not _EMAIL_RE.match(email):
        return jsonify({"success": False, "error": "Valid email address required"}), 400
    if role not in VALID_ROLES:
        return jsonify({"success": False, "error": f"Role must be one of: {', '.join(VALID_ROLES)}"}), 400
    if not display_name:
        display_name = email.split("@")[0].replace(".", " ").title()

    existing = execute_query(
        f"SELECT user_email FROM {_fqn('user_roles')} WHERE user_email = %(email)s",
        {"email": email},
    )
    if existing:
        return jsonify({"success": False, "error": f"User '{email}' already has a role assignment"}), 409

    admin_email = session.get("user", "system")
    execute_write(
        f"""INSERT INTO {_fqn('user_roles')}
            (user_email, role, display_name, assigned_by, updated_at)
            VALUES (%(email)s, %(role)s, %(dn)s, %(admin)s, current_timestamp())""",
        {"email": email, "role": role, "dn": display_name, "admin": admin_email},
    )

    log_action("user_role_created", "user", email, {"role": role})
    logger.info("Role '%s' assigned to '%s' by '%s'", role, email, admin_email)
    return jsonify({"success": True, "username": email, "role": role, "display_name": display_name}), 201


@admin_bp.route("/users/<path:user_email>", methods=["PUT"])
@login_required
@_admin_required
def update_user(user_email):
    user_email = user_email.strip().lower()
    data = request.get_json(silent=True) or {}

    existing = execute_query(
        f"SELECT role FROM {_fqn('user_roles')} WHERE user_email = %(email)s",
        {"email": user_email},
    )
    if not existing:
        return jsonify({"success": False, "error": f"User '{user_email}' not found"}), 404

    new_role = data.get("role")
    if new_role and new_role not in VALID_ROLES:
        return jsonify({"success": False, "error": f"Role must be one of: {', '.join(VALID_ROLES)}"}), 400

    if new_role and new_role != "Admin" and existing[0]["role"] == "Admin":
        admin_count = execute_query(
            f"SELECT COUNT(*) AS cnt FROM {_fqn('user_roles')} WHERE role = 'Admin'"
        )
        if admin_count and admin_count[0]["cnt"] <= 1:
            return jsonify({"success": False, "error": "Cannot remove the last Admin. Promote another user first."}), 400

    updates = []
    params = {"email": user_email, "admin": session.get("user", "system")}
    if new_role:
        updates.append("role = %(role)s")
        params["role"] = new_role
    if "display_name" in data and data["display_name"].strip():
        updates.append("display_name = %(dn)s")
        params["dn"] = data["display_name"].strip()

    if updates:
        updates.append("assigned_by = %(admin)s")
        updates.append("updated_at = current_timestamp()")
        execute_write(
            f"UPDATE {_fqn('user_roles')} SET {', '.join(updates)} WHERE user_email = %(email)s",
            params,
        )

    log_action("user_role_updated", "user", user_email, data)
    logger.info("User '%s' updated by '%s'", user_email, session.get("user"))
    return jsonify({"success": True, "username": user_email, "role": new_role or existing[0]["role"]})


@admin_bp.route("/users/<path:user_email>", methods=["DELETE"])
@login_required
@_admin_required
def delete_user(user_email):
    user_email = user_email.strip().lower()

    if user_email == session.get("user"):
        return jsonify({"success": False, "error": "Cannot remove your own role assignment"}), 400

    existing = execute_query(
        f"SELECT role FROM {_fqn('user_roles')} WHERE user_email = %(email)s",
        {"email": user_email},
    )
    if not existing:
        return jsonify({"success": False, "error": f"User '{user_email}' not found"}), 404

    if existing[0]["role"] == "Admin":
        admin_count = execute_query(
            f"SELECT COUNT(*) AS cnt FROM {_fqn('user_roles')} WHERE role = 'Admin'"
        )
        if admin_count and admin_count[0]["cnt"] <= 1:
            return jsonify({"success": False, "error": "Cannot remove the last Admin"}), 400

    execute_write(
        f"DELETE FROM {_fqn('user_roles')} WHERE user_email = %(email)s",
        {"email": user_email},
    )

    log_action("user_role_deleted", "user", user_email)
    logger.info("User '%s' role removed by '%s'", user_email, session.get("user"))
    return jsonify({"success": True})


@admin_bp.route("/roles", methods=["GET"])
@login_required
@_admin_required
def list_roles():
    return jsonify({
        "success": True,
        "roles": [
            {"name": "Admin", "description": "Full access including user management"},
            {"name": "Developer", "description": "Access to migration and development tools"},
            {"name": "Viewer", "description": "Read-only access to dashboards and reports"},
        ],
    })
