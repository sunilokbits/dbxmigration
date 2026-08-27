"""Convert blueprint — SP/View/UDF listing and PySpark conversion."""
from flask import Blueprint, request, jsonify

from .auth import login_required
from log_config import get_logger
from stored_procedures import STORED_PROCEDURES, ALL_OBJECTS
from sp_converter import get_pyspark_code, get_combined_pyspark_code, get_separate_pyspark_codes
from ai_converter import ai_convert

logger = get_logger(__name__)
convert_bp = Blueprint("convert", __name__, url_prefix="/api/v1")


@convert_bp.route("/stored-procedures", methods=["GET"])
@login_required
def list_stored_procedures():
    procedures = [
        {"name": sp["name"], "description": sp["description"]}
        for sp in STORED_PROCEDURES.values()
    ]
    return jsonify({"success": True, "procedures": procedures})


@convert_bp.route("/sp-code/<sp_name>", methods=["GET"])
@login_required
def get_sp_code(sp_name):
    sp = STORED_PROCEDURES.get(sp_name)
    if not sp:
        return jsonify({"success": False, "error": f"SP '{sp_name}' not found"}), 404
    return jsonify({
        "success": True, "name": sp["name"],
        "description": sp["description"], "code": sp["code"].strip()
    })


@convert_bp.route("/convert", methods=["POST"])
@login_required
def convert_to_pyspark():
    try:
        data = request.get_json()
        sp_name = data.get("sp_name", "").strip()
        if not sp_name:
            return jsonify({"success": False, "error": "sp_name is required"}), 400
        return jsonify(get_pyspark_code(sp_name))
    except Exception as e:
        logger.exception("Conversion failed for SP '%s'", sp_name)
        return jsonify({"success": False, "error": str(e)}), 500


@convert_bp.route("/all-objects", methods=["GET"])
@login_required
def list_all_objects():
    grouped = {"stored_procedure": [], "view": [], "udf": []}
    for key, obj in ALL_OBJECTS.items():
        otype = obj.get("object_type", "stored_procedure")
        grouped[otype].append({
            "key": key, "name": obj.get("name", key),
            "description": obj.get("description", ""),
            "object_type": otype
        })
    return jsonify({"success": True, "grouped": grouped, "total": len(ALL_OBJECTS)})


@convert_bp.route("/object-code/<obj_name>", methods=["GET"])
@login_required
def get_object_code(obj_name):
    obj = ALL_OBJECTS.get(obj_name)
    if not obj:
        return jsonify({"success": False, "error": f"Object '{obj_name}' not found"}), 404
    return jsonify({
        "success": True, "name": obj.get("name", obj_name),
        "description": obj.get("description", ""),
        "object_type": obj.get("object_type", ""),
        "code": obj.get("code", "").strip()
    })


@convert_bp.route("/convert-multi", methods=["POST"])
@login_required
def convert_multi():
    try:
        data = request.get_json()
        object_names = data.get("object_names", [])
        if not object_names:
            return jsonify({"success": False, "error": "object_names list is required"}), 400
        return jsonify(get_combined_pyspark_code(object_names))
    except Exception as e:
        logger.exception("Multi-convert failed")
        return jsonify({"success": False, "error": str(e)}), 500


@convert_bp.route("/convert-separate", methods=["POST"])
@login_required
def convert_separate():
    try:
        data = request.get_json()
        object_names = data.get("object_names", [])
        objects_with_code = data.get("objects_with_code", {})
        if not object_names:
            return jsonify({"success": False, "error": "object_names list is required"}), 400
        return jsonify(get_separate_pyspark_codes(object_names, objects_with_code))
    except Exception as e:
        logger.exception("Separate-convert failed")
        return jsonify({"success": False, "error": str(e)}), 500


@convert_bp.route("/convert/ai-convert", methods=["POST"])
@login_required
def ai_convert_endpoint():
    """AI-powered SQL to PySpark conversion using LLM."""
    data = None
    try:
        data = request.get_json()
        obj_name = (data.get("name") or "").strip()
        model = (data.get("model") or "databricks-claude-opus-4-7").strip()
        if not obj_name:
            return jsonify({"success": False, "error": "name is required"}), 400

        # Get SQL code from ALL_OBJECTS
        obj = ALL_OBJECTS.get(obj_name)
        sql_code = ""
        object_type = "stored_procedure"
        if obj:
            sql_code = obj.get("code", "")
            object_type = obj.get("object_type", "stored_procedure")
        else:
            sql_code = (data.get("code") or "").strip()
            object_type = (data.get("object_type") or "stored_procedure").strip()

        if not sql_code:
            return jsonify({"success": False, "error": "No SQL code found for '" + obj_name + "'"}), 404

        result = ai_convert(obj_name, object_type, sql_code, model=model)
        return jsonify(result)
    except Exception as e:
        logger.exception("AI convert failed")
        return jsonify({"success": False, "error": str(e)}), 500
