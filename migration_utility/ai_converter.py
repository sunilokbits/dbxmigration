"""AI-Powered SQL to PySpark Converter — LLM-based with regex seed."""
import ast
import re
import json
import os
from databricks.sdk import WorkspaceClient


_SYSTEM_PROMPT = """You are an expert SQL Server to PySpark migration engineer for Databricks.

RULES:
1. Convert T-SQL to pure PySpark DataFrame API code. Avoid spark.sql() for core logic.
2. Use fully-qualified 3-part Unity Catalog names: `{catalog}`.`{schema}`.`table`
3. Use dbutils.widgets for parameters.
4. Include print() logging for key steps.
5. Use F.* functions (from pyspark.sql import functions as F).
6. Map CTEs to chained DataFrames, cursors to Window/UDF, MERGE to DeltaTable API.
7. Add error handling with try/except.
8. Return ONLY Python code in a ```python code block.
9. Include a header comment with source object name and conversion date.
10. Add .cache() for DataFrames reused more than once.

T-SQL → PySpark Mappings:
- ISNULL(a,b) → F.coalesce(F.col('a'), F.lit(b))
- GETDATE() → F.current_timestamp()
- DATEDIFF(day,a,b) → F.datediff(F.col('b'), F.col('a'))
- DATEADD(day,n,d) → F.date_add(F.col('d'), n)
- LEN(x) → F.length(F.col('x'))
- CAST(x AS type) → F.col('x').cast('type')
- LEFT(x,n) → F.col('x').substr(1, n)
- IIF(cond,a,b) → F.when(cond, a).otherwise(b)
- STRING_AGG(x,sep) → F.concat_ws(sep, F.collect_list('x'))
- NOLOCK hints → remove (not needed in Spark)
- #TempTable → cached DataFrame variable
- CURSOR → DataFrame operations (Window, groupBy, UDF)
"""


def ai_convert(name: str, object_type: str, sql_code: str, model: str = "databricks-claude-opus-4-7") -> dict:
    """
    Convert a SQL object to PySpark using an LLM.
    Uses the existing regex converter as a seed/hint for the LLM.
    """
    if not sql_code or not sql_code.strip():
        return {"success": False, "error": "No SQL code provided"}

    # Get regex seed from existing converter (best-effort)
    seed_code = ""
    try:
        from sp_converter import sql_to_pyspark_auto
        seed_result = sql_to_pyspark_auto(name, object_type, sql_code)
        if isinstance(seed_result, tuple):
            seed_code = seed_result[0]  # (code, notes)
        elif isinstance(seed_result, str):
            seed_code = seed_result
    except Exception:
        pass

    # Build user message
    user_msg = f"Convert this {object_type} named '{name}' from T-SQL to PySpark DataFrame API:\n\n```sql\n{sql_code[:6000]}\n```"
    if seed_code:
        user_msg += f"\n\n[HINT] Here is a regex-based auto-conversion for reference (may have gaps):\n```python\n{seed_code[:3000]}\n```\nImprove this conversion: fix any errors, handle edge cases, add proper error handling."

    # Call serving endpoint
    try:
        from config_cache import get_config, get_databricks_token
        host = (get_config().get("databricks_host") or "").strip().rstrip("/")
        token = get_databricks_token()
        payload = {
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_msg}
            ],
            "max_tokens": 6144
        }
        if host and token:
            # Databricks Apps always injects DATABRICKS_CLIENT_ID/SECRET, so
            # WorkspaceClient(token=...) dies with "more than one authorization
            # method configured: oauth and pat". Call the endpoint with the PAT
            # directly instead (same approach as databricks_connector).
            import requests as _rq
            _r = _rq.post(
                f"{host}/serving-endpoints/{model}/invocations",
                headers={"Authorization": f"Bearer {token}",
                         "Content-Type": "application/json"},
                json=payload, timeout=300,
            )
            _r.raise_for_status()
            resp = _r.json()
        else:
            w = WorkspaceClient()
            raw = w.api_client.do("POST", f"/serving-endpoints/{model}/invocations", body=payload)
            resp = json.loads(raw.content) if hasattr(raw, "content") else raw
        choices = resp.get("choices", [])

        # Handle array content (reasoning models)
        raw_content = choices[0].get("message", {}).get("content", "") if choices else ""
        if isinstance(raw_content, list):
            response_text = "\n".join(
                item.get("text", "") for item in raw_content
                if isinstance(item, dict) and item.get("type") == "text"
            ) or str(raw_content)
        else:
            response_text = raw_content or "No response"

        # Extract code block from response
        pyspark_code = _extract_code(response_text)

        # Validate syntax
        validation = _validate_python(pyspark_code)

        # Token usage
        usage = resp.get("usage", {})

        return {
            "success": True,
            "name": name,
            "object_type": object_type,
            "model": model,
            "pyspark_code": pyspark_code,
            "explanation": _extract_explanation(response_text),
            "valid_syntax": validation["valid"],
            "syntax_errors": validation.get("errors", []),
            "usage": {
                "prompt_tokens": usage.get("prompt_tokens", 0),
                "completion_tokens": usage.get("completion_tokens", 0),
                "total_tokens": usage.get("total_tokens", 0),
            },
            "had_seed": bool(seed_code),
        }
    except Exception as exc:
        return {"success": False, "error": str(exc), "name": name}


def _extract_code(text: str) -> str:
    """Extract python code block from LLM response."""
    # Look for ```python ... ``` blocks
    pattern = r"```(?:python)?\s*\n(.*?)```"
    matches = re.findall(pattern, text, re.DOTALL)
    if matches:
        # Return the longest code block (main conversion)
        return max(matches, key=len).strip()
    # Fallback: if no code block, return full text
    return text.strip()


def _extract_explanation(text: str) -> str:
    """Extract explanation text (non-code portions) from response."""
    # Remove code blocks
    cleaned = re.sub(r"```(?:python)?\s*\n.*?```", "", text, flags=re.DOTALL)
    lines = [l.strip() for l in cleaned.strip().splitlines() if l.strip()]
    return "\n".join(lines[:10])  # First 10 lines of explanation


def _validate_python(code: str) -> dict:
    """Validate Python/PySpark syntax using ast.parse()."""
    try:
        ast.parse(code)
        return {"valid": True}
    except SyntaxError as e:
        return {
            "valid": False,
            "errors": [f"Line {e.lineno}: {e.msg}"]
        }
