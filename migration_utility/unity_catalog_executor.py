"""
Unity Catalog Table Executor
Validates, previews, and executes PySpark notebooks targeting Unity Catalog tables.
Uses Databricks SDK + SQL Statement Execution API.
"""

import os
import time
from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    StatementState,
)
from log_config import get_logger

logger = get_logger(__name__)


def _is_databricks_app() -> bool:
    return bool(os.environ.get("DATABRICKS_CLIENT_ID") and os.environ.get("DATABRICKS_CLIENT_SECRET"))


class UnityCatalogExecutor:
    """Execute and validate tables in Databricks Unity Catalog."""

    POLL_INTERVAL = 3
    MAX_POLLS = 40

    def __init__(self, host: str, token: str, catalog: str = "main", schema: str = "default"):
        self.host = host.rstrip("/")
        self.token = token
        self.catalog = catalog
        self.schema = schema
        if _is_databricks_app():
            self._client = WorkspaceClient()
        else:
            self._client = WorkspaceClient(host=self.host, token=self.token)

    # ── Statement Execution API ───────────────────────────────────────────────
    def _execute_statement(self, sql: str, warehouse_id: str, wait_timeout: str = "30s") -> dict:
        """Submit a SQL statement and return results as a dict.

        Returns a dict matching the REST API response shape so external callers
        (e.g., settings.py clean_metadata) continue to work unchanged.
        """
        try:
            resp = self._client.statement_execution.execute_statement(
                statement=sql,
                warehouse_id=warehouse_id,
                catalog=self.catalog,
                schema=self.schema,
                wait_timeout=wait_timeout,
                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
                disposition=Disposition.INLINE,
            )
            return self._statement_response_to_dict(resp)
        except Exception as e:
            return {"error": str(e)[:300]}

    def _statement_response_to_dict(self, resp) -> dict:
        """Convert SDK StatementResponse to the dict shape callers expect."""
        result = {}
        if resp.statement_id:
            result["statement_id"] = resp.statement_id
        if resp.status:
            result["status"] = {
                "state": resp.status.state.value if resp.status.state else "UNKNOWN",
            }
            if resp.status.error:
                result["status"]["error"] = {
                    "message": resp.status.error.message or "",
                    "error_code": resp.status.error.error_code.value if resp.status.error.error_code else "",
                }
        if resp.manifest:
            cols = []
            if resp.manifest.schema and resp.manifest.schema.columns:
                cols = [{"name": c.name, "type_name": c.type_name.value if c.type_name else ""} for c in resp.manifest.schema.columns]
            result["manifest"] = {"schema": {"columns": cols}}
        if resp.result and resp.result.data_array:
            result["result"] = {"data_array": resp.result.data_array}
        elif "result" not in result:
            result["result"] = {"data_array": []}
        return result

    def _poll_statement(self, statement_id: str) -> dict:
        """Poll until statement finishes execution."""
        for _ in range(self.MAX_POLLS):
            try:
                resp = self._client.statement_execution.get_statement(statement_id)
                d = self._statement_response_to_dict(resp)
                state = d.get("status", {}).get("state", "")
                if state in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
                    return d
            except Exception as e:
                return {"error": f"Poll error: {str(e)[:200]}"}
            time.sleep(self.POLL_INTERVAL)
        return {"error": "Statement timed out after polling"}

    # ── List SQL Warehouses ───────────────────────────────────────────────────
    def list_warehouses(self) -> dict:
        """List available SQL Warehouses."""
        try:
            whs = list(self._client.warehouses.list())
            return {
                "success": True,
                "warehouses": [
                    {
                        "id": w.id,
                        "name": w.name,
                        "state": w.state.value if w.state else "UNKNOWN",
                        "size": w.cluster_size or "N/A",
                        "type": w.warehouse_type.value if w.warehouse_type else "N/A",
                    }
                    for w in whs
                ],
            }
        except Exception as e:
            return {"success": False, "message": str(e)[:300]}

    # ── List Unity Catalog Tables ─────────────────────────────────────────────
    def list_tables(self) -> dict:
        """List tables in target catalog.schema."""
        try:
            tables = list(self._client.tables.list(
                catalog_name=self.catalog,
                schema_name=self.schema,
            ))
            return {
                "success": True,
                "catalog": self.catalog,
                "schema": self.schema,
                "tables": [
                    {
                        "table_name": t.name,
                        "table_type": t.table_type.value if t.table_type else "N/A",
                        "data_source": t.data_source_format.value if t.data_source_format else "N/A",
                        "row_count": (t.properties or {}).get("numRows", "N/A"),
                        "created_at": t.created_at,
                        "updated_at": t.updated_at,
                    }
                    for t in tables
                ],
            }
        except Exception as e:
            # Fallback: use SHOW TABLES SQL
            return self._list_tables_via_sql()

    def _list_tables_via_sql(self) -> dict:
        """Fallback: list tables via SHOW TABLES SQL."""
        try:
            whs_result = self.list_warehouses()
            warehouses = whs_result.get("warehouses", [])
            running_wh = next((w for w in warehouses if w.get("state") == "RUNNING"), warehouses[0] if warehouses else None)
            if not running_wh:
                return {"success": False, "message": "No SQL Warehouse available"}
            result = self._execute_statement(
                f"SHOW TABLES IN `{self.catalog}`.`{self.schema}`",
                running_wh["id"],
            )
            return {"success": True, "sql_result": result, "fallback": True}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── Get Table Schema ──────────────────────────────────────────────────────
    def describe_table(self, table_name: str, warehouse_id: str) -> dict:
        """Return schema and stats for a Unity Catalog table."""
        try:
            sql = f"DESCRIBE EXTENDED `{self.catalog}`.`{self.schema}`.`{table_name}`"
            data = self._execute_statement(sql, warehouse_id)
            stmt_id = data.get("statement_id")
            if not stmt_id:
                return {"success": False, "message": data.get("error", "No statement ID returned")}
            result = self._poll_statement(stmt_id)
            status = result.get("status", {}).get("state", "UNKNOWN")
            if status == "SUCCEEDED":
                rows = result.get("result", {}).get("data_array", [])
                return {
                    "success": True,
                    "table": f"{self.catalog}.{self.schema}.{table_name}",
                    "columns": rows,
                    "statement_id": stmt_id,
                }
            return {"success": False, "message": f"Describe failed: {status}", "detail": result}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── Preview Table Data ────────────────────────────────────────────────────
    def preview_table(self, table_name: str, warehouse_id: str, limit: int = 20) -> dict:
        """Run SELECT on a Unity Catalog table and return preview data."""
        try:
            sql = f"SELECT * FROM `{self.catalog}`.`{self.schema}`.`{table_name}` LIMIT {limit}"
            data = self._execute_statement(sql, warehouse_id)
            stmt_id = data.get("statement_id")
            if not stmt_id:
                return {"success": False, "message": data.get("error", str(data))}
            result = self._poll_statement(stmt_id)
            status = result.get("status", {}).get("state", "UNKNOWN")
            if status == "SUCCEEDED":
                columns = [col.get("name") for col in result.get("manifest", {}).get("schema", {}).get("columns", [])]
                rows = result.get("result", {}).get("data_array", [])
                row_count_resp = self._execute_statement(
                    f"SELECT COUNT(*) AS cnt FROM `{self.catalog}`.`{self.schema}`.`{table_name}`",
                    warehouse_id,
                )
                count_id = row_count_resp.get("statement_id")
                total_rows = "N/A"
                if count_id:
                    count_result = self._poll_statement(count_id)
                    if count_result.get("status", {}).get("state") == "SUCCEEDED":
                        total_rows = count_result.get("result", {}).get("data_array", [["N/A"]])[0][0]
                return {
                    "success": True,
                    "table": f"{self.catalog}.{self.schema}.{table_name}",
                    "columns": columns,
                    "rows": rows,
                    "preview_rows": len(rows),
                    "total_rows": total_rows,
                    "statement_id": stmt_id,
                }
            return {"success": False, "message": f"Preview failed: {status}", "detail": result}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── Execute Custom SQL ───────────────────────────────────────────────────
    def execute_custom_sql(self, sql: str, warehouse_id: str) -> dict:
        """Execute an ad-hoc SQL statement."""
        try:
            resp = self._execute_statement(sql.strip(), warehouse_id, wait_timeout="50s")
            sid = resp.get("statement_id")
            if not sid:
                return {"success": False, "message": resp.get("error", str(resp)[:300])}
            result = self._poll_statement(sid)
            state = result.get("status", {}).get("state", "UNKNOWN")
            if state == "SUCCEEDED":
                columns = [col.get("name") for col in result.get("manifest", {}).get("schema", {}).get("columns", [])]
                rows = result.get("result", {}).get("data_array", [])
                return {
                    "success": True,
                    "sql_type": "query" if columns else "statement",
                    "columns": columns,
                    "rows": rows,
                    "row_count": len(rows),
                    "statement_id": sid,
                }
            else:
                err_msg = (result.get("status", {}).get("error", {}) or {}).get("message", f"Statement {state}")
                return {"success": False, "message": err_msg, "state": state}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── Execute Full Table Pipeline ───────────────────────────────────────────
    def execute_table_pipeline(self, table_name: str, warehouse_id: str, execute_sql: str = None) -> dict:
        """Execute a full pipeline and validate the output table."""
        try:
            steps = []
            overall = True

            check_sql = f"""
                SELECT COUNT(*) AS cnt
                FROM `{self.catalog}`.INFORMATION_SCHEMA.TABLES
                WHERE table_name = '{table_name}'
                  AND table_schema = '{self.schema}'
            """
            resp = self._execute_statement(check_sql, warehouse_id)
            sid = resp.get("statement_id")
            exists = False
            if sid:
                r = self._poll_statement(sid)
                if r.get("status", {}).get("state") == "SUCCEEDED":
                    cnt = int(r.get("result", {}).get("data_array", [[0]])[0][0])
                    exists = cnt > 0

            steps.append({
                "step": "Table Existence Check",
                "status": "PASS" if exists else "INFO",
                "detail": f"Table `{table_name}` {'exists' if exists else 'will be created'}",
            })

            if execute_sql:
                resp2 = self._execute_statement(execute_sql.strip(), warehouse_id, wait_timeout="50s")
                sid2 = resp2.get("statement_id")
                if sid2:
                    r2 = self._poll_statement(sid2)
                    state2 = r2.get("status", {}).get("state", "UNKNOWN")
                    steps.append({
                        "step": "Custom SQL Execution",
                        "status": "PASS" if state2 == "SUCCEEDED" else "FAIL",
                        "detail": f"State: {state2}",
                    })
                    if state2 != "SUCCEEDED":
                        overall = False

            preview = self.preview_table(table_name, warehouse_id, limit=5)
            if preview["success"]:
                steps.append({
                    "step": "Table Validation",
                    "status": "PASS",
                    "detail": f"Total rows: {preview['total_rows']} | Columns: {len(preview['columns'])}",
                    "columns": preview["columns"],
                    "sample_rows": preview["rows"][:5],
                })
            else:
                steps.append({
                    "step": "Table Validation",
                    "status": "WARN",
                    "detail": preview.get("message", "Table not yet available"),
                })

            opt_resp = self._execute_statement(
                f"OPTIMIZE `{self.catalog}`.`{self.schema}`.`{table_name}`",
                warehouse_id,
            )
            opt_sid = opt_resp.get("statement_id")
            if opt_sid:
                opt_r = self._poll_statement(opt_sid)
                opt_s = opt_r.get("status", {}).get("state", "UNKNOWN")
                steps.append({
                    "step": "Delta OPTIMIZE",
                    "status": "PASS" if opt_s == "SUCCEEDED" else "WARN",
                    "detail": f"Optimize state: {opt_s}",
                })

            return {
                "success": overall,
                "table": f"{self.catalog}.{self.schema}.{table_name}",
                "steps": steps,
                "executed_at": str(datetime.now()),
            }
        except Exception as e:
            return {"success": False, "message": str(e)}
