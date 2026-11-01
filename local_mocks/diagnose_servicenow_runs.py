"""Diagnose recent Migration Studio ServiceNow job runs: per-task state + Extract output."""
import json
import time

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
since_ms = int((time.time() - 3 * 3600) * 1000)

runs = [r for r in w.jobs.list_runs(start_time_from=since_ms, expand_tasks=True)
        if "servicenow" in (r.run_name or "").lower()]
print(f"{len(runs)} ServiceNow runs in the last 3h")
for r in runs[:12]:
    st = r.state
    print(f"\n=== {r.run_name} | run {r.run_id} | {st.life_cycle_state.value if st.life_cycle_state else ''} "
          f"{st.result_state.value if st.result_state else ''}")
    for t in r.tasks or []:
        ts = t.state
        res = ts.result_state.value if ts and ts.result_state else (ts.life_cycle_state.value if ts else "?")
        print(f"   task {t.task_key:16s} -> {res}")
        if t.task_key == "extract":
            try:
                out = w.jobs.get_run_output(t.run_id)
                nb = (out.notebook_output.result if out.notebook_output else "") or ""
                print("     extract exit:", nb[:400])
                if out.error:
                    print("     extract error:", out.error[:600])
                if out.error_trace:
                    print("     trace tail:", out.error_trace[-800:])
            except Exception as e:
                print("     (output unavailable)", str(e)[:200])
