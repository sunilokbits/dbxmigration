"""Bronze/Silver (SDP) diagnostics for recent ServiceNow job runs."""
import time

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
since_ms = int((time.time() - 3 * 3600) * 1000)

for r in w.jobs.list_runs(start_time_from=since_ms, expand_tasks=True):
    if "servicenow" not in (r.run_name or "").lower():
        continue
    run = w.jobs.get_run(r.run_id)
    print(f"\n=== {run.run_name} | {run.state.life_cycle_state.value} "
          f"{run.state.result_state.value if run.state.result_state else ''}")
    for t in run.tasks or []:
        if t.task_key != "bronze_silver":
            continue
        print("   bronze_silver:", t.state.result_state.value if t.state.result_state else t.state.life_cycle_state.value)
        try:
            out = w.jobs.get_run_output(t.run_id)
            if out.notebook_output and out.notebook_output.result:
                print("   exit:", out.notebook_output.result[:700])
            if out.error:
                print("   error:", out.error[:900])
        except Exception as e:
            print("   (output unavailable)", str(e)[:200])

print("\n--- SDP pipelines for ServiceNow tables: latest update + errors ---")
for p in w.pipelines.list_pipelines(filter="name LIKE '%ServiceNow%'"):
    if "Lakeflow" in (p.name or "") or "lakeflow" in (p.name or ""):
        continue
    info = w.pipelines.get(p.pipeline_id)
    lu = (info.latest_updates or [None])[0]
    print(f"\n{p.name} | {lu.state.value if lu else '-'}")
    spec = w.api_client.do("GET", f"/api/2.0/pipelines/{p.pipeline_id}").get("spec", {})
    print("   configuration:", {k: v for k, v in (spec.get("configuration") or {}).items()})
    ev = w.api_client.do("GET", f"/api/2.0/pipelines/{p.pipeline_id}/events", query={"max_results": 30})
    for e in ev.get("events", []):
        if e.get("level") == "ERROR":
            print("   ERROR:", e.get("message", "")[:400])
            break
