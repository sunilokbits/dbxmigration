"""Wait for the latest run of a Migration Studio table job and print task results.

    python wait_latest_run.py servicenow.problem
"""
import sys
import time

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
table = sys.argv[1]
runs = [r for r in w.jobs.list_runs(start_time_from=int((time.time() - 6 * 3600) * 1000))
        if (r.run_name or "").endswith(table)]
if not runs:
    sys.exit(f"no recent run for {table}")
rid = max(runs, key=lambda r: r.start_time or 0).run_id
while True:
    run = w.jobs.get_run(rid)
    lc = run.state.life_cycle_state.value
    if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        break
    time.sleep(20)
print(run.run_name, "|", lc, run.state.result_state.value if run.state.result_state else "")
for t in run.tasks or []:
    res = t.state.result_state.value if t.state and t.state.result_state else "?"
    line = f"   {t.task_key:16s} {res}"
    if t.task_key == "extract":
        out = w.jobs.get_run_output(t.run_id)
        line += "  " + ((out.notebook_output.result if out.notebook_output else "") or out.error or "")[:220]
    print(line)
