"""Preview Pipeline Studio's newest-first order (same rule as the UI sort)."""
from datetime import datetime

import requests

groups = requests.get("http://localhost:5000/api/v1/workflow/pipelines", timeout=300).json().get("groups") or []


def ts(g):
    s = g.get("last_activity") or g.get("created_at") or ""
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0


missing = sum(1 for g in groups if not (g.get("last_activity") or g.get("created_at")))
print(f"{len(groups)} groups, {missing} without a timestamp")
for g in sorted(groups, key=ts, reverse=True)[:10]:
    print(f"  {g.get('last_activity') or g.get('created_at')}  {g['full_table']:32s} {g.get('status')}")
