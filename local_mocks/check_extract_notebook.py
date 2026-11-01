"""Compile-check the generated Extract notebook (both pipeline modes)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "migration_utility"))
from metadata_notebooks import generate_metadata_notebooks  # noqa: E402

for mode in ("standard", "dlt"):
    nbs = generate_metadata_notebooks(catalog="c", schema="s", landing_path="/Volumes/x/y/landing",
                                      pipeline_mode=mode)["notebooks"]
    ext = next(n for n in nbs if n["name"] == "01_Meta_Extract")["code"]
    for n in nbs:
        compile(n["code"], n["name"], "exec")
    assert '.replace("{table}", TABLE_NAME)' in ext, "template braces not rendered as {table}"
    assert "_api_oauth_login" in ext and "IS_LAKEFLOW_SOURCE" in ext
    print(mode, f"all {len(nbs)} notebooks compile OK")
