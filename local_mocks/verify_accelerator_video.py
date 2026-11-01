"""Check the Accelerator Video is served correctly by the running app: page wiring,
full download, byte-range (seek) support, poster, and MP4 faststart (moov first)."""
import re
import sys
import time

import requests

APP = "http://localhost:5000"
for _ in range(60):
    try:
        requests.get(APP + "/api/v1/sources/registry.js", timeout=3)
        break
    except requests.RequestException:
        time.sleep(2)

ok = True
page = requests.get(APP + "/", timeout=60).text
srcs = sorted(set(re.findall(r'/static/video/[^"\']+', page)))
print("page refs   :", srcs)
vid = next((s for s in srcs if s.split("?")[0].endswith(".mp4")), None)
if not vid:
    sys.exit("no video reference in page")

full = requests.get(APP + vid, timeout=120)
print("full GET    :", full.status_code, full.headers.get("Content-Type"), f"{len(full.content)/1048576:.2f} MB")
ok &= full.status_code == 200 and full.headers.get("Content-Type", "").startswith("video/mp4")

rng = requests.get(APP + vid, headers={"Range": "bytes=4000000-4000999"}, timeout=60)
print("range GET   :", rng.status_code, rng.headers.get("Content-Range"), len(rng.content), "bytes")
ok &= rng.status_code == 206 and len(rng.content) == 1000

head = full.content[:64]
moov, mdat = full.content.find(b"moov"), full.content.find(b"mdat")
print("faststart   :", 0 <= moov < mdat, f"(moov@{moov}, mdat@{mdat})")
ok &= 0 <= moov < mdat

poster = next((s for s in srcs if s.split("?")[0].endswith(".jpg")), None)
p = requests.get(APP + poster, timeout=60) if poster else None
print("poster      :", p.status_code if p else None, p.headers.get("Content-Type") if p else None)
ok &= bool(p) and p.status_code == 200

print("RESULT      :", "OK" if ok else "FAILED")
sys.exit(0 if ok else 1)
