"""Consolidate + convert all 3 customer packages into one ADF artifact set."""
import asyncio
import json
import pathlib
from ssis_adf_agent.mcp_server import _consolidate

src = pathlib.Path(r"C:\Users\rowlandmicah\Downloads\Project")
out = pathlib.Path(r"C:\Users\rowlandmicah\Downloads\Project\Converted")
out.mkdir(parents=True, exist_ok=True)

pkgs = sorted(src.glob("*.dtsx"))
print(f"Consolidating {len(pkgs)} packages -> {out}\n")
for p in pkgs:
    print(f"  - {p.name}")

result = asyncio.run(_consolidate({
    "package_paths": [str(p) for p in pkgs],
    "output_dir": str(out),
    "pipeline_prefix": "PL_",
}))
data = json.loads(result[0].text)
print()
print("=" * 100)
print("CONSOLIDATION RESULT")
print("=" * 100)
print(json.dumps(data, indent=2, default=str))
