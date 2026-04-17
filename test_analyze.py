"""Run the analyze MCP tool against the customer packages."""
import asyncio
import json
import pathlib
from ssis_adf_agent.mcp_server import _analyze

pkgs = sorted(pathlib.Path(r"C:\Users\rowlandmicah\Downloads\Project").glob("*.dtsx"))
for p in pkgs:
    print("=" * 100)
    print(f"ANALYZE: {p.name}")
    print("=" * 100)
    result = asyncio.run(_analyze({"package_path": str(p)}))
    data = json.loads(result[0].text)
    c = data["complexity"]
    print(f"Complexity: {json.dumps(c, indent=2)}")
    print(f"Execution order: {data['execution_order']}")
    print(f"Connection managers ({len(data['connection_managers'])}):")
    for cm in data["connection_managers"]:
        print(f"  - {cm['name']} ({cm['type']}) server={cm['server']} db={cm['database']}")
    print(f"Variables ({len(data['variables'])}): {data['variables']}")
    print(f"Parameters ({len(data['parameters'])}): {data['parameters']}")
    print(f"Event handlers: {data['event_handlers']}")
    print(f"Gap count: {data['gap_count']}")
    for sev in ("manual_required", "warning", "info"):
        items = data["gaps_by_severity"][sev]
        if items:
            print(f"  {sev} ({len(items)}):")
            for g in items[:8]:
                print(f"    - [{g['task_name']} / {g['task_type']}] {g['message']}")
    print()
