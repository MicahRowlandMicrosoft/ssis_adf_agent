"""Dry-run deploy of converted artifacts."""
import asyncio, json
from ssis_adf_agent.mcp_server import _deploy

result = asyncio.run(_deploy({
    "artifacts_dir": r"C:\Users\rowlandmicah\Downloads\Project\Converted",
    "subscription_id": "564fde6a-18b1-425a-a184-ea80343143e4",
    "resource_group": "rg-mcp-ssis-to-adf-test",
    "factory_name": "MCPTest",
    "dry_run": False,
}))
print(json.dumps(json.loads(result[0].text), indent=2, default=str))
