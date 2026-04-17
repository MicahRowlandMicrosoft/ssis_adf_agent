"""Convert the ungrouped Transaction-Control package standalone."""
import asyncio
import json
import pathlib
from ssis_adf_agent.mcp_server import _convert

result = asyncio.run(_convert({
    "package_path": r"C:\Users\rowlandmicah\Downloads\Project\SQLMI-ADDS-Transaction-Control.dtsx",
    "output_dir": r"C:\Users\rowlandmicah\Downloads\Project\Converted",
}))
data = json.loads(result[0].text)
print(json.dumps(data, indent=2, default=str))
