
import asyncio
from claude_agent_sdk import query, ClaudeAgentOptions


async def main():
    async for message in query(
        prompt="/source-discovery-skill sqlite dvd rental",
        options=ClaudeAgentOptions(allowed_tools=["Read", "Glob", "Grep", "Bash"], 
                                   mcp_servers={"codemode-sqlite": {"command": "/Users/julianasampar/go/bin/codemode-sqlite-mcp","args": ["--mode=stdio", "--db=/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/dbt_database.db"]}},
                                   setting_sources=[ "project"],
                                   skills="source-discovery-skill",
                                   permission_mode="bypassPermissions"
                                   ),
    ):
        print(message)  


asyncio.run(main())