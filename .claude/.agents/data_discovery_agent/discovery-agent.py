
import asyncio
from claude_agent_sdk import query, ClaudeAgentOptions


async def main():
    async for message in query(
        prompt="/orchestrator others/archive/dvd_rental",
        options=ClaudeAgentOptions(allowed_tools=["Read", "Glob", "Grep", "Bash"], 
                                   setting_sources=[ "project"],
                                   skills="orchestrator",
                                   permission_mode="bypassPermissions"
                                   ),
    ):
        print(message)  


asyncio.run(main())