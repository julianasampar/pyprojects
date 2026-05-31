
import asyncio
from claude_agent_sdk import query, ClaudeAgentOptions


async def main():
    async for message in query(
        prompt="do you have access to my architect-discovery skill?",
        options=ClaudeAgentOptions(allowed_tools=["Read", "Glob", "Grep", "Bash"], 
                                   setting_sources=[ "project"],
                                   skills=["architect-discovery"],
                                   permission_mode="bypassPermissions"
                                   ),
    ):
        print(message)  


asyncio.run(main())