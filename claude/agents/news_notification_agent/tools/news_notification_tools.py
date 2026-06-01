from anthropic.types import ToolParam
import asyncio
from desktop_notifier import DesktopNotifier, Sound, DEFAULT_SOUND

def schedule_notification(yesterdays_news:str):

    notifier = DesktopNotifier()
    
    async def main(notifier):
        await notifier.send(
            title="TIME TO READ YOUR NEWS!!!",
            message=yesterdays_news,
            timeout=0.5,
            sound=DEFAULT_SOUND,
        )

    return asyncio.run(main(notifier))

# Fix the function - the LLM needs to wait the delay to give the reponse back. Not practical

## Defining the JSON schema of the function 
schedule_notification__schema = ToolParam({
    "name": "schedule_notification",
    "description": "Schedule a desktop notification to appear after a specified delay.",
    "input_schema": {
        "type": "object",
        "properties": {
            "yesterdays_news": {
                "type": "string",
                "description": "TOP 3 headlines from yesterday's new. "
            },
        },
        "required": ["yesterdays_news"]
    }
})

web_search__schema = {
  "type": "web_search_20250305",
  "name": "web_search",
  "max_uses": 2
}