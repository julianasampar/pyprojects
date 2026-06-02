import asyncio
from anthropic.types import ToolParam
from desktop_notifier import DesktopNotifier, DEFAULT_SOUND

async def schedule_notification(title:str, content:str):
    
    notifier = DesktopNotifier()
    
    await notifier.send(
            title=title,
            message=content,
            timeout=0.5,
            sound=DEFAULT_SOUND,
        )

    return "Notification was successfully sent"

## Defining the JSON schema of the function 
schedule_notification__schema = ToolParam({
    "name": "schedule_notification",
    "description": "Schedule a desktop notification to appear after a specified delay.",
    "input_schema": {
        "type": "object",
        "properties": {
            "title": {
                "type": "string",
                "description": "Title for the notification"
            },
            "content": {
                "type": "string",
                "description": "Content to be included in the notification body "
            },
        },
        "required": ["title", "content"]
    }
})


web_search__schema = {
  "type": "web_search_20250305",
  "name": "web_search",
  "max_uses": 2
}