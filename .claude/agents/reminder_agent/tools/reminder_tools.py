from anthropic.types import ToolParam
import asyncio
from desktop_notifier import DesktopNotifier, Sound, DEFAULT_SOUND

def schedule_notification(delay_seconds=None):

    notifier = DesktopNotifier()
    
    async def main(notifier):
        await asyncio.sleep(delay_seconds) 
        await notifier.send(
            title="DON'T FORGET YOUR MEDICINE!!!",
            message="Go take your medicine right now",
            timeout=0.5,
            sound=DEFAULT_SOUND,
        )

    return asyncio.run(main(notifier))

schedule_notification(delay_seconds=5)

## Defining the JSON schema of the function 
schedule_notification__schema = ToolParam({
    "name": "schedule_notification",
    "description": "Schedule a desktop notification to appear after a specified delay.",
    "input_schema": {
        "type": "object",
        "properties": {
            "delay_seconds": {
                "type": "integer",
                "description": "Number of seconds to wait before displaying the notification."
            }
        },
        "required": ["delay_seconds"]
    }
})