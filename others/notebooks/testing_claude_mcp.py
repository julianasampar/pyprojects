# Import libraries

import sys
from anthropic import Anthropic
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from typing import Annotated
from pydantic import Field
from dotenv import load_dotenv

# Loading Anthropic API Key
load_dotenv()

# Getting chatbot_agent
sys.path.append("/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/claude")
from agents.notification_agent.tools import datetime_tools, notifier_tools

# Create an API Client
client = Anthropic()

# Defining parameters
model = "claude-haiku-4-5"
max_tokens=1000

mcp = FastMCP("system_notifier", log_level="ERROR")

mcp.add_tool(
    datetime_tools.get_current_datetime,
    name="get_current_datetime",
    description="Returns the current date and time formatted according to the specified format"
)

mcp.add_tool(
    datetime_tools.add_duration_to_datetime,
    name="add_duration_to_datetime",
    description="Adds a specified duration to a datetime string and returns the resulting datetime in a detailed format. This tool converts an input datetime string to a Python datetime object, adds the specified duration in the requested unit, and returns a formatted string of the resulting datetime. It handles various time units including seconds, minutes, hours, days, weeks, months, and years, with special handling for month and year calculations to account for varying month lengths and leap years. The output is always returned in a detailed format that includes the day of the week, month name, day, year, and time with AM/PM indicator (e.g., 'Thursday, April 03, 2025 10:30:00 AM')."
)

mcp.add_tool(
    news_notification_tools.schedule_notification,
    name="schedule_notification",
    description="Schedule a desktop notification with title and content to appear after a specified delay."
)