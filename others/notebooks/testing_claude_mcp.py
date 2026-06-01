# Import libraries

import sys
import importlib
from anthropic import Anthropic
from dotenv import load_dotenv

# Loading Anthropic API Key
load_dotenv()

# Getting chatbot_agent
sys.path.append("/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/claude")
from agents.chatbot_agent import interlocutor

importlib.reload(interlocutor)

# Create an API Client
client = Anthropic()

# Defining parameters
model = "claude-haiku-4-5"
max_tokens=1000

from pydantic import Field
from mcp.server.fastmcp import FastMCP
mcp = FastMCP("mcp_blabla", log_level="ERROR")

@mcp.tool(
    name="blablabla",
    description="Blablabla"
)
def blablabla(
    bla: int = Field(description="blabla")
):
    return bla

