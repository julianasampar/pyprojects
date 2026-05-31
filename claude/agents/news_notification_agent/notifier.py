from anthropic import Anthropic
from anthropic.types import Message
from dotenv import load_dotenv
import duckdb
import json
from agents import utils
from .tools import datetime_tools as dt_tools
from .tools import news_notification_tools as nt_tools

# Loading Anthropic API Key
load_dotenv()

# Create an API Client and define parameters
client = Anthropic()
model = "claude-haiku-4-5"
max_tokens=1000
temperature=0.6
database="agentic_database.db"
database_table='agentic_interlocutor_events'

# Defining tools to be called
tools_functions = {
    "get_current_datetime": dt_tools.get_current_datetime,
    "add_duration_to_datetime": dt_tools.add_duration_to_datetime,
    "schedule_notification": nt_tools.schedule_notification,
}
tools_schemas = [
    dt_tools.get_current_datetime__schema,
    dt_tools.add_duration_to_datetime__schema,
    nt_tools.schedule_notification__schema,
    nt_tools.web_search__schema
]


# Functions add_user_message and add_assistant_message to maintain context for conversations
def add_user_message(messages, text):
    user_message = {"role": "user", "content": text if isinstance(text, Message) else text}
    messages.append(user_message)
    
    value_to_insert = json.dumps(user_message, default=str)
    utils.ingest_metadata(value_to_insert, database=database, database_table=database_table)

def add_assistant_message(messages, text):
    assistant_message = {"role": "assistant", "content": text if isinstance(text, Message) else text}
    messages.append(assistant_message)
    
    value_to_insert = json.dumps(assistant_message, default=str)
    utils.ingest_metadata(value_to_insert, database=database, database_table=database_table)


# Creating function to send request and stream the LLM's responses
def get_streamed_request(**params):
    stream = client.messages.stream(**params)

    with stream as stream:
        for text in stream.text_stream:
            print(text, end="")

    response = stream.get_final_message()

    return response


def interaction(user_input, **params):
    add_user_message(messages, user_input)
    response = get_streamed_request(**params)
    add_assistant_message(messages, response.content)
    return response

# Creating chat prompting interface and 
def chat(messages, system=None, tools=None, tools_functions=tools_functions):
    params = {
        "model": model,
        "max_tokens": max_tokens,
        "messages":messages,
        "temperature": temperature
        }
    
    # Adding optional arguments, if they are declated
    if system: # system = system message. An initial prompt to give the LLM context about how it should approach the interaction
        params["system"] = [{
            "type":"text",
            "text": system,
            "cache_control": {"type": "ephemeral"}
        }]

    if tools: # tools = Python fuctions that the LLM might ask to execute to get external context
        params["tools"] = tools

    user_input = """ Send me a Mac Notifications containg headline and content for one of the 
                        latest news in the world and/or Brazil of the last 5 HOURS.
                """
    add_user_message(messages, user_input)
    response = interaction(user_input, **params)

    while response.stop_reason == 'tool_use': # If the LLM requires a tool call
        tool_outputs = utils.run_tool(response, functions=tools_functions)
        user_input = utils.get_tool_result_block(tool_outputs)
        response = interaction(user_input, **params)

    if response.stop_reason != 'tool_use':
            exit

messages = []
system = """ You are a news reporter. Your role is to create OS Notifications to report the latest news.
            Focus on the text. Don't include any HTML or XML tags.
            The content must fit into the size (320 pixels x 340 pixels) of the MacOs notification banner.
            One notification must report only one news.
            Make sure to keep it concise and to format the text in a readable and appropriate way for Mac notifications.
            Interests: Politics, Economics,  International Affairs.
        """

chat(messages=messages, tools=tools_schemas, system=system)

# To run locally: 
# get inside claude folder 
# and execute: python -m agents.news_notification_agent.notifier