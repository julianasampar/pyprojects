from anthropic import Anthropic
from anthropic.types import Message
from dotenv import load_dotenv
import duckdb
import json
from tools import utils
from tools.datetime_tools import get_current_datetime__schema, add_duration_to_datetime__schema

# Loading Anthropic API Key
load_dotenv()

# Create an API Client

# Define parameters
client = Anthropic()
model = "claude-haiku-4-5"
max_tokens=1000
temperature=0.6
database="agentic_database.db"
database_table='agentic_interlocutor_events'


# Functions ingest_metadata to store each interaction
def ingest_metadata(json):
    connection = duckdb.connect(database)
    connection.sql(f"""CREATE TABLE IF NOT EXISTS {database_table} ( 
                        interaction_log JSON,
                        inserted_at TIMESTAMP
                    );
                    """)
    connection.execute(f"""INSERT INTO {database_table} VALUES 
                        (?, CURRENT_TIMESTAMP);
                        """, 
                        [json]
                    )


# Functions add_user_message and add_assistant_message to maintain context for conversations
def add_user_message(messages, text):
    user_message = {"role": "user", "content": text if isinstance(text, Message) else text}
    messages.append(user_message)
    
    value_to_insert = json.dumps(user_message, default=str)
    ingest_metadata(value_to_insert)

def add_assistant_message(messages, text):
    assistant_message = {"role": "assistant", "content": text if isinstance(text, Message) else text}
    messages.append(assistant_message)
    
    value_to_insert = json.dumps(assistant_message, default=str)
    ingest_metadata(value_to_insert)


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
def chat(messages, system=None, tools=None):
    params = {
        "model": model,
        "max_tokens": max_tokens,
        "messages":messages,
        "temperature": temperature,
        }
    
    # Adding optional arguments, if they are declated
    if system: # system = system message. An initial prompt to give the LLM context about how it should approach the interaction
        params["system"] = system

    if tools: # tools = Python fuctions that the LLM might ask to execute to get external context
        params["tools"] = tools

    while True:
        try: 
            user_input = input("\nPrompt: ")
            if user_input.lower() == 'exit':
                break
        except KeyboardInterrupt:
            break
        except EOFError:
            break
        
        response = interaction(user_input, **params)

        while response.stop_reason == 'tool_use': # If the LLM requires a tool call
            tool_outputs = utils.run_tool(response)
            user_input = utils.get_tool_result_block(tool_outputs)
            response = interaction(user_input, **params)

        if response.stop_reason != 'tool_use':
            continue

messages = []
tool = [get_current_datetime__schema, add_duration_to_datetime__schema]
chat(messages=messages, tools=tool)