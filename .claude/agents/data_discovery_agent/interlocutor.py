from anthropic import Anthropic
from dotenv import load_dotenv
from profiler.reader import get_datasource
import duckdb
import tools

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
                        json_log JSON,
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
    user_message = {"role": "user", "content": text}
    messages.append(user_message)
    ingest_metadata(user_message)

def add_assistant_message(messages, text):
    assistant_message = {"role": "assistant", "content": text}
    messages.append(assistant_message)
    ingest_metadata(assistant_message)


# Creating function to send request and stream the LLM's responses
def interaction(**params):
    stream = client.messages.stream(**params)

    with stream as stream:
        for text in stream.text_stream:
            print(text, end="")

    response = stream.get_final_message()
    response = response.content

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
            user_prompt = input("\nPrompt: ")
            if user_prompt.lower() == 'exit':
                break
        except KeyboardInterrupt:
            break
        except EOFError:
            break

        add_user_message(messages, user_prompt)
        answer = interaction(**params)

        # while stop_reason == 'tool_use'
        add_assistant_message(messages, answer)


messages = []
#tools = []
chat(messages=messages)