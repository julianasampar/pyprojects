from anthropic import Anthropic
from dotenv import load_dotenv
from profiler.reader import get_datasource
import duckdb

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

# Creating functions to maintain context for conversations

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

def add_user_message(messages, text):
    user_message = {"role": "user", "content": text}
    messages.append(user_message)
    ingest_metadata(user_message)

def add_assistant_message(messages, text):
    assistant_message = {"role": "assistant", "content": text}
    messages.append(assistant_message)
    ingest_metadata(assistant_message)


def interaction(**params):
    stream = client.messages.stream(**params)

    with stream as stream:
        for text in stream.text_stream:
            print(text, end="")

    response = stream.get_final_message()

    return response.content[0].text

def chat(messages, system=None):
    params = {
        "model": model,
        "max_tokens": max_tokens,
        "messages":messages,
        "temperature": temperature,
        }
    
    if system:
        params["system"] = system
    
    while True:
        try: 
            user_prompt = input("\nPrompt: ")
            if user_prompt.lower() == 'exit':
                break
        except KeyboardInterrupt:
            print('Assistant: Goodbye')
            break
        except EOFError:
            print('Assistant: Goodbye')
            break

        add_user_message(messages, user_prompt)
        answer = interaction(**params)
        add_assistant_message(messages, answer)


def autofill_datasources():
    datasources = get_datasource('csv', folder_path='/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/others/archive/dvd_rental_store')
    datasources = datasources.list_tables()


messages = []
#system = "You are a data expert in charge of a data discovery project that interacts in a concise way."
chat(messages=messages)