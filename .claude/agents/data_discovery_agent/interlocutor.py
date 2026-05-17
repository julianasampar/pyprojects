from anthropic import Anthropic
from dotenv import load_dotenv

# Loading Anthropic API Key
load_dotenv()

# Create an API Client
client = Anthropic()
model = "claude-haiku-4-5"
max_tokens=1000

# Creating functions to maintain context for conversations

def add_user_message(messages, text):
    user_message = {"role": "user", "content": text}
    messages.append(user_message)

def add_assistant_message(messages, text):
    assistant_message = {"role": "assistant", "content": text}
    messages.append(assistant_message)


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
        "temperature": 0.6,
        }
    
    if system:
        params["system"] = system
    
    while True:
        try: 
            user_prompt = input("Prompt: ")
            if user_prompt.lower() == 'exit':
                break
        except KeyboardInterrupt:
            print('Assistant: Goodbye')
            break
        except EOFError:
            print('Assistant: Goodbye')
            break

        add_user_message(messages, user_prompt)
        print(f"User: {user_prompt}")
        answer = interaction(**params)
        print(f"Assistant: {answer}")
        add_assistant_message(messages, answer)

messages = []
#system = "You are a data expert in charge of a data discovery project that interacts in a concise way."
chat(messages=messages)