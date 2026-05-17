from anthropic import Anthropic
from dotenv import load_dotenv

# Loading Anthropic API Key
load_dotenv()

# Create an API Client
client = Anthropic()
model = "claude-haiku-4-5"
max_tokens=1000
system_prompt = "You are a helpful assistant specialized in botanic."

# Creating functions to maintain context for conversations

def add_user_message(messages, text):
    user_message = {"role": "user", "content": text}
    messages.append(user_message)

def add_assistant_message(messages, text):
    assistant_message = {"role": "assistant", "content": text}
    messages.append(assistant_message)

def chat(messages):
    message = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=messages,
        system=system_prompt
    )
    return message.content[0].text

def conversation(messages):
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
        answer = chat(messages)
        print(f"Assistant: {answer}")
        add_assistant_message(messages, answer)