"""
behaviorist.py

Creates an agent to query data dinamically and analyze the behavior.
"""
from pathlib import Path

from claude.agents.data_discovery_agent__final.tools import tools
from claude.agents.data_discovery_agent__final.tools.utils import runner

from anthropic import Anthropic
from anthropic.types import Message
from dotenv import load_dotenv

# Loading Anthropic API Key
load_dotenv()

client = Anthropic()

model = "claude-haiku-4-5"
max_tokens=1000

# Defining tools to be called
tools_functions = {
    "read_profiles": tools.read_profiles,
    "read_data_sample": tools.read_data_sample
}
tools_schemas = [
    tools.read_profiles__schema,
    tools.read_data_sample__schema
]

# Assigning the SKILL as the system prompt
def load_skill() -> str:
    skill_path = Path(__file__).parent / "SKILL.md"
    return skill_path.read_text(encoding="utf-8")

def run_agent(storage_type: str, source_type: str, folder_path: str):
    """
    Runs the inspector sub-agent.

    Parameters:
        source_type : the storage type where the profiler JSON files are located. It can be local or AWS.
        folder_path : directory folder of the files.

    Returns the final analysis from the agent.
    """
    system_prompt = load_skill()

    # Assigning the initial user prompt

    messages = [
            {
                "role": "user",
                "content": (
                    f"/read_profiles"
                    f"--storage_type {storage_type} "
                    f"--folder_path {folder_path}"
                ),
            }
        ]
    
    print(f"[behavorist-agent] Reading files located in: {folder_path}")

    response = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            tools=tools_schemas,
            tool_choice={"type": "tool", "name": "read_profiles"}, # Forcing the tool
            messages=messages,
        )
    
    print(response.content)

    if response.stop_reason != 'tool_use':
        raise ValueError("[behavorist-agent] The Agent could not be executed. Please try again.")
    
    else:
        result = runner.run_tool(response, tools_functions)
        result = result[0]["response"]

        print(f"[behavorist-agent] Starting behavior analysis.")

    filters = []

    for datasource, profile in result.items():

        messages = [
                {
                    "role": "user",
                    "content": (
                    f"""
                        Create a list of dicts for {datasource} containing each distinct value associated with it's columns name
                        and datasource, following the expected output pattern of 'filter' argument of read_data_sample tool.
                        Use the profile JSON below to find the appropiate distinct values to filter.
                        {profile} 
                    """
                    ),
                },
                {
                    "role": "assistant",
                    "content": "Here is the list of dicts: ```"
                }
            ]
        
        print(f"[behavorist-agent] Loading filters of {datasource}.")
        
        response = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system_prompt,
                tools=tools_schemas,
                messages=messages,
                stop_sequences=["```"]
            )
        
        filter_dict = response.content[0].text
        filters.append(filter_dict)
        
        print(f"[behavorist-agent] Filters of {datasource} successfully loaded.")

        
        messages = [
            {
                "role": "user",
                "content": (
                    f"/read_data_sample"
                    f"--source_type {source_type} "
                    f"--folder_path {folder_path}"
                    f"--filters {filters}"
                ),
            }
        ]

        response = client.messages.create(
                model="claude-opus-4-8",
                max_tokens=max_tokens,
                system=system_prompt,
                tools=tools_schemas,
                tool_choice={"type": "tool", "name": "read_data_sample"}, # Forcing the tool
                messages=messages,
            )
        
        print(response.content)

        if response.stop_reason != 'tool_use':
            raise ValueError("[behavorist-agent] The Agent could not be executed. Please try again.")
        
        else:
            result = runner.run_tool(response, tools_functions)
            result = result[0]["response"]

        print(result)

        return filters
        


# To run it:
run_agent(storage_type='local', source_type='snowflake', folder_path='/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/others/archive/agent_test')
#python3 -m claude.agents.data_discovery_agent__final.behaving.behaviorist