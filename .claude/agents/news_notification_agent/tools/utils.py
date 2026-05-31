# Defining a function to run the tool (based on the LLM answer) and return the result

from tools.datetime_tools import get_current_datetime, add_duration_to_datetime
from tools.news_notification_tools import schedule_notification

def run_tool(response):
    tool_results = []

    tool_blocks = [block for block in response.content if block.type == "tool_use"]

    for tool in tool_blocks:
        tool_name = tool.name
        params = tool.input
        id = tool.id
        function = globals()[tool_name]

        result = {
            "id": id,
            "response": function(**params)
        }

        tool_results.append(result)
    return tool_results


# Creating the ToolResultBlock
def get_tool_result_block(tool_results):
    ToolResultBlock = []

    for result in tool_results:
        try:
            result_block = { 
                "tool_use_id": result['id'],
                "type": "tool_result",
                "content": result['response'],
                "is_error": False
            }
        
        except Exception as e: 
            result_block = { 
                "tool_use_id": result['id'],
                "type": "tool_result",
                "content": f"Failed to execute function. Error: {e}",
                "is_error": True
            }
        
        ToolResultBlock.append(result_block)

    return ToolResultBlock

