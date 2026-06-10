def run_tool(response, functions):
    tool_results = []

    tool_blocks = [block for block in response.content if block.type == "tool_use"]

    for tool in tool_blocks:
        tool_name = tool.name
        params = tool.input
        id = tool.id
        function = functions[tool_name]

        result = {
            "id": id,
            "response": function(**params)
        }

        tool_results.append(result)
    return tool_results