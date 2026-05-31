import duckdb

##############################################
            ## TOOL FUNCTIONS ##
##############################################
# Defining a function to run the tool (based on the LLM answer) and return the result
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


##############################################
            ## METADATA FUNCTIONS ##
##############################################

# Functions ingest_metadata to store each interaction
def ingest_metadata(json, database, database_table):
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