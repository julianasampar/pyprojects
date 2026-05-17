# architect_agent.py

import anthropic
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
client = anthropic.Anthropic()

# ── 1. LOAD THE SKILL AS THE SYSTEM PROMPT ──────────────────
def _load_skill(path: str = ".claude/agents/data_discovery_agent/architect/SKILL.md") -> str:
    return Path(path).read_text(encoding="utf-8")


# ── 2. DEFINE THE TOOLS THE AGENT CAN USE ───────────────────
# These are what replaces "the agent reads files" — it does it
# through tools, not by magic.

TOOLS = [
    {
        "name": "list_json_files",
        "description": "Lists all JSON profile files in a directory matching an optional domain pattern.",
        "input_schema": {
            "type": "object",
            "properties": {
                "directory": {"type": "string", "description": "Path to the profiles directory"},
                "domain":    {"type": "string", "description": "Optional domain filter (e.g. 'dvd_rentals')"},
            },
            "required": ["directory"],
        },
    },
    {
        "name": "read_json_file",
        "description": "Reads and returns the content of a single JSON profile file.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "description": "Full path to the JSON file"},
            },
            "required": ["file_path"],
        },
    },
    {
        "name": "write_result_file",
        "description": "Writes the final discovery analysis to a markdown file.",
        "input_schema": {
            "type": "object",
            "properties": {
                "output_path": {"type": "string", "description": "Full path for the output file"},
                "content":     {"type": "string", "description": "Markdown content to write"},
            },
            "required": ["output_path", "content"],
        },
    },
]


# ── 3. IMPLEMENT THE TOOLS (pure Python, no LLM) ────────────

def list_json_files(directory: str, domain: str = None) -> list[str]:
    path = Path(directory)
    files = list(path.glob("*.json"))
    if domain:
        files = [f for f in files if domain in f.stem]
    return [str(f) for f in files]

def read_json_file(file_path: str) -> dict:
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_result_file(output_path: str, content: str) -> str:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return f"Written to {output_path}"

# Router — maps tool name to the actual function
def _run_tool(tool_name: str, tool_input: dict) -> str:
    if tool_name == "list_json_files":
        result = list_json_files(**tool_input)
    elif tool_name == "read_json_file":
        result = read_json_file(**tool_input)
    elif tool_name == "write_result_file":
        print(f"[write_result_file] received keys: {list(tool_input.keys())}")
        result = write_result_file(
            output_path=tool_input["output_path"],
            content=tool_input["content"],
        )
    else:
        result = f"Unknown tool: {tool_name}"
    return json.dumps(result, default=str)


# ── 4. THE AGENTIC LOOP ──────────────────────────────────────

def run_architect_agent(json_storage_path: str, source_domain: str) -> str:
    """
    Runs the architect-discovery sub-agent.

    Parameters:
        json_storage_path : where the profiler JSON files are stored
        source_domain     : the domain to analyze (e.g. "dvd_rentals")

    Returns the final text response from the agent.
    """
    system_prompt = _load_skill()

    # Initial user message — gives the agent its starting context
    messages = [
        {
            "role": "user",
            "content": (
                f"/architect-discovery "
                f"--json-storage-path {json_storage_path} "
                f"--source-domain {source_domain}"
            ),
        }
    ]

    print(f"[architect-agent] Starting for domain: {source_domain}")

    # The loop runs until the agent stops calling tools (stop_reason = "end_turn")
    while True:
        response = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=8096,
            system=system_prompt,
            tools=TOOLS,
            messages=messages,
        )

        # Always append the full assistant response first
        messages.append({"role": "assistant", "content": response.content})

        # Collect any tool_use blocks from the response content directly
        # (more reliable than checking stop_reason alone)
        tool_use_blocks = [block for block in response.content if block.type == "tool_use"]

        # If there are no tool calls, the agent is done
        if not tool_use_blocks:
            final_text = next(
                (block.text for block in response.content if hasattr(block, "text")),
                "No response generated."
            )
            print(f"[architect-agent] Done.")
            return final_text

        # Execute every tool call and collect ALL results into one message
        # This is critical — all tool_results must go back in a single user message
        tool_results = []
        for block in tool_use_blocks:
            print(f"[architect-agent] Tool call: {block.name}({block.input})")
            result = _run_tool(block.name, block.input)
            tool_results.append({
                "type":        "tool_result",
                "tool_use_id": block.id,
                "content":     result,
            })

        # Send all results back in one single user message
        messages.append({"role": "user", "content": tool_results})



result = run_architect_agent(
    json_storage_path="/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/.claude/agents/data_discovery_agent/profiler/resources/dvd_rental",
    source_domain="dvd_rentals"
)

print(result)