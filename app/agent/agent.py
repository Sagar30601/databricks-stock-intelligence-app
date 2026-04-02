from anthropic import Anthropic
import os
from mcp_server.tools import run_sql, best_month, list_tables, describe_table
# from mcp.client import Client

anthropic_client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

tools = [
    {
        "name": "run_sql",
        "description": "Execute SQL query",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "best_month",
        "description": "Best performing month",
        "input_schema": {
            "type": "object",
            "properties": {
                "stock": {"type": "string"},
                "year": {"type": "integer"}
            },
            "required": ["stock", "year"]
        }
    }
]


SYSTEM_PROMPT = """
You are a financial data analyst.

Rules:
- ALWAYS use tools for real data
- NEVER guess
- Explain insights clearly
- Suggest improvements if relevant
"""


def stream_response(user_query):
    try:
        response = anthropic_client.messages.create(
            model="claude-3-5-sonnet",
            system="You are a financial analyst. Always use tools for data.",
            tools=tools,
            messages=[{"role": "user", "content": user_query}],
            max_tokens=800
        )

        for block in response.content:

            if block.type == "tool_use":
                tool_name = block.name
                args = block.input

                if tool_name == "run_sql":
                    result = run_sql(args["query"])
                elif tool_name == "best_month":
                    result = best_month(args["stock"], args["year"])
                else:
                    result = "Unknown tool"

                # Send result back to Claude
                followup = anthropic_client.messages.create(
                    model="claude-3-5-sonnet",
                    messages=[
                        {"role": "user", "content": str(result)}
                    ]
                )

                yield followup.content[0].text

            elif block.type == "text":
                yield block.text

    except Exception as e:
        yield f"Error: {str(e)}"