from anthropic import Anthropic
from mcp import Client
import os

anthropic_client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
mcp_client = Client("http://localhost:8001")


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
        stream = anthropic_client.messages.stream(
            model="claude-3-5-sonnet",
            tools=mcp_client.tools(),
            system=SYSTEM_PROMPT,
            max_tokens=800,
            messages=[{"role": "user", "content": user_query}],
        )

        for text in stream.text_stream:
            yield text

    except Exception as e:
        yield f"Error: {str(e)}"