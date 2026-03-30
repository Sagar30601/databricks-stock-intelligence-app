#!/bin/bash

echo "Starting MCP server..."
python mcp/server.py &

echo "Starting Streamlit app..."
streamlit run app.py --server.port 8000