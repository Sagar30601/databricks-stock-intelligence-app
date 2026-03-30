from mcp import Server
from tools import run_sql, best_month, list_tables, describe_table

server = Server("databricks-mcp-agent")

# Register tools
server.tool()(run_sql)
server.tool()(best_month)
server.tool()(list_tables)
server.tool()(describe_table)

if __name__ == "__main__":
    server.run(host="0.0.0.0", port=8001)