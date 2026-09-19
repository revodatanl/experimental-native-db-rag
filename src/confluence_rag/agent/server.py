"""Serve the agent over the OpenAI Responses API, with the chat UI proxied behind it."""

from mlflow.genai.agent_server import AgentServer

import confluence_rag.agent.agent  # noqa: F401  # importing registers the invoke and stream handlers

server = AgentServer("ResponsesAgent", enable_chat_proxy=True)
app = server.app


def main() -> None:
    """Entry point for `uv run start-server`. Accepts --port, --workers and --reload."""
    server.run(app_import_string="confluence_rag.agent.server:app")
