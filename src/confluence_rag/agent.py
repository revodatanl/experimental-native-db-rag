"""A LangGraph agent with one tool, `search_wiki`, served by MLflow's agent server.

MLflow's agent server speaks the OpenAI Responses API the chat UI expects, and proxies
the UI's pages. The UI streams, so only a stream handler is registered. The model
endpoint and the index come from environment variables set in resources/app.yml.

The search runs as the signed-in user. Databricks Apps forwards a token for them with
every request, scoped to vector search, so Unity Catalog applies their own privileges
and the app needs no grant of its own on the catalog.
"""

import os
from collections.abc import AsyncGenerator

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks
from langchain.agents import create_agent
from langchain.tools import tool
from mlflow.genai.agent_server import AgentServer, get_request_headers, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentStreamEvent,
    output_to_responses_items_stream,
    to_chat_completions_input,
)

mlflow.langchain.autolog()

SYSTEM_PROMPT = """You answer questions about the company wiki.
Always call search_wiki before answering. If the results look thin, search again with a reformulated query.
Answer only from the results, and list the source paths you used. Say so when the wiki does not contain the answer."""


def user_client() -> WorkspaceClient:
    """A client acting as the signed-in user, from the token Databricks Apps forwards with each request."""
    token = get_request_headers().get("x-forwarded-access-token")
    if not token:
        raise RuntimeError("The request carries no user token: the app needs the `vector-search` user API scope.")
    return WorkspaceClient(token=token, auth_type="pat")


@tool
def search_wiki(query: str) -> list[dict]:
    """Search the company wiki for passages relevant to a query."""
    response = user_client().vector_search_indexes.query_index(
        index_name=os.environ["VECTOR_SEARCH_INDEX"],
        columns=["chunk_to_retrieve", "path"],
        query_text=query,
        query_type="HYBRID",
        num_results=5,
    )
    rows = response.result.data_array if response.result else None
    return [{"text": text, "path": path} for text, path, _score in rows or []]


agent = create_agent(
    model=ChatDatabricks(endpoint=os.environ["LLM_ENDPOINT"]),
    tools=[search_wiki],
    system_prompt=SYSTEM_PROMPT,
)


@stream()
async def stream_handler(request: ResponsesAgentRequest) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    """Emit each step's completed items: the tool call, its results, and the answer."""
    messages = to_chat_completions_input([item.model_dump() for item in request.input])
    async for step in agent.astream({"messages": messages}, stream_mode="updates"):
        for node in step.values():
            for event in output_to_responses_items_stream(node["messages"]):
                yield event


server = AgentServer("ResponsesAgent", enable_chat_proxy=True)
