"""A LangGraph agent with one tool: search the wiki.

Served by MLflow's agent server (server.py), which speaks the OpenAI Responses API
the chat UI expects. The UI asks for a streamed answer first and falls back to a
plain request only on error, so both handlers below are needed. The model endpoint
and the index come from environment variables set in resources/app.yml.
"""

import os
from collections.abc import AsyncGenerator

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks, VectorSearchRetrieverTool
from langchain.agents import create_agent
from langchain.messages import AIMessageChunk
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    create_text_delta,
    output_to_responses_items_stream,
    to_chat_completions_input,
)

from confluence_rag.agent.prompts import SYSTEM_PROMPT

mlflow.langchain.autolog()

# Hybrid search: wiki questions are full of exact names and codes that embeddings blur.
# The SDK client carries the app's service principal credentials.
search_wiki = VectorSearchRetrieverTool(
    index_name=os.environ["VECTOR_SEARCH_INDEX"],
    workspace_client=WorkspaceClient(),
    tool_name="search_wiki",
    tool_description="Search the company wiki for passages relevant to a query.",
    columns=["chunk_to_retrieve", "path"],
    doc_uri="path",
    query_type="HYBRID",
)

agent = create_agent(
    model=ChatDatabricks(endpoint=os.environ["LLM_ENDPOINT"]),
    tools=[search_wiki],
    system_prompt=SYSTEM_PROMPT,
)


def session_id(request: ResponsesAgentRequest) -> str | None:
    """Return the conversation id the UI sends, so the traces of one chat group together."""
    if request.context and request.context.conversation_id:
        return request.context.conversation_id
    if isinstance(request.custom_inputs, dict):
        return request.custom_inputs.get("session_id")
    return None


@stream()
async def stream_handler(request: ResponsesAgentRequest) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    """Stream text deltas as the model writes, and completed items as each step finishes.

    Args:
        request: The Responses API request from the chat UI.

    Yields:
        Responses API stream events.
    """
    if sid := session_id(request):
        mlflow.update_current_trace(metadata={"mlflow.trace.session": sid})
    messages = to_chat_completions_input([item.model_dump() for item in request.input])
    async for mode, data in agent.astream({"messages": messages}, stream_mode=["updates", "messages"]):
        if mode == "updates":
            for node in data.values():
                for event in output_to_responses_items_stream((node or {}).get("messages", [])):
                    yield event
        elif mode == "messages":
            chunk = data[0]
            if isinstance(chunk, AIMessageChunk) and chunk.content:
                yield ResponsesAgentStreamEvent(**create_text_delta(delta=chunk.content, item_id=chunk.id))


@invoke()
async def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    """Answer in one response: the completed items of the stream.

    Args:
        request: The Responses API request.

    Returns:
        The completed output items.
    """
    items = [event.item async for event in stream_handler(request) if event.type == "response.output_item.done"]
    return ResponsesAgentResponse(output=items)
