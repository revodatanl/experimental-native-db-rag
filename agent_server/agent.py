"""A LangGraph agent with one tool: search the wiki.

Served by MLflow's AgentServer (see start_server.py), which speaks the OpenAI
Responses API that the chat UI expects. The model endpoint and the index come
from environment variables set in resources/app.yml.
"""

import logging
import os
from typing import AsyncGenerator

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks, VectorSearchRetrieverTool
from langchain.agents import create_agent
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    to_chat_completions_input,
)

from agent_server.utils import get_session_id, process_agent_astream_events

mlflow.langchain.autolog()
logging.getLogger("mlflow.utils.autologging_utils").setLevel(logging.ERROR)

SYSTEM_PROMPT = """You answer questions about the company wiki.
Always call search_wiki before answering. If the results look thin, search again with a reformulated query.
Answer only from the results, and list the source paths you used. Say so when the wiki does not contain the answer."""

# Hybrid search: wiki questions are full of exact names and codes that embeddings blur.
# The SDK client carries the app's service principal credentials.
search_wiki = VectorSearchRetrieverTool(
    index_name=os.environ["VECTOR_SEARCH_INDEX"],
    workspace_client=WorkspaceClient(),
    tool_name="search_wiki",
    tool_description="Search the company wiki for passages relevant to a query.",
    columns=["chunk_to_retrieve", "path"],
    doc_uri="path",
    num_results=5,
    query_type="HYBRID",
)

agent = create_agent(
    model=ChatDatabricks(endpoint=os.environ["LLM_ENDPOINT"]),
    tools=[search_wiki],
    system_prompt=SYSTEM_PROMPT,
)


@invoke()
async def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    outputs = [
        event.item
        async for event in stream_handler(request)
        if event.type == "response.output_item.done"
    ]
    return ResponsesAgentResponse(output=outputs)


@stream()
async def stream_handler(
    request: ResponsesAgentRequest,
) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    if session_id := get_session_id(request):
        mlflow.update_current_trace(metadata={"mlflow.trace.session": session_id})
    messages = {"messages": to_chat_completions_input([item.model_dump() for item in request.input])}
    async for event in process_agent_astream_events(
        agent.astream(input=messages, stream_mode=["updates", "messages"])
    ):
        yield event
