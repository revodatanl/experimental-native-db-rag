"""A LangGraph agent with one tool, `search_wiki`, served by MLflow's agent server.

MLflow's agent server speaks the OpenAI Responses API the chat UI expects, and proxies
the UI's pages. The UI streams, so only a stream handler is registered. The model
endpoint and the index come from environment variables set in resources/app.yml.
"""

import os
from collections.abc import AsyncGenerator

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks
from langchain.agents import create_agent
from langchain.messages import AIMessageChunk
from langchain.tools import tool
from mlflow.genai.agent_server import AgentServer, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentStreamEvent,
    create_text_delta,
    output_to_responses_items_stream,
    to_chat_completions_input,
)

mlflow.langchain.autolog()

SYSTEM_PROMPT = """You answer questions about the company wiki.
Always call search_wiki before answering. If the results look thin, search again with a reformulated query.
Answer only from the results, and list the source paths you used. Say so when the wiki does not contain the answer."""

w = WorkspaceClient()


@tool
def search_wiki(query: str) -> list[dict]:
    """Search the company wiki for passages relevant to a query."""
    response = w.vector_search_indexes.query_index(
        index_name=os.environ["VECTOR_SEARCH_INDEX"],
        columns=["chunk_to_retrieve", "path"],
        query_text=query,
        query_type="HYBRID",
        num_results=5,
    )
    return [{"text": text, "path": path} for text, path, _score in response.result.data_array or []]


agent = create_agent(
    model=ChatDatabricks(endpoint=os.environ["LLM_ENDPOINT"]),
    tools=[search_wiki],
    system_prompt=SYSTEM_PROMPT,
)


@stream()
async def stream_handler(request: ResponsesAgentRequest) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    """Stream text deltas as the model writes, and completed items as each step finishes."""
    if request.context and request.context.conversation_id:
        # Groups the traces of one chat in MLflow.
        mlflow.update_current_trace(metadata={"mlflow.trace.session": request.context.conversation_id})
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


server = AgentServer("ResponsesAgent", enable_chat_proxy=True)
