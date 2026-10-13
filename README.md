# native-db-rag

Chat with your Confluence wiki on Databricks, built from native building blocks
only.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) for the Python side.
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- A workspace with serverless compute, Lakeflow Connect, Vector Search, and Databricks Apps enabled.
- A Unity Catalog catalog you can create schemas in (default `main`).
- A Unity Catalog connection of type **Confluence** named `confluence`, created once in Catalog Explorer > Connections (OAuth needs a browser, so there is no CLI equivalent).

## Deploy

```bash
databricks bundle validate --profile <profile>
databricks bundle deploy --profile <profile>
```

The first deploy also creates the vector search endpoint, which takes a few
minutes to come online. Deploying uploads the app's code but does not start
it; start it once (later deploys of new code need the same command):

```bash
databricks bundle run chat --profile <profile>
```

The default target runs in development mode, which prefixes every resource
name with your user name: the schema becomes `dev_<you>_confluence_rag`, the
app `dev-<you>-confluence-rag`, and so on. The bundle wires the prefixed names
through, so nothing else changes.

## Load the wiki

```bash
databricks bundle run refresh --profile <profile>
```

One job, two tasks:

1. **ingest**: the connector lands `pages`, `attachments`, `spaces` and a few
   smaller tables in the bundle's schema. Page bodies arrive in Confluence
   storage format (XHTML); attachments arrive as metadata plus a download link.
   Only `pages` is used: the connector does not ship attachment files, so
   images and attachments are left out.
2. **index**: converts the current pages to Markdown, chunks them with
   `ai_prep_search` into `chunks` (Change Data Feed on), creates the
   `chunks_index` Delta Sync index on the first run and syncs it on later runs,
   and grants the app read access to it. Each chunk keeps a hash of its page; a
   run deletes the chunks of changed and deleted pages and chunks only the new
   and changed ones, so only their chunks are re-embedded.

On a 1,300-page wiki the first run chunks for about 20 minutes; later runs
chunk only what changed. The initial embedding of the index runs in the
background after the job. The job is scheduled nightly and ships paused;
unpause it in the Jobs UI when you want the wiki to stay fresh.

## Use it

```bash
databricks apps list --profile <profile>   # the app and its URL
```

Open the URL and ask something only your wiki knows. The agent is one file,
`src/confluence_rag/agent/agent.py`: a LangGraph agent whose only tool,
`search_wiki`, is a `VectorSearchRetrieverTool` doing hybrid search on the
index. The model decides how to phrase the search, searches again when results
are thin, and cites the paths it used. Answers stream token by token; every
answer is traced in the MLflow experiment the bundle creates.

The server is MLflow's `AgentServer`, from Databricks'
[agent-langgraph](https://github.com/databricks/app-templates/tree/main/agent-langgraph)
template. The chat UI is Databricks'
[e2e-chatbot-app-next](https://github.com/databricks/app-templates/tree/main/e2e-chatbot-app-next)
template, cloned from GitHub and built with npm when the app starts, so the
first start takes a few minutes and needs outbound access to GitHub. It runs in
ephemeral mode: conversations live in the browser tab, nothing is stored.

Everyone in the organization can open it: the bundle grants `CAN_USE` to
`account users`, and users sign in with their Databricks SSO.

Run the agent locally with `cp .env.example .env`, fill it in, and
`uv run --env-file .env start-app`.

## What is not here, on purpose

- No images or attachments. The connector does not ship their files, so
  reading them needs an API token, a download step, a vision model and
  `ai_parse_document`. Text only keeps it to one job with two tasks.
- No prompt registry. The prompt is a constant in `agent/agent.py`; change
  it and redeploy.
- No custom chunker or tokenizer. `ai_prep_search` decides chunk boundaries.
- No chat history store. The UI's Lakebase mode is off.
- No evaluation loop yet.
- From the agent template: the MCP tool discovery, Lakebase setup, the
  pre-flight and quickstart scripts, and the AI-assistant skill files were left
  out. The template has them if you want them back.

## Tear down

```bash
databricks bundle destroy --profile <profile>
```

This deletes everything the bundle owns, including the schema and its data.
