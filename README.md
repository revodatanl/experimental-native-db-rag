# native-db-rag

Chat with your Confluence wiki on Databricks, built from native building blocks
only.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) for the Python side.
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- A workspace with serverless compute, Lakeflow Connect, Vector Search, and Databricks Apps enabled.
- A Unity Catalog catalog you can create schemas in (default `main`).
- A Unity Catalog connection of type **Confluence** named `confluence`, created once in Catalog Explorer > Connections (OAuth needs a browser, so there is no CLI equivalent).
- A Confluence API token in a secret scope, for downloading attachments (the
  connector only ships attachment metadata). `put-secret` prompts for the value:

  ```bash
  databricks secrets create-scope confluence --profile <profile>
  databricks secrets put-secret confluence api-token --profile <profile>
  ```

## Deploy

Point the bundle at your wiki, then deploy:

```bash
databricks bundle validate --profile <profile>
databricks bundle deploy --profile <profile> \
  --var="confluence_wiki_url=https://<site>.atlassian.net/wiki" \
  --var="confluence_username=<you>@<company>.com"
```

Or change the two `CHANGE-ME` defaults in `databricks.yml` once and deploy
without the flags.

The first deploy also creates the vector search endpoint, which takes a few
minutes to come online. Deploying uploads the app's code but does not start
it; start it once (later deploys of new code need the same command):

```bash
databricks bundle run chat --profile <profile>
```

## Load the wiki

```bash
databricks bundle run refresh --profile <profile>
```

One job, three tasks:

1. **ingest**: the connector lands `pages`, `attachments`, `spaces` and a few
   smaller tables. Page bodies arrive in Confluence storage format (XHTML);
   attachments arrive as metadata plus a download link.
2. **documents**: the pipeline adds `page_documents` (Markdown, one row per
   current page, image descriptions inlined), `attachment_parsed` (the
   `ai_parse_document` output, one row per PDF, Word or PowerPoint file), and
   the two chunk tables from `ai_prep_search`. Files are downloaded and parsed
   once; a re-uploaded version of an existing attachment is picked up by a
   full refresh of the pipeline. Pages and their chunks are recomputed on
   every run.
3. **index**: merges the chunk tables into `chunks` (Change Data Feed on),
   creates the `chunks_index` Delta Sync index on the first run and syncs it on
   later runs, and grants the app read access to it. Only changed chunks
   produce change events, so only those are re-embedded.

Measured on a 1,300-page wiki with 1,500 attachments, first run: downloads 19
minutes, image descriptions 4 minutes, parsing 9 minutes, page chunking 20
minutes, 4,100 chunks in total. Downloads, descriptions and parsing happen
once; page chunking repeats every run. The initial embedding of the index runs
in the background after the job. The job is scheduled nightly and ships paused;
unpause it in the Jobs UI when you want the wiki to stay fresh.

## Use it

```bash
databricks apps get confluence-rag --profile <profile>   # prints the URL
```

Open the URL and ask something only your wiki knows. The app is Databricks'
[agent-langgraph](https://github.com/databricks/app-templates/tree/main/agent-langgraph)
template with one change, `agent_server/agent.py`: a LangGraph agent whose only
tool, `search_wiki`, is a `VectorSearchRetrieverTool` doing hybrid search on
the index. The model decides how to phrase the search, searches again when
results are thin, and cites the paths it used. Every answer is traced in the
MLflow experiment the bundle creates.

The chat UI is Databricks'
[e2e-chatbot-app-next](https://github.com/databricks/app-templates/tree/main/e2e-chatbot-app-next)
template, cloned by the start script when the app starts. It runs in ephemeral
mode: conversations live in the browser tab, nothing is stored.

Everyone in the organization can open it: the bundle grants `CAN_USE` to
`account users`, and users sign in with their Databricks SSO.

Run the agent locally with `cp .env.example .env`, fill it in, and
`uv run start-app`; `uv run preflight` sends one test request.

## What is not here, on purpose

- No prompt registry. The prompt is a constant in `agent_server/agent.py`;
  change it and redeploy.
- No custom chunker or tokenizer. `ai_prep_search` decides chunk boundaries.
- No chat history store. The UI's Lakebase mode is off.
- No evaluation loop yet.
- From the agent template: the MCP tool discovery, Lakebase setup and
  quickstart scripts, and the AI-assistant skill files were left out. The
  template has them if you want them back.

## Tear down

```bash
databricks bundle destroy --profile <profile>
```

This deletes everything the bundle owns, including the schema and its data.
