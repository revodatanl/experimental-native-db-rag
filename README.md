# native-db-rag

Chat with your Confluence wiki on Databricks, built from native building blocks
only.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) for the Python side.
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- A workspace with serverless compute, Lakeflow Connect, Vector Search, and Databricks Apps enabled.
- A Unity Catalog catalog you can create schemas in (default `main`). The app's
  service principal needs `USE CATALOG` on it.
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

The principal exists once the app does, so grant it after the first deploy:

```bash
databricks apps get confluence-rag --profile <profile> -o json | grep service_principal_client_id
databricks grants update catalog <catalog> --profile <profile> \
  --json '{"changes": [{"principal": "<service_principal_client_id>", "add": ["USE_CATALOG"]}]}'
```

`bundle destroy` deletes the principal with the app, so a later deploy needs
the grant again. The schema grants need no such step: the bundle applies them
on every deploy.

## Load the wiki

```bash
databricks bundle run refresh --profile <profile>
```

One job, three tasks:

1. **ingest**: the connector lands `pages`, `attachments`, `spaces` and a few
   smaller tables in the bundle's schema. Page bodies arrive in Confluence
   storage format (XHTML); attachments arrive as metadata plus a download link.
2. **documents**: the pipeline streams the connector's change feed into
   `page_documents` (Markdown and chunks from `ai_prep_search`, one row per
   current page) and keeps `chunks` (one row per chunk, Change Data Feed on) in
   step with Auto CDC. A run converts and chunks only the pages that changed,
   removes the pages that left the wiki, and touches only the chunks that
   changed. Only pages are used: the connector does not ship attachment files,
   so images and attachments are left out.
3. **index**: creates the `chunks_index` Delta Sync index on the first run and
   syncs it on later runs. Only changed chunks produce change events, so only
   those are re-embedded.

On a 1,300-page wiki the first run chunks for about 20 minutes; later runs chunk
only the pages that changed. The initial embedding of the index runs in the
background after the job. The job is scheduled nightly and ships paused;
unpause it in the Jobs UI when you want the wiki to stay fresh.

## Use it

Open the URL that `bundle run chat` printed and ask something only your wiki
knows. The agent is one file, `src/confluence_rag/agent.py`: a LangGraph agent
whose only tool, `search_wiki`, runs a hybrid search on the index and returns
each hit's `chunk_to_retrieve` and path. The model decides how to phrase the
search, searches again when results are thin, and cites the paths it used.
The search, its results, and the answer appear as each step finishes; every
answer is traced in the MLflow experiment the bundle creates.

The server is MLflow's `AgentServer`, from Databricks'
[agent-langgraph](https://github.com/databricks/app-templates/tree/main/agent-langgraph)
template. The chat UI is Databricks'
[e2e-chatbot-app-next](https://github.com/databricks/app-templates/tree/main/e2e-chatbot-app-next)
template, cloned from GitHub and built with npm when the app starts, so the
first start takes a few minutes and needs outbound access to GitHub.

The app searches as its own service principal: the bundle grants it `USE SCHEMA`
and `SELECT` on the schema, which covers the index and anything added to the
schema later. Everyone in the organization can open it: the bundle grants
`CAN_USE` to `account users`, and users sign in with their Databricks SSO.

## Tear down

```bash
databricks bundle destroy --profile <profile>
```

This deletes everything the bundle owns, including the schema and its data.
