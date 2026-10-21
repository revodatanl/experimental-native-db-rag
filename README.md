# native-db-rag

Chat with your Confluence wiki on Databricks, built from native building blocks
only.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) for the Python side.
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- A workspace with serverless compute, Lakeflow Connect, Vector Search, and Databricks Apps enabled.
- A Unity Catalog catalog where you have `USE CATALOG` and `CREATE SCHEMA`
  (default `main`). The app searches the index as the signed-in user, so the
  app's users need `USE CATALOG` on it too: `main` grants that to all account
  users by default; on another catalog, its owner grants it to the group once.
- A Unity Catalog connection of type **Confluence** named `confluence`, created once in Catalog Explorer > Connections (OAuth needs a browser, so there is no CLI equivalent).
- User authorization for Databricks Apps, with the `vector-search` scope allowed.
  A workspace admin controls which scopes apps may request.

## Deploy

```bash
databricks bundle validate --profile <profile>
databricks bundle deploy --profile <profile>
```

The variables in `databricks.yml` pick the catalog, the schema, the connection
and the group of users; override one with `--var="users=<group>"`.

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

Open the URL that `bundle run chat` printed, consent once to the app searching
on your behalf, and ask something only your wiki knows. The agent is one file, `src/confluence_rag/agent.py`: a LangGraph agent
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

The search runs as the signed-in user: Databricks Apps forwards a token scoped
to vector search with each request, so Unity Catalog applies the user's own
privileges and the app needs no grant of its own on the catalog. Everyone in
the `users` group (default `account users`) can open it: the bundle grants the
group `CAN_USE` on the app and `USE SCHEMA` and `SELECT` on the schema, and
users sign in with their Databricks SSO.

## Tear down

```bash
databricks bundle destroy --profile <profile>
```

This deletes everything the bundle owns, including the schema and its data.
