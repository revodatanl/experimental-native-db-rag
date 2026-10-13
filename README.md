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
minutes to come online.

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
   `ai_prep_search` into `chunks` (Change Data Feed on), and creates the
   `chunks_index` Delta Sync index on the first run and syncs it on later runs.
   Each chunk keeps a hash of its page; a run deletes the chunks of changed and
   deleted pages and chunks only the new and changed ones, so only their chunks
   are re-embedded.

On a 1,300-page wiki the first run chunks for about 20 minutes; later runs
chunk only what changed. The initial embedding of the index runs in the
background after the job. The job is scheduled nightly and ships paused;
unpause it in the Jobs UI when you want the wiki to stay fresh.

## Tear down

```bash
databricks bundle destroy --profile <profile>
```

This deletes everything the bundle owns, including the schema and its data.
