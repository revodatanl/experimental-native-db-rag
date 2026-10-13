# native-db-rag

Chat with your Confluence wiki on Databricks, built from native building blocks only.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) for the Python side.
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- A workspace with serverless compute, Lakeflow Connect, Vector Search, and Databricks Apps enabled.
- A Unity Catalog catalog you can create schemas in (default `main`).
- A Unity Catalog connection of type **Confluence** (here named `confluence`), created once in Catalog Explorer > Connections (OAuth needs a browser, so there is no CLI equivalent).

## Deploy

```bash
databricks bundle validate --profile <profile>
databricks bundle deploy --profile <profile>
```

## Load the wiki

```bash
databricks bundle run ingest --profile <profile>
databricks bundle run documents --profile <profile>
```

The connector lands `pages`, `attachments`, `spaces` and a few smaller tables in the bundle's schema. Page bodies arrive in Confluence storage format (XHTML); attachments arrive as metadata plus a download link.

The documents pipeline streams the connector's change feed into `page_documents` (Markdown and chunks from `ai_prep_search`, one row per current page) and keeps `chunks` (one row per chunk, Change Data Feed on) in step with Auto CDC. A run converts and chunks only the pages that changed, removes the pages that left the wiki, and touches only the chunks that changed. Only pages are used: the connector does not ship attachment files, so images and attachments are left out.

## Tear down

```bash
databricks bundle destroy --profile <profile>
```

This deletes everything the bundle owns, including the schema and its data.
