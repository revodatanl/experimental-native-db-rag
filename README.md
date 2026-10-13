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

## Load the wiki

```bash
databricks bundle run ingest --profile <profile>
```

The connector lands one table, `pages`, in the bundle's schema. Page bodies
arrive in Confluence storage format (XHTML). Images and attachments are left
out: the connector ships attachment metadata only, not the files.

## Tear down

```bash
databricks bundle destroy --profile <profile>
```

This deletes everything the bundle owns, including the schema and its data.
