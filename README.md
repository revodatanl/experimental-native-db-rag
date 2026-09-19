# native-db-rag

Chat with your Confluence wiki on Databricks, built from native building blocks only.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) for the Python side.
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- A workspace with serverless compute, Lakeflow Connect, Vector Search, and Databricks Apps enabled.
- A Unity Catalog catalog you can create schemas in (default `main`).

## Deploy

```bash
databricks bundle validate --profile <profile>
databricks bundle deploy --profile <profile>
```

## Tear down

```bash
databricks bundle destroy --profile <profile>
```

This deletes everything the bundle owns, including the schema and its data.
