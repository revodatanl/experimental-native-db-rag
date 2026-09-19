# experimental-native-db-rag

Chat with your Confluence wiki on Databricks, built from native building blocks
only. Every pull request in this repository is one step; read them in order and
you have replayed the build.

```text
Confluence --Lakeflow Connect--> raw tables (pages, attachments)
                                       |
                            documents pipeline (one file)
        pages -> Markdown, image descriptions inlined where the image sits
        attachments -> ai_parse_document
                                       |
                              ai_prep_search -> chunks
                                       |
                          Vector Search index (Delta Sync)
                                       |
          one Databricks App: LangGraph agent with one tool + chat UI
```

| Step | What it adds |
| --- | --- |
| 1 | The project: `uv init` output plus `databricks.yml` with the two settings every deploy needs |

## Prerequisites

- [uv](https://docs.astral.sh/uv/) for the Python side.
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) 1.3 or newer, logged in:
  `databricks auth login --host https://<your-workspace> --profile <profile>`
- A workspace with serverless compute, Lakeflow Connect, Vector Search, and
  Databricks Apps enabled.
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
