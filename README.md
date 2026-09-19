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

## Load the wiki

```bash
databricks bundle run ingest --profile <profile>
databricks bundle run documents --profile <profile>
```

The connector lands `pages`, `attachments`, `spaces` and a few smaller tables
in the bundle's schema. Page bodies arrive in Confluence storage format
(XHTML); attachments arrive as metadata plus a download link.

The documents pipeline adds `page_documents` (Markdown, one row per current
page, image descriptions inlined) and `attachment_parsed` (the
`ai_parse_document` output, one row per PDF, Word or PowerPoint file). Files
are downloaded and parsed once; a re-uploaded version of an existing attachment
is picked up by a full refresh of the pipeline. Measured on a 1,300-page wiki
with 1,500 attachments: downloads 19 minutes, image descriptions 4 minutes,
parsing 9 minutes, all on the first run only.

## Tear down

```bash
databricks bundle destroy --profile <profile>
```

This deletes everything the bundle owns, including the schema and its data.
