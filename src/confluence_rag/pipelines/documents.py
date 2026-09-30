# Databricks notebook source
# MAGIC %md
# MAGIC # Documents
# MAGIC
# MAGIC Turns the connector's raw Confluence tables into documents:
# MAGIC
# MAGIC - **pages** become Markdown. Every image on a page is described by a vision
# MAGIC   model, and the description is inlined exactly where the image sits, so a
# MAGIC   diagram is retrievable in the context of the text around it.
# MAGIC - **attachments** (PDF, Word, PowerPoint) are downloaded once and parsed once
# MAGIC   with `ai_parse_document`.
# MAGIC
# MAGIC Expensive work (download, vision model, parsing) happens in streaming tables, so
# MAGIC each file is processed once. The two document tables are materialized views and
# MAGIC are recomputed on every run, which is how edits and deletions get in. A
# MAGIC re-uploaded version of an existing attachment needs a full refresh of this
# MAGIC pipeline. Chunking happens in the index task, which chunks only the documents
# MAGIC that changed.
# MAGIC
# MAGIC This notebook only declares the datasets. The transformations live in
# MAGIC `confluence_rag.documents`, installed in the pipeline's environment as the wheel
# MAGIC the bundle builds.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from confluence_rag import documents

raw = f"{spark.conf.get('catalog')}.{spark.conf.get('schema')}"
llm_endpoint = spark.conf.get("llm_endpoint")
download = documents.downloader(
    spark.conf.get("confluence_wiki_url"),
    spark.conf.get("confluence_username"),
    spark.conf.get("confluence_api_token"),  # resolved from the secret scope by the pipeline
)

# COMMAND ----------

# MAGIC %md ## Attachments: download each file once, describe each image once, parse each document once

# COMMAND ----------


@dp.expect_or_drop("downloaded", "content IS NOT NULL")
@dp.table(comment="Attachment bytes, downloaded once per new attachment.")
def attachment_files() -> DataFrame:
    """Stream new attachments from the connector table and download them.

    skipChangeCommits: the connector updates its table in place, and a stream
    cannot follow updates. New attachments flow through; a re-uploaded version
    of an existing attachment needs a full refresh of this pipeline.
    """
    attachments = spark.readStream.option("skipChangeCommits", "true").table(f"{raw}.attachments")
    return documents.attachment_files(attachments, download)


@dp.table(comment="One vision-model description per image attachment.")
def image_descriptions() -> DataFrame:
    """Describe each downloaded image once."""
    return documents.image_descriptions(spark.readStream.table("attachment_files"), llm_endpoint)


# A successful parse carries a VARIANT null in error_status, which is not SQL NULL.
@dp.expect("parsed", "is_variant_null(parsed:error_status)")
@dp.table(comment="ai_parse_document output, one row per attachment.")
def attachment_parsed() -> DataFrame:
    """Parse each downloaded office document once."""
    return documents.attachment_parsed(spark.readStream.table("attachment_files"))


# COMMAND ----------

# MAGIC %md ## Documents: recomputed every run

# COMMAND ----------


@dp.materialized_view(comment="Current pages as Markdown, image descriptions inlined.")
def page_documents() -> DataFrame:
    """Convert current pages to Markdown and inline their image descriptions."""
    return documents.page_documents(spark.read.table(f"{raw}.pages"), spark.read.table("image_descriptions"))


@dp.materialized_view(comment="Parsed attachments the wiki still shows.")
def attachment_documents() -> DataFrame:
    """Keep the parsed attachments that still exist in the wiki."""
    return documents.attachment_documents(spark.read.table("attachment_parsed"), spark.read.table(f"{raw}.attachments"))
