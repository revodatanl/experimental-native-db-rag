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
# MAGIC Expensive work (download, vision model, document parsing) happens in
# MAGIC streaming tables, so each file is processed exactly once. Pages are cheap
# MAGIC and are recomputed on every run, which is how edits and deletions get in.
# MAGIC
# MAGIC The AI functions (`ai_query`, `ai_parse_document`) are SQL functions without
# MAGIC a Python binding, so those calls go through `F.expr`; everything else is the
# MAGIC DataFrame API.

# COMMAND ----------

import requests
from markdownify import markdownify
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")
wiki_url = spark.conf.get("confluence_wiki_url").rstrip("/")
username = spark.conf.get("confluence_username")
api_token = dbutils.secrets.get(spark.conf.get("secret_scope"), spark.conf.get("secret_key"))
vision_endpoint = spark.conf.get("vision_endpoint")

raw = f"{catalog}.{schema}"

DOCUMENT_TYPES = [
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.ms-powerpoint",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
]
IMAGE_TYPES = ["image/png", "image/jpeg"]

# COMMAND ----------

# ## The only two functions in this repository
#
# The connector does not ship attachment bytes and no SQL function can fetch
# binaries, so one download function stays. The Markdown conversion is one
# library call.


@udf("binary")
def download(page_id, attachment_id):
    """Fetch one attachment through Confluence's public REST download endpoint.

    The connector's `downloadLink` points at an internal path that rejects API
    tokens; this endpoint accepts them and redirects to the file.
    """
    url = f"{wiki_url}/rest/api/content/{page_id}/child/attachment/{attachment_id}/download"
    response = requests.get(url, auth=(username, api_token), timeout=60)
    return response.content if response.ok else None


@udf("string")
def to_markdown(xhtml):
    """Convert Confluence storage-format XHTML to Markdown."""
    return markdownify(xhtml or "", heading_style="ATX").strip()


def current(df):
    """Rows the wiki still shows: status current and not soft-deleted."""
    return df.where((F.col("status") == "current") & ~F.coalesce("isDeleted", F.lit(False)))


# COMMAND ----------

# ## Attachments: download each file once


@dp.expect_or_drop("downloaded", "content IS NOT NULL")
@dp.table(comment="Attachment bytes, downloaded once per new attachment.")
def attachment_files():
    # skipChangeCommits: the connector updates its table in place, and a stream
    # cannot follow updates. New attachments flow through; a re-uploaded version
    # of an existing attachment needs a full refresh of this pipeline.
    return (
        current(spark.readStream.option("skipChangeCommits", "true").table(f"{raw}.attachments"))
        .where(F.col("pageId").isNotNull() & F.col("mediaType").isin(DOCUMENT_TYPES + IMAGE_TYPES))
        .select("id", "pageId", "title", "mediaType", "fileSize", "web_url", "modifiedAt")
        .withColumn("content", download("pageId", "id"))
    )


# COMMAND ----------

# ## Images: one description per image, written for retrieval

IMAGE_PROMPT = (
    "Describe this image in 150 to 250 words for a technical knowledge base. "
    "Say what kind of image it is and what it is about, quote the labels, "
    "text and numbers that are visible, and explain how the elements relate "
    "to each other. Plain prose, no lists, nothing that is not visible."
)

# failOnError => false keeps one unreadable image from failing the run and
# returns a struct; its text field is `result` (the docs call it `response`).
describe_image = F.expr(
    f"ai_query('{vision_endpoint}', '{IMAGE_PROMPT}', files => content, failOnError => false)"
)


@dp.table(comment="One vision-model description per image attachment.")
def image_descriptions():
    return (
        spark.readStream.table("attachment_files")
        .where(F.col("mediaType").isin(IMAGE_TYPES))
        .select("pageId", F.col("title").alias("filename"), describe_image["result"].alias("description"))
    )


# COMMAND ----------

# ## Office documents: parse each file once

# A successful parse carries a VARIANT null in error_status, which is not SQL NULL.
@dp.expect("parsed", "is_variant_null(parsed:error_status)")
@dp.table(comment="ai_parse_document output, one row per attachment.")
def attachment_parsed():
    return (
        spark.readStream.table("attachment_files")
        .where(F.col("mediaType").isin(DOCUMENT_TYPES))
        .select(
            "id", "pageId", "title", "web_url", "modifiedAt",
            F.expr("ai_parse_document(content, map('version', '2.0'))").alias("parsed"),
        )
    )


# COMMAND ----------

# ## Pages: Markdown, with image descriptions inlined at their anchor
#
# `regexp_replace` turns every `<ac:image>` anchor into a `[Image: name]` marker
# before the Markdown conversion (the converter would otherwise drop the anchor);
# `aggregate` then walks the page's described images and appends each
# description to its marker.

IMAGE_ANCHOR = r'(?s)<ac:image[^>]*>.*?ri:filename="([^"]*)".*?</ac:image>'


def inline_description(body, image):
    marker = F.concat(F.lit("[Image: "), image["filename"], F.lit("]"))
    return F.replace(body, marker, F.concat(marker, F.lit(" "), image["description"]))


@dp.materialized_view(comment="Current pages as Markdown, image descriptions inlined.")
def page_documents():
    images = (
        spark.read.table("image_descriptions")
        .where(F.col("description").isNotNull())
        .groupBy("pageId")
        .agg(F.collect_list(F.struct("filename", "description")).alias("images"))
    )
    pages = (
        current(spark.read.table(f"{raw}.pages"))
        .withColumn("body", to_markdown(F.regexp_replace("body.storage.value", IMAGE_ANCHOR, "<p>[Image: $1]</p>")))
        .where(F.length("body") > 0)
    )
    return (
        pages.join(images, pages["id"] == images["pageId"], "left")
        .withColumn("content", F.aggregate(F.coalesce("images", F.array()), F.col("body"), inline_description))
        .select("id", "title", F.col("web_url").alias("path"), "modifiedAt", "content")
    )


# COMMAND ----------

# ## Chunks: `ai_prep_search` does the splitting
#
# One function replaces a tokenizer, a text splitter and their settings. It
# returns two texts per chunk: `chunk_to_embed` (enriched with the title and
# section headers, for the index) and `chunk_to_retrieve` (the plain chunk,
# for the model). Page chunks are recomputed with the pages; attachment
# chunks are computed once per file.


def chunks(documents, text, prefix):
    """Explode ai_prep_search output into one row per chunk with a stable id."""
    return (
        documents.withColumn("prep", F.expr(f"ai_prep_search({text})"))
        .select("id", "path", F.explode(F.variant_get("prep", "$.document.contents", "array<variant>")).alias("chunk"))
        .select(
            F.concat(F.lit(prefix), "id", F.lit("-"), F.variant_get("chunk", "$.chunk_position", "int")).alias("chunk_id"),
            "path",
            F.variant_get("chunk", "$.chunk_to_embed", "string").alias("chunk_to_embed"),
            F.variant_get("chunk", "$.chunk_to_retrieve", "string").alias("chunk_to_retrieve"),
        )
    )


@dp.materialized_view(comment="Page chunks from ai_prep_search; recomputed with the pages.")
def page_chunks():
    return chunks(spark.read.table("page_documents"), "content", "page-")


@dp.table(comment="Attachment chunks from ai_prep_search; computed once per file.")
def attachment_chunks():
    documents = spark.readStream.table("attachment_parsed").withColumn("path", F.col("web_url"))
    return chunks(documents, "parsed", "attachment-")
