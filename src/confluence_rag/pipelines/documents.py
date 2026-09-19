"""Turn the connector's raw Confluence tables into documents and chunks.

- Pages become Markdown. Every image on a page is described by a vision model, and
  the description is inlined exactly where the image sits, so a diagram is
  retrievable in the context of the text around it.
- Attachments (PDF, Word, PowerPoint) are downloaded once and parsed once with
  ai_parse_document.
- ai_prep_search splits pages and parsed attachments into chunks.

Expensive work (download, vision model, parsing) happens in streaming tables, so
each file is processed once. Everything after that is a materialized view and is
recomputed on every run, which is how edits and deletions get in. A re-uploaded
version of an existing attachment needs a full refresh of this pipeline.

The AI functions are SQL functions without a Python binding, so those calls go
through F.expr; everything else is the DataFrame API. `spark` is provided by the
pipeline runtime.
"""

import requests
from markdownify import markdownify
from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

raw = f"{spark.conf.get('catalog')}.{spark.conf.get('schema')}"
wiki_url = spark.conf.get("confluence_wiki_url").rstrip("/")
username = spark.conf.get("confluence_username")
api_token = spark.conf.get("confluence_api_token")  # resolved from the secret scope by the pipeline
llm_endpoint = spark.conf.get("llm_endpoint")

DOCUMENT_TYPES = [
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.ms-powerpoint",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
]
IMAGE_TYPES = ["image/png", "image/jpeg"]

IMAGE_PROMPT = (
    "Describe this image in 150 to 250 words for a technical knowledge base. "
    "Say what kind of image it is and what it is about, quote the labels, "
    "text and numbers that are visible, and explain how the elements relate "
    "to each other. Plain prose, no lists, nothing that is not visible."
)

# One <ac:image> block with the attachment filename inside it. The tempered dot
# `(?:(?!</ac:image>).)*?` keeps the match inside a single block; a plain `.*?`
# would run from an external image (no filename) into the next one and delete
# the page text in between.
IMAGE_ANCHOR = r'(?s)<ac:image[^>]*>(?:(?!</ac:image>).)*?ri:filename="([^"]*)"(?:(?!</ac:image>).)*?</ac:image>'


# ## The only two Python functions: download a file, convert XHTML to Markdown


@udf("binary")
def download(page_id: str, attachment_id: str) -> bytes | None:
    """Fetch one attachment through Confluence's public REST download endpoint.

    The connector's `downloadLink` points at an internal path that rejects API
    tokens; this endpoint accepts them and redirects to the file. Transient
    errors are retried, because a row that fails here is gone until a full refresh.
    """
    url = f"{wiki_url}/rest/api/content/{page_id}/child/attachment/{attachment_id}/download"
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    with requests.Session() as session:
        session.mount("https://", HTTPAdapter(max_retries=retry))
        response = session.get(url, auth=(username, api_token), timeout=60)
    return response.content if response.ok else None


@udf("string")
def to_markdown(xhtml: str | None) -> str:
    """Convert Confluence storage-format XHTML to Markdown."""
    return markdownify(xhtml or "", heading_style="ATX").strip()


def current(df: DataFrame) -> DataFrame:
    """Keep the rows the wiki still shows: status current and not soft-deleted."""
    return df.where((F.col("status") == "current") & ~F.coalesce("isDeleted", F.lit(False)))


# ## Attachments: download each file once


@dp.expect_or_drop("downloaded", "content IS NOT NULL")
@dp.table(comment="Attachment bytes, downloaded once per new attachment.")
def attachment_files() -> DataFrame:
    """Stream new attachments from the connector table and download them.

    skipChangeCommits: the connector updates its table in place, and a stream
    cannot follow updates. New attachments flow through; a re-uploaded version
    of an existing attachment needs a full refresh of this pipeline.
    """
    return (
        current(spark.readStream.option("skipChangeCommits", "true").table(f"{raw}.attachments"))
        .where(F.col("pageId").isNotNull() & F.col("mediaType").isin(DOCUMENT_TYPES + IMAGE_TYPES))
        .select("id", "pageId", "title", "mediaType", "fileSize", "web_url", "modifiedAt")
        .withColumn("content", download("pageId", "id"))
    )


# ## Images: one description per image, written for retrieval

# failOnError => false keeps one unreadable image from failing the run and returns
# a struct. Its text field is `result` (the docs call it `response`; verified on
# 1,376 images).
describe_image = F.expr(f"ai_query('{llm_endpoint}', '{IMAGE_PROMPT}', files => content, failOnError => false)")


@dp.table(comment="One vision-model description per image attachment.")
def image_descriptions() -> DataFrame:
    """Describe each downloaded image once."""
    return (
        spark.readStream.table("attachment_files")
        .where(F.col("mediaType").isin(IMAGE_TYPES))
        .select("pageId", F.col("title").alias("filename"), describe_image["result"].alias("description"))
    )


# ## Office documents: parse each file once


# A successful parse carries a VARIANT null in error_status, which is not SQL NULL.
@dp.expect("parsed", "is_variant_null(parsed:error_status)")
@dp.table(comment="ai_parse_document output, one row per attachment.")
def attachment_parsed() -> DataFrame:
    """Parse each downloaded office document once."""
    return (
        spark.readStream.table("attachment_files")
        .where(F.col("mediaType").isin(DOCUMENT_TYPES))
        .select(
            "id",
            "pageId",
            "title",
            "web_url",
            "modifiedAt",
            F.expr("ai_parse_document(content, map('version', '2.0'))").alias("parsed"),
        )
    )


# ## Pages: Markdown, with image descriptions inlined at their anchor
#
# regexp_replace turns every <ac:image> anchor into a `[Image: name]` marker before
# the Markdown conversion (the converter would otherwise drop the anchor); aggregate
# then walks the page's described images and appends each description to its marker.


def inline_description(body: Column, image: Column) -> Column:
    """Append one image's description to its `[Image: name]` marker in the body."""
    marker = F.concat(F.lit("[Image: "), image["filename"], F.lit("]"))
    return F.replace(body, marker, F.concat(marker, F.lit(" "), image["description"]))


@dp.materialized_view(comment="Current pages as Markdown, image descriptions inlined.")
def page_documents() -> DataFrame:
    """Convert current pages to Markdown and inline their image descriptions."""
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


# ## Chunks: ai_prep_search does the splitting
#
# One function replaces a tokenizer, a text splitter and their settings. It returns
# two texts per chunk: chunk_to_embed (enriched with the title and section headers,
# for the index) and chunk_to_retrieve (the plain chunk, for the model).


def chunks(documents: DataFrame, text: str, prefix: str) -> DataFrame:
    """Explode ai_prep_search output into one row per chunk with a stable id."""
    return (
        documents.withColumn("prep", F.expr(f"ai_prep_search({text})"))
        .select("id", "path", F.explode(F.variant_get("prep", "$.document.contents", "array<variant>")).alias("chunk"))
        .select(
            F.concat(F.lit(prefix), "id", F.lit("-"), F.variant_get("chunk", "$.chunk_position", "int")).alias(
                "chunk_id"
            ),
            "path",
            F.variant_get("chunk", "$.chunk_to_embed", "string").alias("chunk_to_embed"),
            F.variant_get("chunk", "$.chunk_to_retrieve", "string").alias("chunk_to_retrieve"),
        )
    )


@dp.materialized_view(comment="Page chunks from ai_prep_search; recomputed with the pages.")
def page_chunks() -> DataFrame:
    """Chunk the current pages."""
    return chunks(spark.read.table("page_documents"), "content", "page-")


@dp.materialized_view(comment="Attachment chunks from ai_prep_search; only attachments the wiki still shows.")
def attachment_chunks() -> DataFrame:
    """Chunk the parsed attachments that still exist in the wiki.

    attachment_parsed is a streaming table and never forgets a row, so the semi-join
    to the connector's current attachments is what lets deleted attachments leave the
    index on the next run.
    """
    live = current(spark.read.table(f"{raw}.attachments")).select("id")
    parsed = spark.read.table("attachment_parsed").join(live, "id", "left_semi").withColumn("path", F.col("web_url"))
    return chunks(parsed, "parsed", "attachment-")
