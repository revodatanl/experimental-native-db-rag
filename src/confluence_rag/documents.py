"""Transformations for the documents pipeline; the pipeline itself is pipelines/documents.py.

Every function here takes DataFrames or configuration in and returns a DataFrame
or Column, so it can be tested with a local Spark session. Reading and writing
tables, and the dataset decorators, stay in the pipeline.

The AI functions are SQL functions without a Python binding, so those calls go
through F.expr; everything else is the DataFrame API.
"""

import requests
from markdownify import markdownify
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.udf import UserDefinedFunction
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

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

IMAGE_ANCHOR = r'(?s)<ac:image[^>]*>(?:(?!</ac:image>).)*?ri:filename="([^"]*)"(?:(?!</ac:image>).)*?</ac:image>'


def current(df: DataFrame) -> DataFrame:
    """Keep the rows the wiki still shows: status current and not soft-deleted."""
    return df.where((F.col("status") == "current") & ~F.coalesce("isDeleted", F.lit(False)))


def downloader(wiki_url: str, username: str, api_token: str) -> UserDefinedFunction:
    """Build the UDF that fetches one attachment through Confluence's public REST download endpoint.

    The connector's `downloadLink` points at an internal path that rejects API
    tokens; this endpoint accepts them and redirects to the file. Transient
    errors are retried, because a row that fails here is gone until a full refresh.
    """
    base = wiki_url.rstrip("/")

    @udf("binary")
    def download(page_id: str, attachment_id: str) -> bytes | None:
        url = f"{base}/rest/api/content/{page_id}/child/attachment/{attachment_id}/download"
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        with requests.Session() as session:
            session.mount("https://", HTTPAdapter(max_retries=retry))
            response = session.get(url, auth=(username, api_token), timeout=60)
        return response.content if response.ok else None

    return download


@udf("string")
def to_markdown(xhtml: str | None) -> str:
    """Convert Confluence storage-format XHTML to Markdown."""
    return markdownify(xhtml or "", heading_style="ATX").strip()


def attachment_files(attachments: DataFrame, download: UserDefinedFunction) -> DataFrame:
    """Select the attachments worth downloading and add their bytes as `content`."""
    return (
        current(attachments)
        .where(F.col("pageId").isNotNull() & F.col("mediaType").isin(DOCUMENT_TYPES + IMAGE_TYPES))
        .select("id", "pageId", "title", "mediaType", "web_url", "modifiedAt")
        .withColumn("content", download("pageId", "id"))
    )


def image_descriptions(files: DataFrame, llm_endpoint: str) -> DataFrame:
    """Describe each image with a vision model: one row per image, `description` may be null.

    failOnError => false keeps one unreadable image from failing the run and returns
    a struct. Its text field is `result` (the docs call it `response`; verified on
    1,376 images).
    """
    describe = F.expr(f"ai_query('{llm_endpoint}', '{IMAGE_PROMPT}', files => content, failOnError => false)")
    return files.where(F.col("mediaType").isin(IMAGE_TYPES)).select(
        "pageId", F.col("title").alias("filename"), describe["result"].alias("description")
    )


def attachment_parsed(files: DataFrame) -> DataFrame:
    """Parse each office document with ai_parse_document into a `parsed` variant."""
    return files.where(F.col("mediaType").isin(DOCUMENT_TYPES)).select(
        "id",
        "pageId",
        "title",
        "web_url",
        "modifiedAt",
        F.expr("ai_parse_document(content, map('version', '2.0'))").alias("parsed"),
    )


def page_documents(pages: DataFrame, descriptions: DataFrame) -> DataFrame:
    """Convert current pages to Markdown with each image's description inlined at its anchor.

    regexp_replace turns every <ac:image> anchor into a `[Image: name]` marker before
    the Markdown conversion (the converter would otherwise drop the anchor); aggregate
    then walks the page's described images and appends each description to its marker.
    """

    def inline_description(body: Column, image: Column) -> Column:
        marker = F.concat(F.lit("[Image: "), image["filename"], F.lit("]"))
        return F.replace(body, marker, F.concat(marker, F.lit(" "), image["description"]))

    images = (
        descriptions.where(F.col("description").isNotNull())
        .groupBy("pageId")
        .agg(F.collect_list(F.struct("filename", "description")).alias("images"))
    )
    markdown = (
        current(pages)
        .withColumn("body", to_markdown(F.regexp_replace("body.storage.value", IMAGE_ANCHOR, "<p>[Image: $1]</p>")))
        .where(F.length("body") > 0)
    )
    return (
        markdown.join(images, markdown["id"] == images["pageId"], "left")
        .withColumn("content", F.aggregate(F.coalesce("images", F.array()), F.col("body"), inline_description))
        .select("id", "title", F.col("web_url").alias("path"), "modifiedAt", "content")
    )


def chunks(documents: DataFrame, text: str, prefix: str) -> DataFrame:
    """Split `text` with ai_prep_search into one row per chunk with a stable `chunk_id`.

    One function replaces a tokenizer, a text splitter and their settings. It returns
    two texts per chunk: chunk_to_embed (enriched with the title and section headers,
    for the index) and chunk_to_retrieve (the plain chunk, for the model).
    """
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


def attachment_chunks(parsed: DataFrame, attachments: DataFrame) -> DataFrame:
    """Chunk the parsed attachments that the wiki still shows.

    The parsed table is a streaming table and never forgets a row, so the semi-join
    to the connector's current attachments is what lets deleted attachments leave the
    index on the next run.
    """
    live = current(attachments).select("id")
    return chunks(parsed.join(live, "id", "left_semi").withColumn("path", F.col("web_url")), "parsed", "attachment-")
