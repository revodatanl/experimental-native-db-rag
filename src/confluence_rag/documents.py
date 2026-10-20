"""Transformations for the documents pipeline; the pipeline itself is pipelines/documents.py.

Every function here takes DataFrames in and returns a DataFrame or Column, so it can
be tested with a local Spark session. Reading tables and the dataset decorators stay
in the pipeline.

ai_prep_search has no Python binding, so it goes through F.expr. It returns two texts
per chunk: chunk_to_embed (for the index) and chunk_to_retrieve (for the model).
"""

from markdownify import markdownify
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

# The part of ai_prep_search's output the pipeline keeps.
CHUNKS = "array<struct<chunk_position: int, chunk_to_embed: string, chunk_to_retrieve: string>>"


@udf("string")
def to_markdown(xhtml: str | None) -> str:
    """Convert Confluence storage-format XHTML to Markdown; images and macros carry no text, so they drop out."""
    return markdownify(xhtml or "", heading_style="ATX").strip()


def page_changes(changes: DataFrame) -> DataFrame:
    """Chunk each page in the change feed of `pages`: `id`, `title`, `path`, `content`, `_commit_version`, `chunks`.

    A page that left the wiki gets no content, which makes it a delete.
    """
    current = F.expr("_change_type != 'delete' AND status = 'current' AND isDeleted IS NOT TRUE")
    markdown = F.nullif(to_markdown("body.storage.value"), F.lit(""))
    return (
        changes.where("_change_type != 'update_preimage'")
        .select(
            "id",
            "title",
            F.col("web_url").alias("path"),
            F.when(current, markdown).alias("content"),
            "_commit_version",
        )
        .withColumn("chunks", F.variant_get(F.expr("ai_prep_search(content)"), "$.document.contents", CHUNKS))
    )


def chunk_snapshot(documents: DataFrame) -> DataFrame:
    """Explode every document's chunks into rows with a stable `chunk_id`."""
    return documents.select("id", "path", F.explode("chunks").alias("chunk")).select(
        F.concat_ws("-", "id", "chunk.chunk_position").alias("chunk_id"),
        "path",
        "chunk.chunk_to_embed",
        "chunk.chunk_to_retrieve",
    )
