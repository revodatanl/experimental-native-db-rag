# Databricks notebook source
# MAGIC %md
# MAGIC # Documents
# MAGIC
# MAGIC Turns the connector's raw pages into documents, then chunks:
# MAGIC
# MAGIC - `page_documents`: every current page as Markdown, with its chunks from
# MAGIC   `ai_prep_search`. Images and macros carry no text, so they drop out.
# MAGIC - `chunks`: one row per chunk, the vector index's source.
# MAGIC
# MAGIC The connector records every insert, update and delete on `pages` in its
# MAGIC change feed. `page_documents` streams that feed, so a run converts and chunks
# MAGIC only the pages that changed, and removes the pages that left the wiki.
# MAGIC `chunks` follows with Auto CDC from a snapshot, so only changed chunks
# MAGIC produce change events and the index embeds only those.

# COMMAND ----------

from markdownify import markdownify
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")

# ai_prep_search has no Python binding, so it goes through F.expr. It returns two
# texts per chunk: chunk_to_embed (for the index) and chunk_to_retrieve (for the
# model). CHUNKS is the part of its output this pipeline keeps.
CHUNKS = "array<struct<chunk_position: int, chunk_to_embed: string, chunk_to_retrieve: string>>"


@udf("string")
def to_markdown(xhtml: str | None) -> str:
    """Convert Confluence storage-format XHTML to Markdown."""
    return markdownify(xhtml or "", heading_style="ATX").strip()


@dp.temporary_view()
def page_changes() -> DataFrame:
    """Chunk each changed page; a page that left the wiki gets no content, which makes it a delete."""
    current = F.expr("_change_type != 'delete' AND status = 'current' AND isDeleted IS NOT TRUE")
    markdown = F.nullif(to_markdown("body.storage.value"), F.lit(""))
    return (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{catalog}.{schema}.pages")
        .where("_change_type != 'update_preimage'")
        .select(
            "id",
            "title",
            F.col("web_url").alias("path"),
            F.when(current, markdown).alias("content"),
            "_commit_version",
        )
        .withColumn("chunks", F.variant_get(F.expr("ai_prep_search(content)"), "$.document.contents", CHUNKS))
    )


dp.create_streaming_table("page_documents", comment="Current pages as Markdown, with their chunks.")
dp.create_auto_cdc_flow(
    target="page_documents",
    source="page_changes",
    keys=["id"],
    sequence_by="_commit_version",
    apply_as_deletes="content IS NULL",
    except_column_list=["_commit_version"],
)


@dp.temporary_view()
def chunk_snapshot() -> DataFrame:
    """Explode every document's chunks into rows with a stable id."""
    return (
        spark.read.table("page_documents")
        .select("id", "path", F.explode("chunks").alias("chunk"))
        .select(
            F.concat_ws("-", "id", "chunk.chunk_position").alias("chunk_id"),
            "path",
            "chunk.chunk_to_embed",
            "chunk.chunk_to_retrieve",
        )
    )


# Every run compares chunk_snapshot with chunks: new chunks are inserted, changed ones
# updated, missing ones deleted, unchanged ones left alone.
dp.create_streaming_table(
    "chunks",
    comment="One row per chunk, the vector index's source.",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
dp.create_auto_cdc_from_snapshot_flow(target="chunks", source="chunk_snapshot", keys=["chunk_id"])
