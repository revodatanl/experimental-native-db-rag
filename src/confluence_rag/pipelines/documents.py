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
# MAGIC
# MAGIC This notebook only declares the datasets. The transformations live in
# MAGIC `confluence_rag.documents`, installed in the pipeline's environment as the wheel
# MAGIC the bundle builds.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from confluence_rag import documents

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")


@dp.temporary_view()
def page_changes() -> DataFrame:
    """Chunk each changed page."""
    return documents.page_changes(spark.readStream.option("readChangeFeed", "true").table(f"{catalog}.{schema}.pages"))


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
    """Explode every document's chunks into rows."""
    return documents.chunk_snapshot(spark.read.table("page_documents"))


# Every run compares chunk_snapshot with chunks: new chunks are inserted, changed ones
# updated, missing ones deleted, unchanged ones left alone.
dp.create_streaming_table(
    "chunks",
    comment="One row per chunk, the vector index's source.",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
dp.create_auto_cdc_from_snapshot_flow(target="chunks", source="chunk_snapshot", keys=["chunk_id"])
