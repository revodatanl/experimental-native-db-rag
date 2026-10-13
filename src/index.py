# Databricks notebook source
# MAGIC %md
# MAGIC # Index
# MAGIC
# MAGIC Runs after ingestion, as the last task of the refresh job:
# MAGIC
# MAGIC 1. Convert the current pages to Markdown. Images and macros carry no text,
# MAGIC    so they drop out.
# MAGIC 2. Chunk them with `ai_prep_search` into one `chunks` table with Change Data
# MAGIC    Feed on. Every chunk carries its page's `version`, a hash of the page's
# MAGIC    path, title and Markdown. A run deletes the chunks whose version is gone
# MAGIC    and chunks only the pages whose version is new, so only new and changed
# MAGIC    pages are chunked and embedded. The path carries the page title, so a
# MAGIC    rename is a change too.
# MAGIC 3. Create the Delta Sync vector index on the first run, sync it on every
# MAGIC    later run. The index is created here rather than in the bundle because
# MAGIC    it cannot exist before its source table does, and only running code can
# MAGIC    create the table.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    PipelineType,
    VectorIndexType,
)
from markdownify import markdownify
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

catalog = dbutils.widgets.get("catalog")
schema = f"{catalog}.{dbutils.widgets.get('schema')}"
endpoint = dbutils.widgets.get("vector_search_endpoint")

chunks_table = f"{schema}.chunks"
index_name = f"{schema}.chunks_index"
embedding_endpoint = "databricks-gte-large-en"

# COMMAND ----------

# ## Pages to Markdown


@udf("string")
def to_markdown(xhtml: str | None) -> str:
    """Convert Confluence storage-format XHTML to Markdown."""
    return markdownify(xhtml or "", heading_style="ATX").strip()


pages = spark.read.table(f"{schema}.pages")
current = pages.where((F.col("status") == "current") & ~F.coalesce("isDeleted", F.lit(False)))
documents = (
    current.select("id", "title", F.col("web_url").alias("path"), to_markdown("body.storage.value").alias("content"))
    .where(F.length("content") > 0)
    .withColumn("version", F.md5(F.concat_ws("\n", "path", "title", "content")))
)

# COMMAND ----------

# ## Chunk new and changed pages
#
# DELETE has no DataFrame API, so it stays SQL. ai_prep_search has no Python
# binding, so it goes through F.expr. It returns two texts per chunk:
# chunk_to_embed (the chunk with context an LLM writes, for the index) and
# chunk_to_retrieve (the plain chunk, for the model). It leaves the title out of
# Markdown input, so the title is prepended to chunk_to_embed here.

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {chunks_table} (
        chunk_id STRING NOT NULL,
        version STRING NOT NULL,
        path STRING,
        chunk_to_embed STRING,
        chunk_to_retrieve STRING
    )
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
documents.select("version").createOrReplaceTempView("versions")
spark.sql(f"DELETE FROM {chunks_table} WHERE version NOT IN (SELECT version FROM versions)")

known = spark.read.table(chunks_table).select("version")
title = F.concat(F.lit("Document Title: "), "title")
chunks = (
    documents.join(known, "version", "left_anti")
    .withColumn("prep", F.expr("ai_prep_search(content)"))
    .select(
        "id",
        "version",
        "path",
        "title",
        F.explode(F.variant_get("prep", "$.document.contents", "array<variant>")).alias("chunk"),
    )
    .select(
        F.concat("id", F.lit("-"), F.variant_get("chunk", "$.chunk_position", "int")).alias("chunk_id"),
        "version",
        "path",
        F.concat_ws("\n", title, F.variant_get("chunk", "$.chunk_to_embed", "string")).alias("chunk_to_embed"),
        F.variant_get("chunk", "$.chunk_to_retrieve", "string").alias("chunk_to_retrieve"),
    )
)
chunks.write.mode("append").saveAsTable(chunks_table)

# COMMAND ----------

# ## Create or sync the index

w = WorkspaceClient()
try:
    w.vector_search_indexes.create_index(
        name=index_name,
        endpoint_name=endpoint,
        primary_key="chunk_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=chunks_table,
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[
                EmbeddingSourceColumn(name="chunk_to_embed", embedding_model_endpoint_name=embedding_endpoint)
            ],
        ),
    )
    print(f"Created {index_name}; the first sync starts on its own.")
except ResourceAlreadyExists:
    w.vector_search_indexes.sync_index(index_name=index_name)
    print(f"Sync triggered for {index_name}.")
