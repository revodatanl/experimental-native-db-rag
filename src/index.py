# Databricks notebook source
# MAGIC %md
# MAGIC # Index
# MAGIC
# MAGIC Runs after the documents pipeline: creates the Delta Sync vector index on
# MAGIC `chunks` on the first run, syncs it on every later run. The index is created
# MAGIC here rather than in the bundle because it cannot exist before its source
# MAGIC table does, and the pipeline creates that table on its first run.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    PipelineType,
    VectorIndexType,
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
endpoint = dbutils.widgets.get("vector_search_endpoint")

chunks_table = f"{catalog}.{schema}.chunks"
index_name = f"{catalog}.{schema}.chunks_index"
# AI Search does not accept Unity Gateway names yet; later: system.ai.qwen3-embedding-0-6b.
embedding_endpoint = "databricks-qwen3-embedding-0-6b"

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
