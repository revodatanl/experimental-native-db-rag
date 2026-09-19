# Databricks notebook source
# MAGIC %md
# MAGIC # Index
# MAGIC
# MAGIC Runs after the documents pipeline. Two jobs:
# MAGIC
# MAGIC 1. Merge the pipeline's chunk tables into one `chunks` table with Change
# MAGIC    Data Feed on. A merge, not an overwrite: unchanged chunks produce no
# MAGIC    change events, so the index embeds only what changed.
# MAGIC 2. Create the Delta Sync vector index on the first run, sync it on every
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

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
endpoint = dbutils.widgets.get("vector_search_endpoint")
embedding_endpoint = dbutils.widgets.get("embedding_endpoint")
app_name = dbutils.widgets.get("app_name")

chunks_table = f"{catalog}.{schema}.chunks"
index_name = f"{catalog}.{schema}.chunks_index"

# COMMAND ----------

# ## Merge the chunks
#
# MERGE is a SQL statement with no DataFrame equivalent, so this stays SQL.

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {chunks_table} (
        chunk_id STRING NOT NULL,
        path STRING,
        chunk_to_embed STRING,
        chunk_to_retrieve STRING
    )
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

spark.sql(f"""
    MERGE INTO {chunks_table} AS target
    USING (
        SELECT * FROM {catalog}.{schema}.page_chunks
        UNION ALL
        SELECT * FROM {catalog}.{schema}.attachment_chunks
    ) AS source
    ON target.chunk_id = source.chunk_id
    WHEN MATCHED AND NOT (target.chunk_to_embed <=> source.chunk_to_embed) THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
""")

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

# COMMAND ----------

# ## Let the app read the index
#
# The app queries the index as its own service principal. GRANT has no Python
# API, so this stays SQL. Grants are idempotent, so this runs on every refresh.

principal = w.apps.get(app_name).service_principal_client_id
for grant in (
    f"USE CATALOG ON CATALOG {catalog}",
    f"USE SCHEMA ON SCHEMA {catalog}.{schema}",
    f"SELECT ON TABLE {index_name}",
):
    spark.sql(f"GRANT {grant} TO `{principal}`")
print(f"App {app_name} can read {index_name}.")
