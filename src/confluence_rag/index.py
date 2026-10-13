"""The three steps of the index task; the task itself is the notebook src/refresh_index.py.

1. Merge the pipeline's chunk tables into one `chunks` table with Change Data Feed
   on. A merge, not an overwrite: unchanged chunks produce no change events, so the
   index embeds only what changed.
2. Create the Delta Sync index on the first run, sync it on every later run. The
   index is created here rather than in the bundle because it cannot exist before
   its source table does, and only running code creates the table.
3. Grant the app's service principal read access to the index. Grants are
   idempotent, so this runs on every refresh.

MERGE and GRANT have no DataFrame or Python API, so they stay SQL.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    PipelineType,
    VectorIndexType,
)
from pyspark.sql import SparkSession

EMBEDDING_ENDPOINT = "databricks-gte-large-en"


def merge_chunks(spark: SparkSession, schema: str) -> str:
    """Upsert both chunk tables into `chunks`, delete vanished chunks, and return the table's full name."""
    table = f"{schema}.chunks"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            chunk_id STRING NOT NULL,
            path STRING,
            chunk_to_embed STRING,
            chunk_to_retrieve STRING
        )
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    spark.sql(f"""
        MERGE INTO {table} AS target
        USING (
            SELECT * FROM {schema}.page_chunks
            UNION ALL
            SELECT * FROM {schema}.attachment_chunks
        ) AS source
        ON target.chunk_id = source.chunk_id
        WHEN MATCHED AND NOT (
            struct(target.path, target.chunk_to_embed, target.chunk_to_retrieve)
            <=> struct(source.path, source.chunk_to_embed, source.chunk_to_retrieve)
        ) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE THEN DELETE
    """)
    return table


def create_or_sync_index(w: WorkspaceClient, chunks_table: str, endpoint: str) -> str:
    """Create the Delta Sync index on the first run, sync it on later runs, and return its full name."""
    index = f"{chunks_table}_index"
    try:
        w.vector_search_indexes.create_index(
            name=index,
            endpoint_name=endpoint,
            primary_key="chunk_id",
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table=chunks_table,
                pipeline_type=PipelineType.TRIGGERED,
                embedding_source_columns=[
                    EmbeddingSourceColumn(name="chunk_to_embed", embedding_model_endpoint_name=EMBEDDING_ENDPOINT)
                ],
            ),
        )
        print(f"Created {index}; the first sync starts on its own.")
    except ResourceAlreadyExists:
        w.vector_search_indexes.sync_index(index_name=index)
        print(f"Sync triggered for {index}.")
    return index


def grant_app_access(spark: SparkSession, w: WorkspaceClient, catalog: str, schema: str, index: str, app: str) -> None:
    """Let the app's service principal query the index."""
    principal = w.apps.get(app).service_principal_client_id
    for grant in (
        f"USE CATALOG ON CATALOG {catalog}",
        f"USE SCHEMA ON SCHEMA {schema}",
        f"SELECT ON TABLE {index}",
    ):
        spark.sql(f"GRANT {grant} TO `{principal}`")
    print(f"App {app} can read {index}.")
