"""The three steps of the index task; the task itself is the notebook jobs/refresh_index.py.

1. Chunk the pipeline's two document tables into one `chunks` table with Change
   Data Feed on. Every chunk carries its document's `version`, a hash of the
   document's path, title and text. A run deletes the chunks whose version is gone
   and chunks only the documents whose version is new, so ai_prep_search runs on
   new and changed documents only and the index embeds only their chunks. The path
   carries the page title or file name, so a rename is a change too.
2. Create the Delta Sync index on the first run, sync it on every later run. The
   index is created here rather than in the bundle because it cannot exist before
   its source table does, and only running code creates the table.
3. Grant the app's service principal read access to the index. Grants are
   idempotent, so this runs on every refresh.

DELETE and GRANT have no DataFrame or Python API, so they stay SQL. ai_prep_search
has no Python binding, so it goes through F.expr.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    PipelineType,
    VectorIndexType,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

EMBEDDING_ENDPOINT = "databricks-gte-large-en"


def versioned(documents: DataFrame, text: str) -> DataFrame:
    """Add `version`, a hash of what a document's chunks are built from: its path, title and text."""
    return documents.withColumn("version", F.md5(F.concat_ws("\n", "path", "title", F.col(text).cast("string"))))


def chunks(documents: DataFrame, text: str, prefix: str) -> DataFrame:
    """Split `text` with ai_prep_search into one row per chunk with a stable `chunk_id`.

    One function replaces a tokenizer, a text splitter and their settings. It returns
    two texts per chunk: chunk_to_embed (the chunk with context an LLM writes, for the
    index) and chunk_to_retrieve (the plain chunk, for the model). ai_prep_search
    leaves the title out of Markdown input, so it is prepended to chunk_to_embed here.
    """
    title = F.concat(F.lit("Document Title: "), "title")
    return (
        documents.withColumn("prep", F.expr(f"ai_prep_search({text})"))
        .select(
            "id",
            "version",
            "path",
            "title",
            F.explode(F.variant_get("prep", "$.document.contents", "array<variant>")).alias("chunk"),
        )
        .select(
            F.concat(F.lit(prefix), "id", F.lit("-"), F.variant_get("chunk", "$.chunk_position", "int")).alias(
                "chunk_id"
            ),
            "version",
            "path",
            F.concat_ws("\n", title, F.variant_get("chunk", "$.chunk_to_embed", "string")).alias("chunk_to_embed"),
            F.variant_get("chunk", "$.chunk_to_retrieve", "string").alias("chunk_to_retrieve"),
        )
    )


def refresh_chunks(spark: SparkSession, schema: str) -> str:
    """Chunk new and changed documents into `chunks`, delete stale chunks, and return the table's full name."""
    table = f"{schema}.chunks"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            chunk_id STRING NOT NULL,
            version STRING NOT NULL,
            path STRING,
            chunk_to_embed STRING,
            chunk_to_retrieve STRING
        )
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    pages = versioned(spark.read.table(f"{schema}.page_documents"), "content")
    attachments = versioned(spark.read.table(f"{schema}.attachment_documents"), "parsed")
    pages.select("version").union(attachments.select("version")).createOrReplaceTempView("versions")
    spark.sql(f"DELETE FROM {table} WHERE version NOT IN (SELECT version FROM versions)")
    known = spark.read.table(table).select("version")
    new = chunks(pages.join(known, "version", "left_anti"), "content", "page-").union(
        chunks(attachments.join(known, "version", "left_anti"), "parsed", "attachment-")
    )
    new.write.mode("append").saveAsTable(table)
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
