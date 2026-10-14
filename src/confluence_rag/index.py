"""The two steps of the index task; the task itself is the notebook jobs/refresh_index.py.

1. Create the Delta Sync index on the pipeline's `chunks` table on the first run,
   sync it on every later run. The index is created here rather than in the bundle
   because it cannot exist before its source table does, and the pipeline creates
   that table on its first run.
2. Grant the app's service principal USE SCHEMA and SELECT on the schema, which
   covers the index and anything added to the schema later. Grants are idempotent,
   so this runs on every refresh. USE CATALOG is not granted here: only the
   catalog's owner can, and `main` gives it to everyone.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.catalog import PermissionsChange, Privilege
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    PipelineType,
    VectorIndexType,
)

EMBEDDING_ENDPOINT = "databricks-gte-large-en"


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


def grant_app_access(w: WorkspaceClient, catalog: str, schema: str, app: str) -> None:
    """Let the app's service principal read the schema, and so the index in it."""
    principal = w.apps.get(app).service_principal_client_id
    w.grants.update(
        "schema",
        f"{catalog}.{schema}",
        changes=[PermissionsChange(principal=principal, add=[Privilege.USE_SCHEMA, Privilege.SELECT])],
    )
    print(f"App {app} can read {catalog}.{schema}.")
