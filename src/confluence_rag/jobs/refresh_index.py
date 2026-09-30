# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh the index
# MAGIC
# MAGIC Runs as the last task of the refresh job, after the documents pipeline: chunk
# MAGIC the new and changed documents, create or sync the vector index, grant the app
# MAGIC access. The steps live in `confluence_rag.index`, installed in the task's
# MAGIC environment as the wheel the bundle builds.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

from confluence_rag import index

catalog = dbutils.widgets.get("catalog")
schema = f"{catalog}.{dbutils.widgets.get('schema')}"
w = WorkspaceClient()

chunks_table = index.refresh_chunks(spark, schema)
vs_index = index.create_or_sync_index(w, chunks_table, dbutils.widgets.get("vector_search_endpoint"))
index.grant_app_access(spark, w, catalog, schema, vs_index, dbutils.widgets.get("app_name"))
