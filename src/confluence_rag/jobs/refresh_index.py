# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh the index
# MAGIC
# MAGIC Runs as the last task of the refresh job, after the documents pipeline:
# MAGIC create or sync the vector index, grant the app access. The steps live in
# MAGIC `confluence_rag.index`, installed in the task's environment as the wheel the
# MAGIC bundle builds.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

from confluence_rag import index

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
w = WorkspaceClient()

index.create_or_sync_index(w, f"{catalog}.{schema}.chunks", dbutils.widgets.get("vector_search_endpoint"))
index.grant_app_access(w, catalog, schema, dbutils.widgets.get("app_name"))
