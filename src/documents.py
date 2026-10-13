# Databricks notebook source
# MAGIC %md
# MAGIC # Documents
# MAGIC
# MAGIC Turns the connector's raw pages into documents: every current page becomes
# MAGIC Markdown. Images and macros carry no text, so they drop out.
# MAGIC
# MAGIC `page_documents` is a materialized view and is recomputed on every run,
# MAGIC which is how edits and deletions get in.

# COMMAND ----------

from markdownify import markdownify
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")


@udf("string")
def to_markdown(xhtml: str | None) -> str:
    """Convert Confluence storage-format XHTML to Markdown."""
    return markdownify(xhtml or "", heading_style="ATX").strip()


@dp.materialized_view(comment="Current pages as Markdown.")
def page_documents() -> DataFrame:
    """Convert the pages the wiki still shows to Markdown."""
    return (
        spark.read.table(f"{catalog}.{schema}.pages")
        .where("status = 'current' AND isDeleted IS NOT TRUE")
        .select(
            "id",
            "title",
            F.col("web_url").alias("path"),
            to_markdown("body.storage.value").alias("content"),
        )
        .where("content != ''")
    )
