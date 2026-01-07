# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Libraries

# COMMAND ----------

# ============================================================
# Imports + project source path bootstrap
# ============================================================

from __future__ import annotations

import os
import sys

from pyspark.sql import DataFrame
from pyspark.sql.functions import *  # noqa: F403  (Databricks-style convenience)
from pyspark.sql.types import *      # noqa: F403  (Databricks-style convenience)

# ------------------------------------------------------------
# Project path setup
#
# Assumption:
# - Notebook lives somewhere under the repo, and your Python package
#   folder is: <repo_root>/spotify_dab
# - We walk up 3 directories from the notebook's current working directory.
# ------------------------------------------------------------

PROJECT_ROOT: str = os.path.abspath(os.path.join(os.getcwd(), "..", "..", ".."))
SRC_ROOT: str = os.path.join(PROJECT_ROOT, "spotify_dab")

if SRC_ROOT not in sys.path:
    # Put project code first so it wins over any similarly named installed packages.
    sys.path.insert(0, SRC_ROOT)

# Local project imports (after sys.path adjustment)
from utils.transformations import reusable  # noqa: E402


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### DimUser

# COMMAND ----------

# ============================================================
# Bronze base path resolution + static read
#
# Purpose
# -------
# Determine the root storage location for the Bronze layer and
# demonstrate a static (non-streaming) read from that location.
#
# This logic allows the same notebook to run:
#   - in Unity Catalog–enabled environments
#   - in non-UC / local / CI environments via env variables
# ============================================================

from __future__ import annotations

import os
from typing import Optional


# ------------------------------------------------------------
# Helper: resolve a Unity Catalog External Location URL
#
# This function attempts to query Unity Catalog metadata to
# retrieve the physical storage URL backing an external location.
#
# If the location does not exist or is not accessible, the
# function fails gracefully and returns None.
# ------------------------------------------------------------

def _get_external_location_url(location_name: str) -> Optional[str]:
    """
    Attempt to fetch the URL for a Unity Catalog External Location.

    Parameters
    ----------
    location_name:
        Name of the external location (e.g. "bronze", "silver").

    Returns
    -------
    Optional[str]
        The external location URL with any trailing slash removed,
        or None if the location cannot be resolved.

    Notes
    -----
    Common failure reasons include:
      - Unity Catalog is not enabled
      - The external location does not exist
      - The current principal lacks permission to DESCRIBE it
    """
    try:
        row = (
            spark.sql(f"DESCRIBE EXTERNAL LOCATION {location_name}")
            .select("url")
            .first()
        )

        if row and row["url"]:
            # Normalise by removing any trailing slash
            return str(row["url"]).rstrip("/")

    except Exception:
        # Swallow exceptions intentionally:
        # this allows a clean fallback to environment variables
        return None

    return None


# ------------------------------------------------------------
# Public API: resolve Bronze base path
#
# Resolution strategy (in priority order):
#   1) Unity Catalog external location named "bronze"
#   2) Environment variable BRONZE_BASE_PATH
#
# This makes the notebook portable across environments.
# ------------------------------------------------------------

def get_bronze_base_path() -> str:
    """
    Resolve the base path for Bronze storage.

    Returns
    -------
    str
        Normalised Bronze base path (no trailing slash).

    Raises
    ------
    RuntimeError
        If neither a Unity Catalog external location nor the
        BRONZE_BASE_PATH environment variable is available.
    """
    # Preferred path: Unity Catalog external location
    url = _get_external_location_url("bronze")
    if url:
        return url

    # Fallback path: environment variable
    env_base = os.environ.get("BRONZE_BASE_PATH")
    if env_base:
        return env_base.rstrip("/")

    # Hard failure if no resolution strategy succeeds
    raise RuntimeError(
        "Bronze base path not found. "
        "Grant access to external location 'bronze' "
        "or set BRONZE_BASE_PATH."
    )


# ------------------------------------------------------------
# Resolve Bronze base path once for downstream use
# ------------------------------------------------------------

bronze_base: str = get_bronze_base_path()


# ------------------------------------------------------------
# Example: static (non-streaming) read from Bronze
#
# This demonstrates how the resolved base path is used.
# In the rest of the notebook, streaming reads typically
# replace this pattern.
# ------------------------------------------------------------

df_dim_user_bronze: DataFrame = (
    spark.read
    .format("parquet")
    .load(f"{bronze_base}/DimUser")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### AutoLoader

# COMMAND ----------

# ============================================================
# Silver base path resolution
#
# Purpose
# -------
# Determine the root storage location for the Silver layer.
#
# This mirrors the Bronze path-resolution logic and allows the
# notebook to run consistently across:
#   - Unity Catalog–enabled environments
#   - Local / CI / non-UC environments via environment variables
# ============================================================

from __future__ import annotations

import os


# ------------------------------------------------------------
# Public API: resolve Silver base path
#
# Resolution strategy (in priority order):
#   1) Unity Catalog external location named "silver"
#   2) Environment variable SILVER_BASE_PATH
#
# This keeps storage configuration externalised and portable.
# ------------------------------------------------------------

def get_silver_base_path() -> str:
    """
    Resolve the base path for Silver storage.

    Returns
    -------
    str
        Normalised Silver base path (no trailing slash).

    Raises
    ------
    RuntimeError
        If neither a Unity Catalog external location nor the
        SILVER_BASE_PATH environment variable is available.
    """
    # Preferred resolution path: Unity Catalog external location
    url = _get_external_location_url("silver")
    if url:
        return url

    # Fallback resolution path: environment variable
    env_base = os.environ.get("SILVER_BASE_PATH")
    if env_base:
        return env_base.rstrip("/")

    # Hard failure if no resolution strategy succeeds
    raise RuntimeError(
        "Silver base path not found. "
        "Grant access to external location 'silver' "
        "or set SILVER_BASE_PATH."
    )


# ------------------------------------------------------------
# Resolve Silver base path once for downstream use
# ------------------------------------------------------------

silver_base: str = get_silver_base_path()


# COMMAND ----------

# ============================================================
# Streaming ingestion: DimUser (Bronze parquet → Silver stream)
# Using Databricks Autoloader (cloudFiles)
#
# Purpose
# -------
# Continuously (or incrementally, depending on trigger)
# ingest DimUser parquet files from the Bronze layer into a
# Structured Streaming DataFrame for downstream processing.
#
# This stream forms the foundation for:
#   - cleaning (e.g. deduplication)
#   - enrichment
#   - persistence to Delta paths or Unity Catalog tables
# ============================================================

from pyspark.sql import DataFrame


# ------------------------------------------------------------
# Autoloader schema checkpoint
#
# dim_user_schema_checkpoint:
#   - Stores the inferred schema for DimUser
#   - Tracks schema evolution over time
#   - REQUIRED for Autoloader to safely handle new columns
# ------------------------------------------------------------

dim_user_schema_checkpoint: str = f"{silver_base}/DimUser/checkpoint/schema"


# ------------------------------------------------------------
# Create the streaming DataFrame using Autoloader
#
# cloudFiles.format = "parquet"
#   - Input files are parquet
#
# cloudFiles.schemaEvolutionMode = "rescue"
#   - Unexpected columns are captured in `_rescued_data`
#     instead of failing the stream
#
# cloudFiles.schemaLocation
#   - Location where Autoloader stores schema metadata
# ------------------------------------------------------------

df_user_stream: DataFrame = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    # "rescue" keeps unexpected fields in `_rescued_data`
    # instead of failing the stream.
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.schemaLocation", dim_user_schema_checkpoint)
    .load(f"{bronze_base}/DimUser")
)


# COMMAND ----------

# ============================================================
# DimUser: write streaming data to Delta (availableNow trigger)
#
# Purpose
# -------
# Persist the DimUser streaming DataFrame to a Delta Lake
# location in the Silver layer using a bounded streaming run.
#
# The `availableNow=True` trigger processes:
#   - all data currently available at the source
#   - then stops automatically
#
# This provides batch-like semantics while still using
# Structured Streaming under the hood.
# ============================================================

from pyspark.sql.streaming import StreamingQuery


# ------------------------------------------------------------
# 1) Paths used by this streaming query
#
# checkpoint_path:
#   - Stores streaming offsets, progress, and state
#   - MUST be stable across runs to guarantee exactly-once
#     semantics for this Delta sink
#
# output_path:
#   - Physical storage location for the Delta table data
# ------------------------------------------------------------

checkpoint_path: str = f"{silver_base}/DimUser/checkpoint"
output_path: str = f"{silver_base}/DimUser/data"


# ------------------------------------------------------------
# 2) Configure and start the streaming write
#
# format("delta"):
#   - Use Delta Lake format for ACID guarantees
#
# outputMode("append"):
#   - Append-only ingestion (typical for dimension ingestion)
#
# trigger(availableNow=True):
#   - Process all currently available input data
#   - Automatically stop when no more data is available
#
# NOTE:
# availableNow=True is preferred over once=True when supported,
# as it provides better semantics for incremental ingestion.
# ------------------------------------------------------------

query: StreamingQuery = (
    df_user_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("path", output_path)
    .trigger(availableNow=True)  # alternative: .trigger(once=True)
    .start()
)


# ------------------------------------------------------------
# 3) Block until the streaming query finishes
#
# The timeout is intentionally short for notebook runs.
# Increase this value for larger datasets or slower storage.
# ------------------------------------------------------------

query.awaitTermination(10)


# ------------------------------------------------------------
# 4) Verification step
#
# Read the Delta output as a static DataFrame and display it.
# This confirms that data has been successfully written.
# ------------------------------------------------------------

df_dim_user_silver = (
    spark.read
    .format("delta")
    .load(output_path)
)

display(df_dim_user_silver)


# COMMAND ----------

# ============================================================
# Housekeeping: clear DimUser *display* checkpoint folder
#
# Purpose
# -------
# When iterating in notebooks, it's common to run a "display/preview" stream
# multiple times. Using a separate checkpoint directory for this avoids
# messing with the "real" ingestion checkpoint used for durable processing.
#
# WARNING
# -------
# Deleting a checkpoint resets stream progress for that checkpoint path.
# Only do this for non-production / display checkpoints.
# ============================================================

from __future__ import annotations

DIM_USER_DISPLAY_CHECKPOINT_PATH: str = f"{silver_base}/DimUser/checkpoint/display"
DIM_USER_OUTPUT_PATH: str = f"{silver_base}/DimUser/data"

# Recursive delete; safe for notebook iteration, be cautious in production.
dbutils.fs.rm(DIM_USER_DISPLAY_CHECKPOINT_PATH, True)


# COMMAND ----------

# ============================================================
# DimUser: clean streaming data and write to Delta
#
# This cell performs THREE logical steps:
#   1) Define a reusable cleaning function for DimUser
#   2) Apply that cleaning logic to the existing streaming DF
#   3) Write the cleaned stream to Delta and verify the result
#
# IMPORTANT
# ---------
# This cell assumes the following already exist:
#   - df_user_stream : streaming DataFrame created via Autoloader
#   - checkpoint     : checkpoint path defined earlier
#   - output_path    : Delta output path defined earlier
# ============================================================

from __future__ import annotations

from pyspark.sql import DataFrame


# ------------------------------------------------------------
# 1) Cleaning function for DimUser
#
# This function encapsulates all DimUser-specific cleaning
# logic so that:
#   - the rules are documented in one place
#   - the logic is easy to extend later
#   - the transformation is easy to test in isolation
# ------------------------------------------------------------

def clean_dim_user(df: DataFrame) -> DataFrame:
    """
    Clean the DimUser streaming DataFrame.

    Rules
    -----
    1) Drop the Autoloader rescue column (`_rescued_data`)
       - This column is automatically added by Autoloader
         when schemaEvolutionMode="rescue" is enabled
       - It contains unexpected or newly arrived fields

    2) Drop duplicate users by `user_id`
       - Ensures one logical record per user

    Notes
    -----
    - dropDuplicates on a streaming DataFrame is STATEFUL
    - Spark tracks previously seen `user_id` values using
      the streaming checkpoint
    - If the cardinality of `user_id` is very large, this
      state can grow over time
    """
    # Instantiate the reusable transformation utilities
    utils = reusable()

    # Apply cleaning rules in a readable, chained form
    return (
        utils.dropColumns(df, ["_rescued_data"])
        .dropDuplicates(["user_id"])
    )


# ------------------------------------------------------------
# 2) Apply cleaning logic to the existing streaming DataFrame
#
# df_user_stream:
#   - Created earlier using Autoloader
#   - Represents the raw DimUser stream from Bronze
# ------------------------------------------------------------

df_user_clean: DataFrame = clean_dim_user(df_user_stream)


# ------------------------------------------------------------
# 3) Write the cleaned stream to Delta
#
# format("delta"):
#   - Use Delta Lake for ACID guarantees and schema support
#
# outputMode("append"):
#   - Append-only ingestion pattern
#
# checkpointLocation:
#   - Stores offsets and state for this streaming query
#   - Required for fault tolerance and exactly-once semantics
#
# trigger(once=True):
#   - Process all currently available data
#   - Terminate automatically when complete
# ------------------------------------------------------------

query = (
    df_user_clean.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .option("path", output_path)
    .trigger(once=True)
    .start()
)


# ------------------------------------------------------------
# 4) Block until the streaming query finishes
#
# Required in notebooks to ensure the write completes
# before running downstream verification logic.
# ------------------------------------------------------------

query.awaitTermination()


# ------------------------------------------------------------
# 5) Verification step
#
# Read the Delta output as a STATIC DataFrame and display it.
# This confirms that the cleaned data has been written
# successfully to `output_path`.
# ------------------------------------------------------------

display(
    spark.read
    .format("delta")
    .load(output_path)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### DimArtist

# COMMAND ----------

# ============================================================
# DimUser: write cleaned streaming data to Unity Catalog table
#
# Sink
# ----
# - Table: spotify.silver.dim_user
# - Mode : append
#
# Notes
# -----
# - Uses a dedicated checkpoint directory for table writes.
# - trigger(once=True) processes all available data and then stops.
# ============================================================

table_name: str = "spotify.silver.dim_user"
checkpoint: str = f"{silver_base}/_checkpoints/dim_user"

(
    df_user_clean.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .trigger(once=True)
    .toTable(table_name)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### DimArtist

# COMMAND ----------

# ============================================================
# DimArtist: Autoloader streaming read (Bronze → streaming DF)
#
# Source
# ------
# - Input path : {bronze_base}/DimArtist
# - File type  : parquet
#
# Schema handling
# ---------------
# - schemaEvolutionMode = "rescue"
#   * Unexpected / new columns are captured in `_rescued_data`
#     instead of failing the stream.
# - schemaLocation
#   * Persists inferred schema and schema evolution metadata.
# ============================================================

df_art = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option(
        "cloudFiles.schemaLocation",
        f"{silver_base}/DimArtist/checkpoint/schema",
    )
    .load(f"{bronze_base}/DimArtist")
)


# COMMAND ----------

# ============================================================
# DimArtist: streaming clean + write to Delta (notebook run)
#
# This cell performs THREE distinct steps:
#   1) Define storage locations for checkpointing and output data
#   2) Apply a simple cleaning rule to the streaming DataFrame
#   3) Execute a one-off streaming write and verify the result
#
# IMPORTANT
# ---------
# This uses a *display / exploratory* checkpoint path.
# It is intended for notebook iteration, NOT long-running jobs.
# ============================================================


# ------------------------------------------------------------
# 1) Paths used by this streaming query
#
# checkpoint:
#   - Stores streaming progress metadata (offsets, state, etc.)
#   - Deleting this resets the stream for this checkpoint path
#
# output_path:
#   - Location where the Delta table data files are written
# ------------------------------------------------------------

checkpoint = f"{silver_base}/DimArtist/checkpoint/display"
output_path = f"{silver_base}/DimArtist/data"


# ------------------------------------------------------------
# 2) Clean the DimArtist streaming DataFrame
#
# dropDuplicates(["artist_id"]):
#   - Ensures each artist appears only once
#   - In streaming, this is STATEFUL across micro-batches
#   - Spark keeps track of seen artist_ids using the checkpoint
#
# NOTE:
# If artist_id cardinality is very large, this can grow state.
# ------------------------------------------------------------

df_art_clean = (
    df_art.dropDuplicates(["artist_id"])
)


# ------------------------------------------------------------
# 3) Write the cleaned stream to Delta
#
# format("delta"):
#   - Writes data in Delta Lake format
#
# outputMode("append"):
#   - Only new rows are appended
#   - Required for most streaming ingestion patterns
#
# trigger(once=True):
#   - Processes all currently available data
#   - Terminates automatically (batch-like behaviour)
#
# start():
#   - Launches the streaming query
# ------------------------------------------------------------

query = (
    df_art_clean.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .option("path", output_path)
    .trigger(once=True)
    .start()
)


# ------------------------------------------------------------
# 4) Block until the streaming query finishes
#
# Required when using trigger(once=True) in notebooks,
# otherwise the cell may finish before the write completes.
# ------------------------------------------------------------

query.awaitTermination()


# ------------------------------------------------------------
# 5) Verification step
#
# Read the Delta output as a static DataFrame and display it.
# This does NOT use streaming.
# ------------------------------------------------------------

display(
    spark.read
    .format("delta")
    .load(output_path)
)


# COMMAND ----------

# ============================================================
# DimArtist: write cleaned streaming data to Unity Catalog table
#
# Purpose
# -------
# Persist the cleaned DimArtist stream as a managed Delta table
# in Unity Catalog, suitable for downstream consumption.
#
# This is a STREAMING write, even though trigger(once=True)
# makes it behave like a batch job.
# ============================================================


# ------------------------------------------------------------
# 1) Target table definition
#
# table_name:
#   - Fully-qualified Unity Catalog table name
#   - Format: <catalog>.<schema>.<table>
# ------------------------------------------------------------

table_name = "spotify.silver.dim_artist"


# ------------------------------------------------------------
# 2) Streaming checkpoint location
#
# checkpoint:
#   - Stores streaming offsets and state for this table write
#   - MUST be stable across runs to guarantee exactly-once
#     semantics for this sink
#
# NOTE:
# This checkpoint is intentionally separate from any
# notebook "display" checkpoints used for experimentation.
# ------------------------------------------------------------

checkpoint = f"{silver_base}/_checkpoints/dim_artist"


# ------------------------------------------------------------
# 3) Write cleaned stream to Unity Catalog table
#
# format("delta"):
#   - Required for Delta Lake tables
#
# outputMode("append"):
#   - New rows are appended to the table
#
# trigger(once=True):
#   - Processes all currently available data
#   - Terminates automatically
#
# toTable(table_name):
#   - Creates the table if it does not exist
#   - Appends data if the table already exists
# ------------------------------------------------------------

(
    df_art_clean.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .trigger(once=True)
    .toTable(table_name)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### DimTrack

# COMMAND ----------

# ============================================================
# DimTrack: Autoloader streaming read (Bronze parquet → stream)
#
# Purpose
# -------
# Continuously (or one-off, depending on trigger downstream)
# ingest track-level data from the Bronze layer into a
# Structured Streaming DataFrame.
#
# This DataFrame will later be:
#   - cleaned / enriched
#   - written to Delta paths or Unity Catalog tables
# ============================================================


# ------------------------------------------------------------
# Autoloader configuration
#
# cloudFiles.format = "parquet"
#   - Source files are expected to be parquet
#
# cloudFiles.schemaEvolutionMode = "rescue"
#   - New or unexpected columns are captured in `_rescued_data`
#     instead of failing the stream
#
# cloudFiles.schemaLocation
#   - Persistent storage for inferred schema and schema changes
#   - REQUIRED for Autoloader to track schema evolution
# ------------------------------------------------------------

df_track = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option(
        "cloudFiles.schemaLocation",
        f"{silver_base}/DimTrack/checkpoint/schema",
    )
    .load(f"{bronze_base}/DimTrack")
)


# COMMAND ----------

# ============================================================
# DimTrack: enrich stream + write to Delta (notebook run) + verify
#
# This cell does three things:
#   1) Defines checkpoint/output locations for this notebook-run stream
#   2) Enriches the streaming data with a derived categorical feature
#   3) Writes the stream to Delta and reads it back for verification
#
# IMPORTANT
# ---------
# This uses a *display / exploratory* checkpoint path.
# It is suitable for notebook iteration, not for production jobs.
# ============================================================


# ------------------------------------------------------------
# 1) Paths used by this streaming query
#
# checkpoint:
#   - Stores streaming offsets + state for THIS query
#   - If you delete it, the query will "start over" for this checkpoint path
#
# output_path:
#   - Destination folder containing the Delta Lake data files
# ------------------------------------------------------------

checkpoint = f"{silver_base}/DimTrack/checkpoint/display"
output_path = f"{silver_base}/DimTrack/data"


# ------------------------------------------------------------
# 2) Enrichment: create a duration bucket
#
# durationFlag:
#   - Categorises `duration_sec` into a simple bucket:
#       < 150 seconds  -> "low"
#       < 300 seconds  -> "medium"
#       otherwise      -> "high"
#
# Notes
# -----
# - `withColumn` adds (or replaces) a column.
# - `when` creates a conditional expression.
# - We use col("duration_sec") to reference the column safely.
# ------------------------------------------------------------

df_track_clean = (
    df_track.withColumn(
        "durationFlag",
        when(col("duration_sec") < 150, "low")
        .when(col("duration_sec") < 300, "medium")
        .otherwise("high"),
    )
)


# ------------------------------------------------------------
# 3) Write the enriched stream to Delta
#
# format("delta"):
#   - Writes using Delta Lake format
#
# outputMode("append"):
#   - Appends new rows only (typical ingestion pattern)
#
# trigger(once=True):
#   - Processes all currently available data then stops
#   - Useful for notebook-driven incremental loads
#
# option("path", output_path):
#   - Sets the physical storage location for the Delta table data
# ------------------------------------------------------------

query = (
    df_track_clean.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .option("path", output_path)
    .trigger(once=True)
    .start()
)


# ------------------------------------------------------------
# 4) Block until the stream finishes
#
# With trigger(once=True), this should complete automatically
# once all available input data has been processed.
# ------------------------------------------------------------

query.awaitTermination()


# ------------------------------------------------------------
# 5) Verification: read the Delta output as a static DataFrame
#
# This is a non-streaming read. It simply loads whatever the
# stream wrote to `output_path`.
# ------------------------------------------------------------

display(
    spark.read
    .format("delta")
    .load(output_path)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### DimDate

# COMMAND ----------

# ============================================================
# DimDate: Autoloader streaming read (Bronze parquet → stream)
#
# Purpose
# -------
# Ingest date / calendar dimension data from the Bronze layer
# using Databricks Autoloader into a Structured Streaming
# DataFrame for downstream processing.
#
# This stream is typically low-volume but foundational, as
# date dimensions are referenced by many fact tables.
# ============================================================


# ------------------------------------------------------------
# Autoloader configuration
#
# cloudFiles.format = "parquet"
#   - Source files are stored as parquet
#
# cloudFiles.schemaEvolutionMode = "rescue"
#   - Any unexpected or new columns are captured in `_rescued_data`
#     rather than causing the stream to fail
#
# cloudFiles.schemaLocation
#   - Persistent storage location used by Autoloader to:
#       * store the inferred schema
#       * track schema evolution over time
#   - REQUIRED for schema evolution support
# ------------------------------------------------------------

df_date = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option(
        "cloudFiles.schemaLocation",
        f"{silver_base}/DimDate/checkpoint/schema",
    )
    .load(f"{bronze_base}/DimDate")
)


# COMMAND ----------

# ============================================================
# DimDate: write streaming data to Delta (notebook run) + verify
#
# This cell performs a one-off streaming write of the DimDate
# dimension from Bronze → Silver storage using Delta Lake.
#
# Unlike other dimensions, no additional cleaning or enrichment
# is applied here — the data is written as-is.
# ============================================================


# ------------------------------------------------------------
# 1) Paths used by this streaming query
#
# checkpoint:
#   - Stores streaming offsets and progress information
#   - Deleting this directory resets the stream for this query
#
# output_path:
#   - Physical storage location for the Delta table data
# ------------------------------------------------------------

checkpoint = f"{silver_base}/DimDate/checkpoint/display"
output_path = f"{silver_base}/DimDate/data"


# ------------------------------------------------------------
# 2) Write the DimDate stream to Delta
#
# format("delta"):
#   - Use Delta Lake as the storage format
#
# outputMode("append"):
#   - Append new rows only
#
# trigger(once=True):
#   - Process all currently available data
#   - Automatically stop when complete
#
# option("path", output_path):
#   - Specifies where the Delta files are written
# ------------------------------------------------------------

query = (
    df_date.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .option("path", output_path)
    .trigger(once=True)
    .start()
)


# ------------------------------------------------------------
# 3) Block until the streaming query finishes
#
# Required in notebooks to ensure the write completes before
# moving on to downstream cells.
# ------------------------------------------------------------

query.awaitTermination()


# ------------------------------------------------------------
# 4) Verification step
#
# Read the Delta output as a static DataFrame and display it.
# This does not use streaming.
# ------------------------------------------------------------

display(
    spark.read
    .format("delta")
    .load(output_path)
)


# COMMAND ----------

# ============================================================
# DimDate: write streaming data to Unity Catalog table
#
# Purpose
# -------
# Persist the DimDate dimension as a managed Delta table in
# Unity Catalog for reliable downstream joins and analytics.
#
# This is a Structured Streaming write, even though
# trigger(once=True) makes it behave like a batch job.
# ============================================================


# ------------------------------------------------------------
# 1) Target table definition
#
# table_name:
#   - Fully-qualified Unity Catalog table name
#   - Format: <catalog>.<schema>.<table>
# ------------------------------------------------------------

table_name = "spotify.silver.dim_date"


# ------------------------------------------------------------
# 2) Streaming checkpoint location
#
# checkpoint:
#   - Stores streaming offsets and state for this table sink
#   - Must remain stable across runs to ensure exactly-once
#     semantics when appending to the table
#
# NOTE:
# This checkpoint is intentionally separate from any notebook
# display checkpoints used for exploratory runs.
# ------------------------------------------------------------

checkpoint = f"{silver_base}/_checkpoints/dim_date"


# ------------------------------------------------------------
# 3) Write the DimDate stream to the Unity Catalog table
#
# format("delta"):
#   - Required storage format for Unity Catalog tables
#
# outputMode("append"):
#   - Appends new rows to the existing table
#
# trigger(once=True):
#   - Processes all currently available data
#   - Terminates automatically
#
# toTable(table_name):
#   - Creates the table if it does not exist
#   - Appends data if the table already exists
# ------------------------------------------------------------

(
    df_date.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .trigger(once=True)
    .toTable(table_name)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### FactStream

# COMMAND ----------

# ============================================================
# FactStream: Autoloader streaming read (Bronze parquet → stream)
#
# Purpose
# -------
# Ingest fact-level event / transaction data from the Bronze
# layer using Databricks Autoloader into a Structured Streaming
# DataFrame for downstream transformations and persistence.
#
# Fact tables are typically:
#   - high volume
#   - append-only
#   - the primary driver of analytical workloads
# ============================================================


# ------------------------------------------------------------
# Autoloader configuration
#
# cloudFiles.format = "parquet"
#   - Source files are stored as parquet
#
# cloudFiles.schemaEvolutionMode = "rescue"
#   - New or unexpected columns are captured in `_rescued_data`
#     rather than failing the stream
#
# cloudFiles.schemaLocation
#   - Persistent storage location used by Autoloader to:
#       * store inferred schema
#       * track schema evolution over time
#   - REQUIRED for schema evolution support
# ------------------------------------------------------------

df_fact = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option(
        "cloudFiles.schemaLocation",
        f"{silver_base}/FactStream/checkpoint/schema",
    )
    .load(f"{bronze_base}/FactStream")
)


# COMMAND ----------

# ============================================================
# FactStream: write streaming facts to Delta (notebook run) + verify
#
# Purpose
# -------
# Perform a one-off (trigger-once) streaming write of the FactStream
# dataset from the Bronze layer into a Delta location in Silver storage.
#
# Why "streaming" for facts?
# --------------------------
# Fact data is typically append-only and arrives incrementally, which
# makes it a natural fit for Structured Streaming — even if we run it
# in a batch-like way inside a notebook using trigger(once=True).
# ============================================================


# ------------------------------------------------------------
# 1) Paths used by this streaming query
#
# checkpoint:
#   - Stores streaming progress metadata (offsets, state, etc.)
#   - Deleting this resets the stream for this checkpoint path
#
# output_path:
#   - Physical storage location for the Delta files
# ------------------------------------------------------------

checkpoint = f"{silver_base}/FactStream/checkpoint/display"
output_path = f"{silver_base}/FactStream/data"


# ------------------------------------------------------------
# 2) Write the FactStream to Delta
#
# format("delta"):
#   - Use Delta Lake format for ACID + schema features
#
# outputMode("append"):
#   - Standard ingestion mode for fact/event streams
#
# trigger(once=True):
#   - Process all currently available files
#   - Stop automatically when no more data is available
#
# option("path", output_path):
#   - Sets the target Delta storage path
# ------------------------------------------------------------

query = (
    df_fact.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .option("path", output_path)
    .trigger(once=True)
    .start()
)


# ------------------------------------------------------------
# 3) Block until the streaming query finishes
#
# Ensures the write completes before verification.
# ------------------------------------------------------------

query.awaitTermination()


# ------------------------------------------------------------
# 4) Verification step
#
# Read the Delta output as a static DataFrame and display it.
# This confirms that data has landed at `output_path`.
# ------------------------------------------------------------

display(
    spark.read
    .format("delta")
    .load(output_path)
)


# COMMAND ----------

# ============================================================
# FactStream: write streaming facts to Unity Catalog table
#
# Purpose
# -------
# Persist the FactStream dataset as a managed Delta table in
# Unity Catalog for reliable, repeatable analytics.
#
# Fact tables are typically:
#   - append-only
#   - high volume
#   - the primary source for downstream aggregations
#
# This is implemented as a Structured Streaming write, even
# though trigger(once=True) makes it behave like a batch load.
# ============================================================


# ------------------------------------------------------------
# 1) Target table definition
#
# table_name:
#   - Fully-qualified Unity Catalog table name
#   - Format: <catalog>.<schema>.<table>
# ------------------------------------------------------------

table_name = "spotify.silver.fact_stream"


# ------------------------------------------------------------
# 2) Streaming checkpoint location
#
# checkpoint:
#   - Stores streaming offsets and state for this table sink
#   - Must remain stable across runs to guarantee exactly-once
#     semantics when appending to the table
#
# NOTE:
# This checkpoint is intentionally separate from notebook
# display checkpoints used for exploratory runs.
# ------------------------------------------------------------

checkpoint = f"{silver_base}/_checkpoints/fact_stream"


# ------------------------------------------------------------
# 3) Write the FactStream to the Unity Catalog table
#
# format("delta"):
#   - Required storage format for Unity Catalog tables
#
# outputMode("append"):
#   - Append new rows to the existing table
#
# trigger(once=True):
#   - Process all currently available data
#   - Terminate automatically when complete
#
# toTable(table_name):
#   - Creates the table if it does not exist
#   - Appends data if the table already exists
# ------------------------------------------------------------

(
    df_fact.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .trigger(once=True)
    .toTable(table_name)
)
