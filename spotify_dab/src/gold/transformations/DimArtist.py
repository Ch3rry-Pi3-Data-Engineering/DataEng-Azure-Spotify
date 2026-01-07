# ============================================================
# Delta Live Tables (DLT): SCD Type 2 dimension build for dim_artist
#
# Goal
# ----
# Maintain a slowly changing dimension (SCD Type 2) table called `dim_artist`
# from the Silver source table `spotify.silver.dim_artist`, tracking history
# over time using a change sequence column (`updated_at`) and a business key
# (`artist_id`).
#
# What this code does (high level)
# -------------------------------
# 1) Defines a staging DLT table/view `dim_artist_stg` that streams from Silver
# 2) Creates the target streaming table `dim_artist`
# 3) Applies CDC into `dim_artist` using SCD Type 2 semantics
#
# Assumptions
# -----------
# - `spotify.silver.dim_artist` exists and is readable by the pipeline
# - The source includes:
#     * `artist_id` (unique business key)
#     * `updated_at` (monotonic or at least correctly ordered change sequence)
# - The DLT pipeline is configured to run in "continuous" mode if once=False
# ============================================================

import dlt


# ------------------------------------------------------------
# 1) Staging stream
#
# Why a staging table?
# - Keeps the source read isolated and easy to inspect
# - Provides a stable "source" name for the CDC flow
#
# NOTE
# - `spark.readStream.table(...)` treats the source as a streaming input.
# - If the upstream table is not suitable for streaming reads, this will fail.
# ------------------------------------------------------------

@dlt.table
def dim_artist_stg():
    """
    Staging stream for dim_artist CDC.

    Returns
    -------
    pyspark.sql.DataFrame
        Streaming DataFrame read from the Silver dimension table.
    """
    df = spark.readStream.table("spotify.silver.dim_artist")
    return df


# ------------------------------------------------------------
# 2) Create the target table
#
# This declares the target table that will receive CDC-applied rows.
# You typically create the target first, then define the CDC flow into it.
# ------------------------------------------------------------

dlt.create_streaming_table("dim_artist")


# ------------------------------------------------------------
# 3) Apply CDC as SCD Type 2
#
# Key parameters (what they mean)
# ------------------------------
# target:
#   - The DLT table that will store the SCD2 dimension (`dim_artist`)
#
# source:
#   - The staging DLT table/view that provides changes (`dim_artist_stg`)
#
# keys:
#   - Business key columns used to identify entities (artist_id)
#
# sequence_by:
#   - Column used to order changes for each key (updated_at)
#   - Must be reliable for sequencing; if it's null or out-of-order, SCD results
#     can be incorrect.
#
# stored_as_scd_type = 2:
#   - SCD Type 2 tracks history (i.e., inserts new versions of changed rows)
#
# track_history_column_list / track_history_except_column_list:
#   - Controls which columns are tracked for change detection.
#   - With both set to None, DLT uses its default behaviour.
#
# once:
#   - If False, the flow is configured to run continuously (pipeline dependent).
#   - If True, it attempts a one-time backfill-style run (where supported).
# ------------------------------------------------------------

dlt.create_auto_cdc_flow(
    target="dim_artist",
    source="dim_artist_stg",
    keys=["artist_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    track_history_column_list=None,
    track_history_except_column_list=None,
    name=None,
    once=False,
)
