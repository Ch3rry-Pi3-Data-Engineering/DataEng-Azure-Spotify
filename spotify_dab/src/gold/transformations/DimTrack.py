# ============================================================
# Delta Live Tables (DLT): SCD Type 2 dimension build for dim_track
#
# Goal
# ----
# Maintain a slowly changing dimension (SCD Type 2) table called `dim_track`
# from the Silver source table `spotify.silver.dim_track`, tracking history
# over time using a change sequence column (`updated_at`) and a business key
# (`track_id`).
#
# What this code does (high level)
# -------------------------------
# 1) Defines a staging DLT table/view `dim_track_stg` that streams from Silver
# 2) Creates the target streaming table `dim_track`
# 3) Applies CDC into `dim_track` using SCD Type 2 semantics
#
# Assumptions
# -----------
# - `spotify.silver.dim_track` exists and is readable by the pipeline
# - The source includes:
#     * `track_id` (unique business key)
#     * `updated_at` (reliable ordering for change sequencing)
# ============================================================

import dlt


# ------------------------------------------------------------
# 1) Staging stream
#
# Why a staging table?
# - Provides a stable named source for the CDC flow
# - Keeps the streaming read isolated and easy to inspect/debug
#
# NOTE:
# - `spark.readStream.table(...)` treats the source as a streaming input.
# - The upstream Silver table must support streaming reads.
# ------------------------------------------------------------

@dlt.table
def dim_track_stg():
    """
    Staging stream for dim_track CDC.

    Returns
    -------
    pyspark.sql.DataFrame
        Streaming DataFrame read from the Silver dimension table.
    """
    df = spark.readStream.table("spotify.silver.dim_track")
    return df


# ------------------------------------------------------------
# 2) Create the target table
#
# This declares the target table that will receive CDC-applied rows.
# You typically create the target first, then apply the CDC flow into it.
# ------------------------------------------------------------

dlt.create_streaming_table("dim_track")


# ------------------------------------------------------------
# 3) Apply CDC as SCD Type 2
#
# keys:
#   - Business key identifying each track entity
#
# sequence_by:
#   - Column used to order changes for each key (updated_at)
#   - Should be populated and correctly ordered; null/out-of-order
#     values can lead to incorrect history.
#
# stored_as_scd_type = 2:
#   - SCD Type 2 tracks history by inserting a new row version
#     whenever relevant attributes change.
#
# once:
#   - If False, the flow runs continuously (pipeline-dependent).
#   - If True, it attempts a one-time processing run (where supported).
# ------------------------------------------------------------

dlt.create_auto_cdc_flow(
    target="dim_track",
    source="dim_track_stg",
    keys=["track_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    track_history_column_list=None,
    track_history_except_column_list=None,
    name=None,
    once=False,
)
