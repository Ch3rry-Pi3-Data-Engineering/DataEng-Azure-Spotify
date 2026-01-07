# ============================================================
# Delta Live Tables (DLT): SCD Type 2 dimension build for dim_user
#
# Goal
# ----
# Maintain a slowly changing dimension (SCD Type 2) table called `dim_user`
# from the Silver source table `spotify.silver.dim_user`, tracking history
# over time using a change sequence column (`updated_at`) and a business key
# (`user_id`).
#
# This version also introduces a simple data quality rule:
#   - Drop records where `user_id` is NULL
#
# What this code does (high level)
# -------------------------------
# 1) Defines a staging DLT table/view `dim_user_stg` that streams from Silver
# 2) Creates the target streaming table `dim_user` with expectations enforced
# 3) Applies CDC into `dim_user` using SCD Type 2 semantics
#
# Assumptions
# -----------
# - `spotify.silver.dim_user` exists and is readable by the DLT pipeline
# - The source includes:
#     * `user_id` (unique business key)
#     * `updated_at` (reliable ordering for change sequencing)
# ============================================================

import dlt


# ------------------------------------------------------------
# 0) Data quality expectations
#
# expectations:
#   - Dictionary of named rules → SQL boolean expressions
#
# expect_all_or_drop:
#   - Rows that fail ANY expectation are dropped from the target table
#
# NOTE:
# Keep expectation names descriptive (e.g. "user_id_not_null") so DLT logs
# are easy to interpret when reviewing pipeline quality metrics.
# ------------------------------------------------------------

expectations = {
    "rule 1": "user_id IS NOT NULL",
}


# ------------------------------------------------------------
# 1) Staging stream
#
# Why a staging table?
# - Keeps the source read isolated and easy to inspect
# - Provides a stable `source` name for the CDC flow
#
# NOTE:
# - `spark.readStream.table(...)` treats the source as a streaming input.
# - The upstream Silver table must support streaming reads.
# ------------------------------------------------------------

@dlt.table
def dim_user_stg():
    """
    Staging stream for dim_user CDC.

    Returns
    -------
    pyspark.sql.DataFrame
        Streaming DataFrame read from the Silver dimension table.
    """
    df = spark.readStream.table("spotify.silver.dim_user")
    return df


# ------------------------------------------------------------
# 2) Create the target table with expectations
#
# name:
#   - Target DLT table name (`dim_user`)
#
# expect_all_or_drop:
#   - Apply all expectations and drop failing rows
#
# Practical tip:
# - Start with strict rules (like non-null keys) and expand gradually.
# - Avoid overly strict rules early unless you're sure they won't drop
#   legitimate data.
# ------------------------------------------------------------

dlt.create_streaming_table(
    name="dim_user",
    expect_all_or_drop=expectations,
)


# ------------------------------------------------------------
# 3) Apply CDC as SCD Type 2
#
# keys:
#   - Business key used to identify user entities (`user_id`)
#
# sequence_by:
#   - Column used to order changes for each key (`updated_at`)
#   - Must be reliable; nulls/out-of-order values can distort history
#
# stored_as_scd_type = 2:
#   - SCD Type 2: inserts new versions of rows when changes occur
#
# once:
#   - If False: run continuously (pipeline-dependent)
#   - If True : run as a one-time processing flow (where supported)
# ------------------------------------------------------------

dlt.create_auto_cdc_flow(
    target="dim_user",
    source="dim_user_stg",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    track_history_column_list=None,
    track_history_except_column_list=None,
    name=None,
    once=False,
)
