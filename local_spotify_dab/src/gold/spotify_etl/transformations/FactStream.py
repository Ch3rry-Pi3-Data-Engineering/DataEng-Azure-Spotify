# ============================================================
# Delta Live Tables (DLT): CDC flow for fact_stream (SCD Type 1)
#
# Goal
# ----
# Maintain a CDC-applied fact table called `fact_stream` from the Silver
# source table `spotify.silver.fact_stream`.
#
# Key design choices
# ------------------
# - keys        : ["stream_id"]         (unique identifier for each fact event)
# - sequence_by : "stream_timestamp"    (orders changes for each key)
# - SCD type    : 1                     (overwrite / upsert semantics)
#
# Why SCD Type 1 for a fact table?
# -------------------------------
# Fact tables are typically append-only. However, if upstream events can be:
#   - late arriving
#   - corrected (e.g. updated listen_duration, fixed user_id, etc.)
#   - reprocessed with more accurate attributes
#
# then SCD Type 1 provides "latest truth" behaviour:
#   - the record for a given stream_id is updated in place
#   - history is not preserved (unlike SCD Type 2)
#
# Assumptions
# -----------
# - `spotify.silver.fact_stream` exists and is readable by the pipeline
# - The source includes:
#     * `stream_id` (unique identifier / business key)
#     * `stream_timestamp` (reliable sequencing column for CDC ordering)
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
def fact_stream_stg():
    """
    Staging stream for fact_stream CDC.

    Returns
    -------
    pyspark.sql.DataFrame
        Streaming DataFrame read from the Silver fact table.
    """
    df = spark.readStream.table("spotify.silver.fact_stream")
    return df


# ------------------------------------------------------------
# 2) Create the target table
#
# This declares the target table that will receive CDC-applied rows.
# ------------------------------------------------------------

dlt.create_streaming_table("fact_stream")


# ------------------------------------------------------------
# 3) Apply CDC as SCD Type 1
#
# keys:
#   - Unique identifier for fact events (`stream_id`)
#   - Used to match incoming records to existing target records
#
# sequence_by:
#   - Orders changes for each key (`stream_timestamp`)
#   - Must be reliable; if events arrive out of order, updates could be applied
#     incorrectly without a stable sequencing strategy
#
# stored_as_scd_type = 1:
#   - SCD Type 1: maintain the latest version only (overwrite/upsert)
#   - No historical row versions are kept
#
# once:
#   - If False: run continuously (pipeline-dependent)
#   - If True : run as a one-time processing run (where supported)
# ------------------------------------------------------------

dlt.create_auto_cdc_flow(
    target="fact_stream",
    source="fact_stream_stg",
    keys=["stream_id"],
    sequence_by="stream_timestamp",
    stored_as_scd_type=1,
    track_history_column_list=None,
    track_history_except_column_list=None,
    name=None,
    once=False,
)
