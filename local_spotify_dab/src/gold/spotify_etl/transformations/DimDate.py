import dlt

@dlt.table
def dim_date_stg():
    df = spark.readStream.table(
        "spotify.silver.dim_date"
    )
    return df

dlt.create_streaming_table("dim_date")

dlt.create_auto_cdc_flow(
    target = "dim_date",
    source = "dim_date_stg",
    keys = ["date_key"],
    sequence_by = "date",
    stored_as_scd_type = 2,
    track_history_column_list = None,
    track_history_except_column_list = None,
    name = None,
    once = False
)