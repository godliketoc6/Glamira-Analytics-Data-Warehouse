from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from data.warehouse.main_schema import get_main_json_schema, get_dim_date_schema

def build_dim_date(json_df: DataFrame) -> DataFrame:
    """
    Build dim_date table from JSON data containing timestamp information.
    Supports both BIGINT (epoch) and TIMESTAMP input types.
    """
    
    main_schema = get_main_json_schema()
    
    parsed_df = json_df.select(
        from_json(col("json_str"), main_schema).alias("data")
    ).select("data.*")

    # ✅ Normalize time_stamp to TIMESTAMP
    date_df = parsed_df.select(
        when(
            col("time_stamp").isNull(),
            current_timestamp()
        )
        .when(
            col("time_stamp").cast("string").rlike("^[0-9]+$"),  # if numeric epoch
            from_unixtime(col("time_stamp").cast("bigint")).cast(TimestampType())
        )
        .otherwise(col("time_stamp").cast(TimestampType()))  # already a timestamp
        .alias("timestamp_col")
    )

    # Extract components
    date_df = date_df.select(
        to_date(col("timestamp_col")).alias("date_col"),
        col("timestamp_col")
    ).select(
        date_format(col("date_col"), "yyyyMMdd").cast(IntegerType()).alias("date_id"),
        col("date_col").alias("date"),
        dayofmonth(col("date_col")).alias("day"),
        month(col("date_col")).alias("month"),
        date_format(col("date_col"), "MMMM").alias("month_name"),
        year(col("date_col")).alias("year"),
        dayofweek(col("date_col")).alias("weekday"),
        date_format(col("date_col"), "EEEE").alias("weekday_name"),
        when(dayofweek(col("date_col")).isin(1, 7), lit(True)).otherwise(lit(False)).alias("is_weekend")
    ).distinct()

    # Handle nulls
    dim_date_df = date_df.select(
        col("date_id"),
        when(col("date").isNull(), lit(None)).otherwise(col("date")).alias("date"),
        when(col("day").isNull(), lit(-1)).otherwise(col("day")).alias("day"),
        when(col("month").isNull(), lit(-1)).otherwise(col("month")).alias("month"),
        when(col("month_name").isNull() | (col("month_name") == ""), lit("xna")).otherwise(col("month_name")).alias("month_name"),
        when(col("year").isNull(), lit(-1)).otherwise(col("year")).alias("year"),
        when(col("weekday").isNull(), lit(-1)).otherwise(col("weekday")).alias("weekday"),
        when(col("weekday_name").isNull() | (col("weekday_name") == ""), lit("xna")).otherwise(col("weekday_name")).alias("weekday_name"),
        when(col("is_weekend").isNull(), lit(False)).otherwise(col("is_weekend")).alias("is_weekend")
    )

    # Apply schema + remove duplicates
    dim_date_schema = get_dim_date_schema()
    final_df = (
        dim_date_df
        .select([col(field.name).cast(field.dataType).alias(field.name) for field in dim_date_schema.fields])
        .dropDuplicates(["date_id"])
    )

    return final_df
