from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, explode, first, when, lit
from data.warehouse.main_schema import get_main_json_schema

def build_dim_product(json_df: DataFrame) -> DataFrame:
    """
    Build the product dimension from the raw JSON stream.
    """
    # 1️⃣ Get your main JSON schema
    main_schema = get_main_json_schema()

    # 2️⃣ Parse the JSON string from Kafka
    parsed_df = (
        json_df
        .select(from_json(col("json_str"), main_schema).alias("data"))
        .select("data.*")
    )
    
    exploded_df = parsed_df.withColumn("option", explode(col("option")))

    # 3️⃣ Now you can safely select product-related fields
    dim_product = (
        exploded_df
        .withWatermark("time_stamp", "10 minutes")  # or any window duration
        .groupBy("product_id", "collection")
        .agg(
            first(
                when(col("option.option_label") == "diamond", col("option.option_id")),
                ignorenulls=True
            ).alias("diamond_option_id"),
            first(
                when(col("option.option_label") == "alloy", col("option.option_id")),
                ignorenulls=True
            ).alias("alloy_option_id")
        )
        .withColumn(
            "diamond_option_id",
            when(col("diamond_option_id").isNull() | (col("diamond_option_id") == ""), "xna")
            .otherwise(col("diamond_option_id"))
        )
        .withColumn(
            "alloy_option_id",
            when(col("alloy_option_id").isNull() | (col("alloy_option_id") == ""), "xna")
            .otherwise(col("alloy_option_id"))
        )
    )

    return dim_product
