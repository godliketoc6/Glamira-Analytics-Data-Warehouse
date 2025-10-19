from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from data.warehouse.main_schema import get_main_json_schema, get_dim_currency_schema

def build_dim_currency(json_df: DataFrame, spark) -> DataFrame:
    """
    Enrich the raw currency column with details from reference CSV.
    """
    # 1. Parse JSON input
    main_schema = get_main_json_schema()
    
    parsed_df = json_df.select(
        from_json(col("json_str"), main_schema).alias("data")
    ).select("data.*")

    # 2. Keep only raw currency (filter null/empty) 
    raw_currency_df = parsed_df.select(col("currency").alias("raw_currency")) \
        .filter(col("currency").isNotNull() & (col("currency") != ""))

    # 3. Load reference currency CSV (dim_currency lookup table)
    currency_schema = get_dim_currency_schema()
    
    ref_currency_df = spark.read.csv(
        "/data/unigap/currency.csv",
        header=True,
        schema=currency_schema      
    )

    # 4. Join raw with reference table (match by symbol or code depending on data)
    enriched_df = raw_currency_df.join(
        ref_currency_df,
        (raw_currency_df.raw_currency == ref_currency_df.symbol) |
        (raw_currency_df.raw_currency == ref_currency_df.currency_code),
        "left"
    )

    return enriched_df