from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, explode, first, when, lit
from data.warehouse.main_schema import get_main_json_schema

def build_dim_prd(json_df: DataFrame) -> DataFrame:
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


    # 3️⃣ Now you can safely select product-related fields
    dim_prd = parsed_df.select(
        "product_id",
        "current_url"
    )

    return dim_prd
