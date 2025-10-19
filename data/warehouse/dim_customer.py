from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from data.warehouse.main_schema import get_main_json_schema

def build_dim_customer(json_df: DataFrame) -> DataFrame:
    """
    Build dim_customer
    """
    
    main_schema = get_main_json_schema()
    
    parsed_df = json_df.select(
        from_json(col("json_str"), main_schema).alias("data")
    ).select("data.*")
    
    dim_customer = (
        parsed_df
        .select(
            trim(col("user_id_db")).alias("user_id"),
            trim(col("email_address")).alias("email_address"),
            col("show_recommendation")
        )
        .filter(
            (col("user_id").isNotNull()) & (col("user_id") != "") |
            (col("email_address").isNotNull()) & (col("email_address") != "")
        )
        .dropDuplicates(["user_id", "email_address"])
    )
    
    return dim_customer