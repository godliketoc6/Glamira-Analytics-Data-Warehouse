from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from data.warehouse.main_schema import get_main_json_schema

def build_dim_diamond_option(json_df: DataFrame) -> DataFrame:
    """ 
    Build get_dim_diamond_option
    """
    
    main_schema = get_main_json_schema()
    
    parsed_df = json_df.select(
        from_json(col("json_str"), main_schema).alias("data")
    ).select("data.*")
    
    dim_diamond_option = (
        parsed_df
        .withColumn("option", explode(col("option")))
        .select(
            col("option.option_label").alias("optionLabel"),
            col("option.option_id").alias("optionId"),
            col("option.value_label").alias("valueLabel"),
            col("option.value_id").alias("valueId")
        )
        .filter(col("optionLabel") == "diamond")
        .filter(
            ((col("optionLabel").isNotNull()) & (col("optionLabel") != "")) |
            ((col("optionId").isNotNull()) & (col("optionId") != "")) |
            ((col("valueLabel").isNotNull()) & (col("valueLabel") != "")) |
            ((col("valueId").isNotNull()) & (col("valueId") != ""))
        ).dropDuplicates(["optionLabel", "optionId"])
    )
    
    return dim_diamond_option