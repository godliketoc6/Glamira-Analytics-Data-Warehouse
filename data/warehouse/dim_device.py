from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from data.warehouse.main_schema import get_main_json_schema, get_dim_device_schema

def build_dim_device(json_df: DataFrame) -> DataFrame:
    """ 
    Build dim_device
    """
    
    main_schema = get_main_json_schema()
    
    parsed_df = json_df.select(
        from_json(col("json_str"), main_schema).alias("data")
    ).select("data.*")
    
    dim_device = parsed_df.withColumn(
        "model",
        regexp_extract(col("user_agent"), r"([A-Z0-9\-]+);?\s?\)", 1)
    ).withColumn(
        "device_type",
        when(col("user_agent").rlike("Android|iPhone|Mobile"), "Mobile")
        .when(col("user_agent").rlike("Windows|Macintosh|Linux"), "Desktop")
        .otherwise("Unknown")
    )
    
    # Extract system (OS) name
    dim_device = dim_device.withColumn(
        "system",
        when(col("user_agent").rlike("Windows"), "Windows")
        .when(col("user_agent").rlike("Macintosh|Mac OS X"), "Mac")
        .when(col("user_agent").rlike("Android"), "Android")
        .when(col("user_agent").rlike("iPhone|iPad|iOS"), "iOS")
        .when(col("user_agent").rlike("Linux"), "Linux")
        .otherwise("Unknown")
    )

    # Extract browser name
    dim_device = dim_device.withColumn(
        "browser",
        when(col("user_agent").rlike("Chrome"), "Chrome")
        .when(col("user_agent").rlike("Safari"), "Safari")
        .when(col("user_agent").rlike("Firefox"), "Firefox")
        .when(col("user_agent").rlike("Edge"), "Edge")
        .when(col("user_agent").rlike("MSIE|Trident"), "Internet Explorer")
        .otherwise("Unknown")
    )
    
    schema = get_dim_device_schema()
    
    casted_df = dim_device.select([
        col(f.name).cast(f.dataType).alias(f.name)
        for f in schema.fields
    ])
    
    return casted_df