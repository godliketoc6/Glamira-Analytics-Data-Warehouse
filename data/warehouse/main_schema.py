from pyspark.sql.types import *

def get_main_json_schema() -> StructType:
    """
    Define the main JSON schema for parsing incoming Kafka messages
    This schema matches your actual JSON data structure
    """
    
    # Define schema for option array
    option_schema = ArrayType(
        StructType([
            StructField("option_label", StringType(), True),
            StructField("option_id", StringType(), True),
            StructField("value_label", StringType(), True),
            StructField("value_id", StringType(), True)
        ])
    )
    
    # Main JSON schema
    return StructType([
        StructField("_id", StringType(), True),
        StructField("time_stamp", TimestampType(), True),
        StructField("ip", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("resolution", StringType(), True),
        StructField("user_id_db", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("api_version", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("local_time", StringType(), True),
        StructField("show_recommendation", StringType(), True),
        StructField("current_url", StringType(), True),
        StructField("referrer_url", StringType(), True),
        StructField("email_address", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("option", option_schema, True),
        StructField("id", StringType(), True),
        StructField("currency", StringType(), True),
    ])

def get_dim_product_schema() -> StructType:
    """
    Define the schema for dim_product table
    """
    return StructType([
        StructField("product_id", StringType(), False),  # Primary key, not null
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("raw_price", StringType(), True),  # TEXT field in your schema
        StructField("price", DecimalType(10, 2), True),
        StructField("quality", StringType(), True),
        StructField("quality_label", StringType(), True),
        StructField("record_created_date", TimestampType(), True),
        StructField("record_expired_date", TimestampType(), True),
        StructField("is_current_record", BooleanType(), True)
    ])

def get_dim_date_schema() -> StructType:
    """
    Define the schema for dim_date table
    """
    return StructType([
        StructField("date_id", IntegerType(), False),  # Primary key
        StructField("date", DateType(), True),
        StructField("day", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("month_name", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("weekday", IntegerType(), True),
        StructField("weekday_name", StringType(), True),
        StructField("is_weekend", BooleanType(), True)
    ])
    
def get_dim_currency_schema() -> StructType:
    """
    Define the schema for dim_currency table
    """
    return StructType([
        StructField("currency_code", StringType(), False),
        StructField("currency_name", StringType(), True),
        StructField("symbol", StringType(), True),      
        StructField("country_code", StringType(), True),
        StructField("usd_exchange_rate", DecimalType(18, 6), True)
    ])

def get_dim_device_schema() -> StructType:
    return StructType([
        StructField("device_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("model", StringType(), True),
        StructField("system", StringType(), True),
        StructField("browser", StringType(), True)
    ])
    
