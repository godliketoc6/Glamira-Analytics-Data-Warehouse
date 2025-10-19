from config.spark_connection import get_spark_session
from config.kafka_connection import read_from_kafka
from data.warehouse.dim_product import build_dim_product
from data.warehouse.dim_date import build_dim_date
from data.warehouse.dim_currency import build_dim_currency
from data.warehouse.dim_device import build_dim_device
from data.warehouse.dim_customer import build_dim_customer
from data.warehouse.dim_diamond_option import build_dim_diamond_option
from data.warehouse.dim_alloy_option import build_dim_alloy_option
from data.warehouse.dim_prd import build_dim_prd
from src.load import save_stream_to_postgres

def main():
    print("🚀 Starting ETL Pipeline...")            
    
    # 1. Start Spark
    spark = get_spark_session(app_name="ETL-DimTables")
    
    # 2. Read from Kafka
    print("📡 Connecting to Kafka...")
    kafka_df = read_from_kafka(spark, topic="product_view")     
    
    # 3. Parse Kafka value (string)
    print("🔄 Parsing JSON data...")
    df_parsed = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    
    # 4. Build dimension tables
    print("🏗️ Building dimension tables...")
    dim_product_df = build_dim_product(df_parsed)
    dim_date_df = build_dim_date(df_parsed)
    dim_currency_df = build_dim_currency(df_parsed, spark)
    dim_device_df = build_dim_device(df_parsed)
    dim_customer_df = build_dim_customer(df_parsed)
    dim_diamond_option_df = build_dim_diamond_option(df_parsed)
    dim_prd_df = build_dim_prd(df_parsed)
    dim_alloy_option_df = build_dim_alloy_option(df_parsed)
    
    # 5. Save to PostgreSQL with different output modes
    print("💾 Saving to PostgreSQL...")
    
    query_product = save_stream_to_postgres(dim_product_df, "dim_product", "update")
    query_date = save_stream_to_postgres(dim_date_df, "dim_date", "append")
    query_currency = save_stream_to_postgres(dim_currency_df, "dim_currency", "append")
    query_device = save_stream_to_postgres(dim_device_df, "dim_device", "append")
    query_customer = save_stream_to_postgres(dim_customer_df, "dim_customer", "append")
    query_diamond = save_stream_to_postgres(dim_diamond_option_df, "dim_diamond_option", "append")
    query_prd = save_stream_to_postgres(dim_prd_df, "dim_prd", "append")
    query_alloy = save_stream_to_postgres(dim_alloy_option_df, "dim_alloy_option", "append")
    
    print("⏳ Waiting for data... (Press Ctrl+C to stop)")
    
    # Wait for all queries
    try:
        query_product.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping all streams...")
        query_product.stop()
        query_date.stop()
        query_currency.stop()
        query_device.stop()
        query_customer.stop()
        query_diamond.stop()
        query_prd.stop()
        query_alloy.stop()
        print("✅ All streams stopped gracefully")

if __name__ == "__main__":
    main()