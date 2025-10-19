import os
from pyspark.sql import DataFrame
from dotenv import load_dotenv

load_dotenv()

def save_stream_to_postgres(df: DataFrame, table_name: str, output_mode: str, debug: bool = True):
    """
    Save streaming DataFrame to PostgreSQL.
    
    Args:
        df: Streaming DataFrame to save
        table_name: Target table name in PostgreSQL
        output_mode: Streaming output mode ('append', 'update', 'complete')
        debug: If True, print batch data to console (default: True)
    
    Returns:
        StreamingQuery object
    """
    def write_batch(batch_df, batch_id):
        if batch_df.count() > 0:
            if debug:
                print(f"\n{'='*60}")
                print(f"📦 Table: {table_name} | Batch ID: {batch_id} | Rows: {batch_df.count()}")
                print(f"{'='*60}")
                batch_df.show(truncate=False)
                print(f"{'='*60}\n")
            
            # Write to PostgreSQL
            batch_df.write \
                .format("jdbc") \
                .option("url", os.getenv("POSTGRES_URL")) \
                .option("dbtable", table_name) \
                .option("user", os.getenv("POSTGRES_USER")) \
                .option("password", os.getenv("POSTGRES_PASSWORD")) \
                .option("driver", os.getenv("POSTGRES_DRIVER")) \
                .mode("append") \
                .save()
            
            if debug:
                print(f"✅ Successfully saved {batch_df.count()} rows to {table_name}\n")
        else:
            if debug:
                print(f"⚠️  Empty batch for {table_name} (Batch ID: {batch_id})")
    
    query = df.writeStream \
        .foreachBatch(write_batch) \
        .outputMode(output_mode) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    return query