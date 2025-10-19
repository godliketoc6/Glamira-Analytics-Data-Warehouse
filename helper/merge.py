import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def merge_tables():
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    PORT = os.getenv("PORT", "5432")

    # ✅ Keep host and dbname fixed
    conn = psycopg2.connect(
        host="localhost",
        dbname="postgres",
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=PORT
    )
    cur = conn.cursor()

    print("🚀 Running merge query...")

    query = """
    INSERT INTO dim_product_final (
        product_id, diamond_option_id, alloy_option_id, 
        product_name, collection, price, description
    )
    SELECT 
        dp.product_id,
        dp.diamond_option_id,
        dp.alloy_option_id,
        pi.product_name,
        dp.collection,
        pi.price,
        pi.description
    FROM dim_product dp
    JOIN dim_prd dprd ON dp.product_id = dprd.product_id
    JOIN product_info pi ON dprd.current_url = pi.current_url
    LEFT JOIN dim_product_final f ON dp.product_id = f.product_id
    WHERE f.product_id IS NULL;
    """

    cur.execute(query)
    conn.commit()

    cur.close()
    conn.close()

    print("✅ Merge complete!")

if __name__ == "__main__":
    merge_tables()
