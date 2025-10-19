from pyspark.sql import SparkSession

def get_spark_session(app_name="DataEngineeringPipeline"):
    """
    Get or create Spark session - works with spark-submit
    """
    print(f"🔧 Getting Spark session for: {app_name}")
    
    try:
        # Try to get existing session first
        spark = SparkSession.getActiveSession()
        if spark is not None:
            print(f"✅ Found existing Spark session: {spark.version}")
            spark.sparkContext.setLogLevel("ERROR")
            return spark
    except:
        pass
    
    # Create new session (this works even with spark-submit)
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        print(f"✅ Created Spark session: {spark.version}")
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        print(f"❌ Failed to get Spark session: {e}")
        raise