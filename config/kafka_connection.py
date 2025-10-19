import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
}

print("Kafka Config:", kafka_config)

def read_from_kafka(spark: SparkSession, topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config['bootstrap.servers'])
        .option("subscribe", topic)
        .option("kafka.security.protocol", kafka_config['security.protocol'])
        .option("kafka.sasl.mechanism", kafka_config['sasl.mechanism'])
        .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["sasl.username"]}" password="{kafka_config["sasl.password"]}";')
        .option("startingOffsets", "latest")  # Start from beginning
        .option("failOnDataLoss", "false")      # Don't fail on missing data
        .load()
    )


   

