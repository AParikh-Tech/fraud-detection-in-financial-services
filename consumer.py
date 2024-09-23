import xgboost as xgb
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import json
import sys


# Load the pre-trained model
bst = xgb.Booster()
bst.load_model('fraud_detection.model')

# Create Spark session
spark = SparkSession.builder \
    .appName("FraudDetection") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")


def predict_fraud(transaction):
    # Extract features
    print(f"Transaction: {transaction}")
    amount = transaction['amt']
    time_of_day = transaction['time_of_day']
    city_pop = transaction['city_pop']
    lat = transaction['lat']
    long = transaction['long']

   # Prepare the full feature vector
    features = [[
        amount,
        time_of_day,
        city_pop,
        lat,
        long
    ]]

    print(features)

    # Make prediction
    fraudulent_score = bst.predict(xgb.DMatrix(features))[0]

    return fraudulent_score


# Process the JSON values
def process_batch(batch_df, batch_id):
    transactions = batch_df.collect()
    print(f"Processing batch {batch_id} with {len(transactions)} records")

    for row in transactions:
        transaction = json.loads(row.json_value)
        fradulent_score = predict_fraud(transaction)

        if fradulent_score > 0.8:
            print(f"Alert: Potential fraud detected for transaction: {transaction}")

# Read from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

# Parse the Kafka message value
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_value")

# Write the output of the parsed stream
query = parsed_stream \
    .writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
