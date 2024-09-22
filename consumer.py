import xgboost as xgb
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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


def encode_location(location):
    # One-hot encoding for location (example for NY, LA, and CHI)
    locations = ['NY', 'CA', 'IL']
    return [1 if loc == location else 0 for loc in locations]


def predict_fraud(transaction):
    # Extract features
    amount = transaction['amount']
    location_encoded = encode_location(transaction['location']) # One hot encoding
    time_of_day = transaction['time_of_day']
    merchant_category = transaction['merchant_category']
    user_avg_transaction = transaction['user_avg_transaction']

    # Prepare features as a 2D array
    features = [[amount] + location_encoded + [time_of_day, merchant_category, user_avg_transaction]]

    # Make prediction
    fradulent_score = bst.predict(xgb.DMatrix(features))[0]

    return fradulent_score


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
