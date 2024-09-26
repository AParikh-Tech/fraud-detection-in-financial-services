import xgboost as xgb
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import numpy as np
import json
import sys


# Load the test data for reference and encoding
test_df = pd.read_csv('test_data.csv')

# Keep the same columns used during training
columns_for_model = ['amt', 'time_of_day', 'city_pop', 'lat', 'long', 'merchant', 'category', 'state']

# Preprocess the test data
test_df['time_of_day'] = pd.to_datetime(test_df['trans_date_trans_time']).dt.hour + pd.to_datetime(test_df['trans_date_trans_time']).dt.minute / 60.0
test_df = test_df[columns_for_model]

# Create a dummy frame using the training structure
encoded_test_df = pd.get_dummies(test_df, columns=['merchant', 'category', 'state'], drop_first=True)

# Get the set of all columns after one-hot encoding from training (saved during training)
# Assuming you have saved the one-hot encoded column structure from training
encoded_columns_train = pd.get_dummies(test_df[columns_for_model], columns=['merchant', 'category', 'state'], drop_first=True).columns.to_list()

# Ensure that test_df has the same one-hot encoded structure as training data
encoded_test_df = encoded_test_df.reindex(columns=encoded_columns_train, fill_value=0)


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
    # print(f"Transaction: {transaction}")

    # Ensure that the transaction only has relevant fields
    relevant_keys = ['amt', 'time_of_day', 'city_pop', 'lat', 'long', 'merchant', 'category', 'state']

    # Ensure all relevant keys are in the transaction
    if not all(key in transaction for key in relevant_keys):
        print("Transaction is missing some required fields.")
        return None

    # Prepare the full feature vector
    feature_values = [[
        transaction['amt'],
        transaction['time_of_day'],
        transaction['city_pop'],
        transaction['lat'],
        transaction['long'],
        transaction['merchant'],
        transaction['category'],
        transaction['state']
    ]]

    input_data = np.array(feature_values)
    input_df = pd.DataFrame(input_data, columns=relevant_keys)

    # Apply the same encoding used during training
    transaction_encoded = pd.get_dummies(input_df, columns=['merchant', 'category', 'state'], drop_first=True)

    # Align with training columns
    transaction_encoded = transaction_encoded.reindex(columns=encoded_columns_train, fill_value=0)

    # Ensure all columns are numeric
    transaction_encoded = transaction_encoded.apply(pd.to_numeric, errors='coerce')

    # Make prediction
    fraudulent_score = bst.predict(xgb.DMatrix(transaction_encoded))[0]

    return fraudulent_score


# Process the JSON values
def process_batch(batch_df, batch_id):
    transactions = batch_df.collect()
    print(f"Processing batch {batch_id} with {len(transactions)} records")

    for row in transactions:
        transaction = json.loads(row.json_value)
        fradulent_score = predict_fraud(transaction)

        print(f"Fradulent Score: {fradulent_score}")

        if fradulent_score > 0.8:
            print(f"Alert: Potential fraud detected for transaction: {transaction}")
        else:
            print(f"Legit Transaction: {transaction}")

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
