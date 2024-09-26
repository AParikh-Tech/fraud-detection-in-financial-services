import random
import json
import time
from datetime import datetime
import pandas as pd
from kafka import KafkaProducer

# Initialize the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Load the test data
test_df = pd.read_csv('test_data.csv')

# Convert columns to numeric, forcing errors to NaN
numeric_columns = ['amt', 'city_pop', 'lat', 'long']
for col in numeric_columns:
    test_df[col] = pd.to_numeric(test_df[col], errors='coerce')

# Drop rows with NaN values to deal only with a complete clean set
test_df.dropna()

# Randomly sample rows from the test data for simulation
test_data_samples = test_df.sample(frac=1).reset_index(drop=True)  # Shuffle the data for randomness

# Function to generate time_of_day feature
def get_time_of_day(trans_date_trans_time):
    time = pd.to_datetime(trans_date_trans_time)
    return time.hour + time.minute / 60.0

for index, row in test_data_samples.iterrows():
    transaction = {
        'amt': row['amt'],
        'time_of_day': get_time_of_day(row['trans_date_trans_time']),
        'city_pop': row['city_pop'],
        'lat': row['lat'],
        'long': row['long'],
        'merchant': row['merchant'],
        'category': row['category'],
        'state': row['state']
    }

    # Send transaction to Kafka topic 'transactions'
    producer.send('transactions', value=transaction)
    print(f"Sent transaction: {transaction}")
    time.sleep(1)
