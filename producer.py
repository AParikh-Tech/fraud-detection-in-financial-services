import random
import json
import time
from datetime import datetime
from kafka import KafkaProducer

# Initialize the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Possible locations, merchants, and categories (based on your sample)
locations = [
    {'state': 'NY', 'city_pop': 10000, 'lat': 40.7128, 'long': -74.0060},
    {'state': 'CA', 'city_pop': 15000, 'lat': 34.0522, 'long': -118.2437},
    {'state': 'IL', 'city_pop': 8000, 'lat': 41.8781, 'long': -87.6298}
]


# Function to generate time_of_day feature
def get_time_of_day():
    current_time = datetime.now()
    return current_time.hour + current_time.minute / 60.0

for i in range(5):
    # Select a random location (includes state, city_pop, lat, long)
    location = random.choice(locations)

    # Generate a random transaction
    transaction = {
        'amt': round(random.uniform(10, 500), 2),    # Transaction amount
        'time_of_day': get_time_of_day(),            # Time of transaction in hours
        'city_pop': location['city_pop'],            # Population of the city
        'lat': location['lat'],                      # Latitude of the transaction
        'long': location['long']                     # Longitude of the transaction
    }

    # Send transaction to Kafka topic 'transactions'
    producer.send('transactions', value=transaction)
    print(f"Sent transaction: {transaction}")
    time.sleep(3)
