import json
import random
import time
from kafka import KafkaProducer


# Configure Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

locations = ['NY', 'CA', 'IL']
merchants = ["fraud_Keeling-Crist", "fraud_Corwin-Collins", "fraud_Lockman Ltd", "fraud_Harris Inc"]
categories = ["grocery_pos", "gas_transport", "food_dining", "shopping_pos", "misc_net"]

while True:
    # Generate a random transaction
    transaction = {
        'amt': round(random.uniform(10, 500), 2),
        'state': random.choice(locations),
        'merchant': random.choice(merchants),
        'category': random.choice(categories)
    }

    # Send transaction to Kafka
    producer.send('transactions', value=transaction)
    print(f"Sent transaction: {transaction}")

    # Adjust time interval as needed
    time.sleep(3)
