import csv
import math
from confluent_kafka import Producer
import time

# Kafka producer configurations
configs = {'bootstrap.servers': '127.0.0.1:9092'}
producer = Producer(configs)

# Callback function to track delivery status
def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [Partition={msg.partition()}] at Offset={msg.offset()}")

# Prepare the data to send and also save to CSV
data = []
for i in range(10000):
    value = round(abs(100 * math.sin(i / 100)), 3)  # Generating a sine wave value
    key = str(i)

    # Produce message to Kafka with a callback for each message
    producer.produce('demo_python', key=key, value=str(value), callback=delivery_report)

    # Collect the data for writing into CSV later
    data.append({"Timestamp": i, "Value": value})

    # Poll to handle delivery reports
    producer.poll(0)

# Flush to ensure all messages are sent before exiting
producer.flush()

# Write the data to a CSV file
csv_file_path = '../generate_data/streaming_data.csv'
with open(csv_file_path, mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=["Timestamp", "Value"])
    writer.writeheader()
    writer.writerows(data)

print(f"Data successfully written to {csv_file_path}")
