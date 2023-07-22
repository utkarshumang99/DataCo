from kafka import KafkaProducer
import json
import time

# Configure Kafka producer settings
bootstrap_servers = 'localhost:9092'
topic = 'DataCo'

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to send click event messages
def send_click_event(click_event):
    producer.send(topic, value=click_event)

# Simulate generating click event data
def generate_click_event(user_id, timestamp, url, ip_address, user_agent):
    return {
        "user_id": user_id,
        "timestamp": timestamp,
        "url": url,
        "ip_address": ip_address,
        "user_agent": user_agent
    }

# Main loop to continuously send click event data
while True:
    click_event_data = generate_click_event("user123", int(time.time()), "https://example.com/page1", "192.168.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36")
    send_click_event(click_event_data)
    time.sleep(1)  # Wait for 1 second before sending the next click event
