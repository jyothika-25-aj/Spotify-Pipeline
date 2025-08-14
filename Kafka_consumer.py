from kafka import KafkaConsumer
import json

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'spotify_tracks',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='spotify-group',
    
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening to topic 'spotify_tracks'...")
# for message in consumer:
#     data = message.value
#     print(f"Received data: {data}")

seen = set()

try: 
    for message in consumer:
        data = message.value
        key = f"{data.get('artist')}:{data.get('track')}"  # Unique key
        if key not in seen:
            print(f"Recieved data: {data}")
            seen.add(key)
            
except KeyboardInterrupt:
    print("Consumer interrupted by user. Exiting..")
