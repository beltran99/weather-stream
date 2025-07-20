from kafka import KafkaConsumer
import json
import pandas as pd

consumer = KafkaConsumer('weather-data',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=True)

for message in consumer:
    key = message.key
    print(key)

    weather_df = pd.DataFrame(message.value)
    print(weather_df)