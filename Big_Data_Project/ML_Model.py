from kafka import KafkaConsumer, KafkaProducer
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import pandas as pd
import json
import logging

logging.basicConfig(level=logging.INFO)

consumer = KafkaConsumer('users_created', bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: json.loads(x.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Consume data
data = []
for message in consumer:
    user_data = message.value
    data.append(user_data)
    if len(data) >= 50:  # Process in batches of 50
        df = pd.DataFrame(data)
        features = df[['age', 'location_latitude', 'location_longitude']]
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(features)

        kmeans = KMeans(n_clusters=5, random_state=42)
        clusters = kmeans.fit_predict(X_scaled)

        df['cluster'] = clusters
        for _, row in df.iterrows():
            producer.send('clustered_users', row.to_dict())

        data = []  # Clear the data list
