#3.1

from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json, requests

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

API_URL = "http://localhost:8001/score"

#moj kod

for message in consumer:
    tx = message.value
    
    # Wyciągnij cechy
    hour = int(tx.get('hour', datetime.fromisoformat(tx['timestamp']).hour))
    features = {
        "amount": tx['amount'],
        "hour": hour,
        "is_electronics": 1 if tx.get('category') == 'elektronika' else 0,
        "tx_per_day": 5
    }
    
    # Odpytaj API
    response = requests.post(API_URL, json=features)
    result = response.json()
    
    # Jeśli fraud — wyślij alert
    if result['is_fraud']:
        alert = {**tx, **result}
        alert_producer.send('alerts', value=alert)
        print(f"🚨 ML ALERT | {tx['tx_id']} | {tx['amount']:.2f} PLN | prob={result['fraud_probability']}")
