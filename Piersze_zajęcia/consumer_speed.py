from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='consumer-speed-v1',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_events = defaultdict(deque)

print("Nasłuchuję anomalii prędkości...")

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    ts = datetime.fromisoformat(tx['timestamp'])

    user_events[user_id].append(ts)

    while user_events[user_id] and (ts - user_events[user_id][0]).total_seconds() > 60:
        user_events[user_id].popleft()

    if len(user_events[user_id]) > 3:
        print(f"ALERT: {user_id} wykonał {len(user_events[user_id])} transakcje w ciągu 60 sekund")
