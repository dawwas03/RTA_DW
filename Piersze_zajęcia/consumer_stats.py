from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='consumer-stats-v1',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {
    'count': 0,
    'sum': 0.0,
    'min': float('inf'),
    'max': float('-inf')
})

msg_count = 0

print("Zliczam statystyki per kategoria...")

for message in consumer:
    tx = message.value
    category = tx['category']
    amount = tx['amount']

    stats[category]['count'] += 1
    stats[category]['sum'] += amount
    stats[category]['min'] = min(stats[category]['min'], amount)
    stats[category]['max'] = max(stats[category]['max'], amount)

    msg_count += 1
    print(f"Odebrano {msg_count}: {category} | {amount:.2f}")

    if msg_count % 3 == 0:
        print("\nKategoria | Liczba | Suma | Min | Max")
        print("-" * 55)
        for category, s in stats.items():
            print(f"{category} | {s['count']} | {s['sum']:.2f} | {s['min']:.2f} | {s['max']:.2f}")
