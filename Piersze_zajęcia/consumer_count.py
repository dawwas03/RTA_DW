from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

print("Zliczam transakcje per sklep...")

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']

    store_counts[store] += 1

    if store not in total_amount:
        total_amount[store] = 0
    total_amount[store] += amount

    msg_count += 1

    if msg_count % 10 == 0:
        print("\nSklep | Liczba | Suma | Średnia")
        print("-" * 40)

        for store in store_counts:
            count = store_counts[store]
            total = total_amount[store]
            avg = total / count
            print(f"{store} | {count} | {total:.2f} | {avg:.2f}")
