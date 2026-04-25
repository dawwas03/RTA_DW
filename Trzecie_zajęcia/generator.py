# generator.py
import json, os, random, time
from datetime import datetime, timedelta

output_dir = "data/stream"
os.makedirs(output_dir, exist_ok=True)

sklepy = ['Warszawa', 'Kraków', 'Gdańsk', 'Wrocław']
kategorie = ['elektronika', 'odzież', 'żywność', 'książki']

def generate_transaction():
    return {
        'tx_id': f'TX{random.randint(1000,9999)}',
        'user_id': f'u{random.randint(1,20):02d}',
        'amount': round(random.uniform(5.0, 5000.0), 2),
        'store': random.choice(sklepy),
        'category': random.choice(kategorie),
        'timestamp': datetime.now().isoformat(),
    }

# Simulate file-based streaming
while True:
    batch = [generate_transaction() for _ in range(2)]
    filename = f"{output_dir}/events_{int(time.time())}.json"
    with open(filename, "w") as f:
        for e in batch:
            f.write(json.dumps(e) + "\n")
    print(f"Wrote: {filename}")
    time.sleep(5)
