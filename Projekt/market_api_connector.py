import json
import requests
import os
from kafka import KafkaConsumer


# KONFIGURACJA
KAFKA_TOPIC = "market_events"
KAFKA_BROKER = "broker:9092"
FLASK_URL = "http://localhost:5000/score"
TEMP_DIR = "./temp_storage"

# Tworzenie folderu tymczasowego, jeśli nie istnieje
if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)
    print(f"Utworzono folder tymczasowy: {TEMP_DIR}")

# Inicjalizacja konsumenta Kafki
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Połączono z Kafką. Nasłuchiwanie na topicu: {KAFKA_TOPIC}")
print(f"Dane będą przesyłane do: {FLASK_URL}")
print("-" * 50)

try:
    for message in consumer:
        event = message.value
        
        # 1. Wybór wymaganych pól
        processed_data = {
            "ticker": event.get("ticker"),
            "timestamp": event.get("timestamp"),
            "price": event.get("price"),
            "volume": event.get("volume"),
            "change_pct": event.get("change_pct"),
            "hour": event.get("hour"),
            "source": event.get("source"),
            "status": event.get("status")
        }
        
        print(f"Przetwarzanie zdarzenia dla: {processed_data['ticker']}")

        # 2. Przekazanie danych do Flask API /score
        try:
            response = requests.post(FLASK_URL, json=processed_data, timeout=5)
            
            if response.status_code == 200:
                result = response.json()
                risk = result.get("risk_level", "UNKNOWN")
                score = result.get("score", 0)
                print(f" -> API Score: {score} | Poziom ryzyka: {risk}")
                
                # Opcjonalne: zapis alertów do folderu tymczasowego
                if risk in ["HIGH", "CRITICAL"]:
                    log_path = os.path.join(TEMP_DIR, "alerts_log.txt")
                    with open(log_path, "a") as f:
                        f.write(f"{processed_data['timestamp']} - {processed_data['ticker']} - {risk}\n")
            else:
                print(f" -> Błąd API: {response.status_code}")
                
        except requests.exceptions.ConnectionError:
            print(" -> BŁĄD: Nie można połączyć się z Flask API. Sprawdź, czy flask_scoring_api.py jest uruchomiony.")
        
        print("-" * 50)

except KeyboardInterrupt:
    print("\nZatrzymano procesor.")
