from kafka import KafkaConsumer
import json

# Konfiguracja konsumenta - najprostsza działająca wersja
consumer = KafkaConsumer(
    'market_events',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


print("Oczekuję na dane.\n")
print("=" * 80)

# Pętla nasłuchująca na nowe wiadomości
for message in consumer:
    data = message.value
    
    # Wyciąganie danych zgodnie z kluczami zdefiniowanymi przez Dawida w 'event'
    ticker = data.get('ticker', 'UNKNOWN')
    price = data.get('price', 0.0)
    volume = data.get('volume', 0)
    change_pct = data.get('change_pct', 0.0)
    source = data.get('source', 'unknown')
    
    # Eleganckie formatowanie wyjścia dla każdego zdarzenia
    print(f"[{source.upper()}] {ticker} | Cena: {price:.2f} | Wolumen: {volume} | Zmiana: {change_pct:.4f}%")
    
    # Prosty test alertu z Waszego planu (Reguła R1: |change_pct| > 0.05%)
    if change_pct is not None and abs(change_pct) > 0.05:
        print(f"Uwaga: {ticker} - Zmiana {change_pct:.4f}% przekracza dopuszczalny próg 0.05%!")
        
    print("-" * 80)
