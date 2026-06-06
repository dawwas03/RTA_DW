import yfinance as yf
from kafka import KafkaProducer
from datetime import datetime
import time
import json
import random

# =========================
# USTAWIENIA
# =========================

TICKERS = ["SPY", "QQQ", "NVDA"]
TOPIC = "market_events"
BOOTSTRAP_SERVERS = "broker:9092"
SLEEP_SECONDS = 30

backup_prices = {
    "SPY": 520.0,
    "QQQ": 450.0,
    "NVDA": 900.0
}

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)


def get_market_event_from_yfinance(ticker):
    """
    Pobiera dane 1-minutowe z yfinance.
    Bierzemy ostatnią świecę z wolumenem > 0, bo najnowsza świeca może być jeszcze niedomknięta.
    """

    try:
        df = yf.Ticker(ticker).history(period="1d", interval="1m")

        if df.empty:
            return None

        df = df.dropna()

        # zostawiamy tylko świece z wolumenem większym niż 0
        df = df[df["Volume"] > 0]

        if len(df) < 2:
            return None

        previous = df.iloc[-2]
        current = df.iloc[-1]

        previous_price = float(previous["Close"])
        current_price = float(current["Close"])

        if previous_price == 0:
            return None

        change_pct = ((current_price - previous_price) / previous_price) * 100

        event = {
            "ticker": ticker,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "price": round(current_price, 4),
            "volume": int(current["Volume"]),
            "change_pct": round(change_pct, 4),
            "hour": datetime.now().hour,
            "source": "yfinance",
            "status": "OK"
        }

        return event

    except Exception as e:
        print(f"Błąd yfinance dla {ticker}: {e}")
        return None


def get_market_event_from_backup(ticker):
    """
    Generator backupowy używany, gdy yfinance nie zwróci poprawnych danych, bo np. giełda jest zamknięta.
    """

    old_price = backup_prices[ticker]

    change_pct = random.choices(
        population=[
            random.uniform(-0.05, 0.05),
            random.uniform(-0.20, 0.20),
            random.uniform(-0.80, 0.80)
        ],
        weights=[80, 15, 5],
        k=1
    )[0]

    new_price = old_price * (1 + change_pct / 100)
    backup_prices[ticker] = new_price

    event = {
        "ticker": ticker,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "price": round(new_price, 4),
        "volume": random.randint(50_000, 5_000_000),
        "change_pct": round(change_pct, 4),
        "hour": datetime.now().hour,
        "source": "backup_generator",
        "status": "OK"
    }

    return event


print("Start Kafka producer dla danych rynkowych.")
print("Topic:", TOPIC)
print("Bootstrap servers:", BOOTSTRAP_SERVERS)
print("Instrumenty:", ", ".join(TICKERS))
print(f"Częstotliwość wysyłki: co {SLEEP_SECONDS} sekund")
print("Zatrzymanie: CTRL + C")
print("-" * 100)

while True:
    for ticker in TICKERS:
        event = get_market_event_from_yfinance(ticker)

        if event is None:
            event = get_market_event_from_backup(ticker)

        producer.send(TOPIC, event)

        print("Wysłano do Kafki:", json.dumps(event, ensure_ascii=False))

    producer.flush()
    print("-" * 100)

    time.sleep(SLEEP_SECONDS)
