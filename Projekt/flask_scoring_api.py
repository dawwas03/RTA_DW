
from flask import Flask, request, jsonify
from datetime import datetime

app = Flask(__name__)


# PAMIĘĆ PODRĘCZNA STATYSTYK
stats = {
    "total_scored": 0,
    "by_risk": {"LOW": 0, "MEDIUM": 0, "HIGH": 0, "CRITICAL": 0},
    "by_ticker": {},
    "alerts_generated": 0,
    "started_at": datetime.now().isoformat(timespec="seconds")
}

# Ostatnie zdarzenia do dashboardu
recent_events = []


# LOGIKA SCORINGU
def score_event(data):
    """
    Przyjmuje słownik zdarzenia rynkowego i zwraca score, risk_level oraz listę
    uruchomionych reguł.

    Reguły:
      R1 – umiarkowana zmiana ceny: |change_pct| > 0.05%  → +1 punkt
      R2 – duża zmiana ceny:        |change_pct| > 0.10%  → +2 punkty
      R3 – bardzo duża zmiana ceny: |change_pct| > 0.20%  → +2 punkty
      R4 – podwyższony wolumen LUB źródło backup_generator → +1 punkt

    Progi ryzyka:
      0     → LOW
      1–2   → MEDIUM
      3–4   → HIGH
      5+    → CRITICAL
    """

    change_pct = abs(data.get("change_pct", 0.0))
    volume = data.get("volume", 0)
    source = data.get("source", "yfinance")

    score = 0
    triggered = []

    # R1
    if change_pct > 0.05:
        score += 1
        triggered.append("R1")

    # R2
    if change_pct > 0.10:
        score += 2
        triggered.append("R2")

    # R3
    if change_pct > 0.20:
        score += 2
        triggered.append("R3")

    # R4
    VOLUME_THRESHOLD = 1_000_000
    if volume > VOLUME_THRESHOLD or source == "backup_generator":
        score += 1
        triggered.append("R4")

    # Mapowanie score → risk_level
    if score == 0:
        risk_level = "LOW"
    elif score <= 2:
        risk_level = "MEDIUM"
    elif score <= 4:
        risk_level = "HIGH"
    else:
        risk_level = "CRITICAL"

    return score, risk_level, triggered


# ENDPOINTY

@app.route("/health", methods=["GET"])
def health():
    """Sprawdzenie, czy API działa."""
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat(timespec="seconds")
    }), 200


@app.route("/score", methods=["POST"])
def score():
    """
    Przyjmuje JSON zdarzenia rynkowego, ocenia ryzyko i zwraca wynik.
    Dla zdarzeń HIGH i CRITICAL drukuje alert w terminalu.
    """

    data = request.get_json(silent=True)

    # --- walidacja ---
    if not data:
        return jsonify({"error": "Brak danych JSON w żądaniu"}), 400

    required_fields = ["ticker", "timestamp", "price", "volume", "change_pct", "source"]
    missing = [f for f in required_fields if f not in data]

    if missing:
        return jsonify({"error": f"Brakujące pola: {', '.join(missing)}"}), 400

    if not isinstance(data.get("price"), (int, float)) or data["price"] <= 0:
        return jsonify({"error": "Pole 'price' musi być dodatnią liczbą"}), 400

    if not isinstance(data.get("volume"), int) or data["volume"] < 0:
        return jsonify({"error": "Pole 'volume' musi być nieujemną liczbą całkowitą"}), 400

    # --- scoring ---
    score_val, risk_level, triggered_rules = score_event(data)

    # --- aktualizacja statystyk ---
    stats["total_scored"] += 1
    stats["by_risk"][risk_level] += 1

    ticker = data.get("ticker", "UNKNOWN")

    if ticker not in stats["by_ticker"]:
        stats["by_ticker"][ticker] = {"total": 0, "HIGH": 0, "CRITICAL": 0}

    stats["by_ticker"][ticker]["total"] += 1

    if risk_level in ("HIGH", "CRITICAL"):
        stats["by_ticker"][ticker][risk_level] += 1

    # --- alert w terminalu ---
    if risk_level in ("HIGH", "CRITICAL"):
        stats["alerts_generated"] += 1

        print(f"\n{'=' * 60}")
        print(f"  ALERT: {ticker} - {risk_level}")
        print(
            f"  Cena: {data.get('price')} | Zmiana: {data.get('change_pct')}%"
            f" | Wolumen: {data.get('volume')}"
        )
        print(f"  Reguły: {', '.join(triggered_rules)} | Score: {score_val}")
        print(f"  Źródło: {data.get('source')} | {data.get('timestamp')}")
        print(f"{'=' * 60}\n")

    # --- odpowiedź API ---
    response = {
        "ticker": ticker,
        "score": score_val,
        "risk_level": risk_level,
        "triggered_rules": triggered_rules,
        "timestamp": data.get("timestamp")
    }

    # --- zapis ostatnich zdarzeń do dashboardu ---
    recent_events.append({
        "ticker": ticker,
        "timestamp": data.get("timestamp"),
        "price": data.get("price"),
        "volume": data.get("volume"),
        "change_pct": data.get("change_pct"),
        "source": data.get("source"),
        "score": score_val,
        "risk_level": risk_level,
        "triggered_rules": triggered_rules
    })

    # Trzymamy tylko ostatnie 20 zdarzeń
    if len(recent_events) > 20:
        recent_events.pop(0)

    return jsonify(response), 200


@app.route("/stats", methods=["GET"])
def get_stats():
    """Zwraca zagregowane statystyki od uruchomienia API."""
    return jsonify({
        "stats": stats,
        "uptime_since": stats["started_at"],
        "current_time": datetime.now().isoformat(timespec="seconds")
    }), 200


@app.route("/dashboard", methods=["GET"])
def dashboard():
    """Prosty dashboard HTML pokazujący stan systemu i ostatnie zdarzenia."""

    rows = ""

    for event in reversed(recent_events):
        risk = event["risk_level"]

        if risk == "LOW":
            color = "#d4edda"
        elif risk == "MEDIUM":
            color = "#fff3cd"
        elif risk == "HIGH":
            color = "#f8d7da"
        else:
            color = "#f5c6cb"

        rows += f"""
        <tr style="background-color:{color}">
            <td>{event['timestamp']}</td>
            <td>{event['ticker']}</td>
            <td>{event['price']}</td>
            <td>{event['volume']}</td>
            <td>{event['change_pct']}%</td>
            <td>{event['source']}</td>
            <td>{event['score']}</td>
            <td><b>{event['risk_level']}</b></td>
            <td>{', '.join(event['triggered_rules'])}</td>
        </tr>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="refresh" content="5">
        <title>Market Risk Dashboard</title>

        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 30px;
                background-color: #f4f6f8;
                color: #1f2933;
            }}

            h1 {{
                color: #003049;
                margin-bottom: 25px;
            }}

            h2 {{
                color: #003049;
                margin-top: 30px;
            }}

            .cards {{
                display: flex;
                gap: 15px;
                margin-bottom: 25px;
                flex-wrap: wrap;
            }}

            .card {{
                background: white;
                padding: 18px;
                border-radius: 10px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.12);
                min-width: 150px;
            }}

            .card-title {{
                font-size: 14px;
                color: #555;
            }}

            .number {{
                font-size: 28px;
                font-weight: bold;
                margin-top: 8px;
                color: #003049;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                background: white;
                box-shadow: 0 2px 6px rgba(0,0,0,0.12);
            }}

            th, td {{
                padding: 10px;
                border-bottom: 1px solid #ddd;
                text-align: left;
                font-size: 14px;
            }}

            th {{
                background-color: #003049;
                color: white;
            }}

            .footer {{
                margin-top: 20px;
                color: #666;
                font-size: 13px;
            }}
        </style>
    </head>

    <body>
        <h1>Market Risk Monitoring Dashboard</h1>

        <div class="cards">
            <div class="card">
                <div class="card-title">Wszystkie ocenione zdarzenia</div>
                <div class="number">{stats["total_scored"]}</div>
            </div>

            <div class="card">
                <div class="card-title">Alerty HIGH/CRITICAL</div>
                <div class="number">{stats["alerts_generated"]}</div>
            </div>

            <div class="card">
                <div class="card-title">LOW</div>
                <div class="number">{stats["by_risk"]["LOW"]}</div>
            </div>

            <div class="card">
                <div class="card-title">MEDIUM</div>
                <div class="number">{stats["by_risk"]["MEDIUM"]}</div>
            </div>

            <div class="card">
                <div class="card-title">HIGH</div>
                <div class="number">{stats["by_risk"]["HIGH"]}</div>
            </div>

            <div class="card">
                <div class="card-title">CRITICAL</div>
                <div class="number">{stats["by_risk"]["CRITICAL"]}</div>
            </div>
        </div>

        <h2>Ostatnie zdarzenia rynkowe</h2>

        <table>
            <tr>
                <th>Czas</th>
                <th>Ticker</th>
                <th>Cena</th>
                <th>Wolumen</th>
                <th>Zmiana %</th>
                <th>Źródło</th>
                <th>Score</th>
                <th>Ryzyko</th>
                <th>Reguły</th>
            </tr>
            {rows}
        </table>

        <div class="footer">
            Dashboard odświeża się automatycznie co 5 sekund.
            Dane są przechowywane w pamięci działania Flask API.
        </div>
    </body>
    </html>
    """

    return html


# START
if __name__ == "__main__":
    print("Flask API uruchomione.")
    print("Endpointy:")
    print("  GET  /health")
    print("  POST /score")
    print("  GET  /stats")
    print("  GET  /dashboard")
    print("-" * 40)

    app.run(host="0.0.0.0", port=5000, debug=True)
