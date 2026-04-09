from flask import Flask, jsonify
import requests
import json

app = Flask(__name__)

COINGECKO_COINS_LIST_URL = "https://api.coingecko.com/api/v3/coins/list"

@app.route("/coingecko/coins", methods=["GET"])
def get_coins_list():
    response = requests.get(COINGECKO_COINS_LIST_URL, timeout=10)
    response.raise_for_status()

    data = response.json()

    # Size calculations
    json_bytes = len(json.dumps(data).encode("utf-8"))
    json_mb = json_bytes / (1024 * 1024)

    print(f"[CoinGecko] Coins returned: {len(data)}")
    print(f"[CoinGecko] Response size: {json_mb:.2f} MB")

    return jsonify(data)

if __name__ == "__main__":
    app.run(port=5001, debug=True)
