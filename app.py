import datetime

from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, firestore
import os
import json
import base64

encoded_credentials = os.environ.get('FIREBASE_CREDENTIALS_BASE64')
if encoded_credentials:
    decoded_credentials = json.loads(base64.b64decode(encoded_credentials))
    cred = credentials.Certificate(decoded_credentials)
    firebase_admin.initialize_app(cred)
else:
    raise ValueError("Firebase credentials environment variable is not set.")

app = Flask(__name__)
db = firestore.client()


@app.route('/post-data', methods=['POST'])
def post_data():
    try:
        data = request.json
        mac = data.get('mac', 'defaultDocument')
        now = datetime.datetime.now()

        year = now.strftime("%Y")
        month = now.strftime("%m")
        week = str(now.isocalendar()[1])
        day = now.strftime("%d")
        hour_block = str(now.hour // 4 * 4).zfill(2) + "-" + str(now.hour // 4 * 4 + 4).zfill(2)

        path = f"devices/{mac}/{year}/{month}/data/{week}/data/{day}/data/{hour_block}"
        document_ref = db.document(path)

        doc = document_ref.get()
        if doc.exists:
            current_data = doc.to_dict().get('data', [])
        else:
            current_data = []

        current_data.append(data)

        document_ref.set({'data': current_data}, merge=True)

        aggregated_data = aggregate_hourly_data(db.collection(path).document("data").get().to_dict().get('data'))
        print(aggregated_data)

        return jsonify({"status": "success", "message": "Data posted to Firestore successfully."}), 200
    except Exception as e:
        print("Error occurred:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500


def aggregate_hourly_data(hourly_block):
    aggregates = {key: {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0}
                  for key in hourly_block[0] if key != 'mac'}

    for entry in hourly_block:
        for key, value in entry.items():
            if key == 'mac':
                continue
            aggregates[key]['min'] = min(aggregates[key]['min'], value)
            aggregates[key]['max'] = max(aggregates[key]['max'], value)
            aggregates[key]['sum'] += value
            aggregates[key]['count'] += 1

    for key in aggregates:
        aggregates[key]['avg'] = aggregates[key]['sum'] / aggregates[key]['count']
        del aggregates[key]['sum'], aggregates[key]['count']

    return aggregates


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
