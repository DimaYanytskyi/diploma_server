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
        client_timestamp = datetime.datetime.fromtimestamp(data.get('timestamp'), tz=datetime.timezone.utc)

        year = client_timestamp.strftime("%Y")
        month = client_timestamp.strftime("%m")
        week = str(client_timestamp.isocalendar()[1])
        day = client_timestamp.strftime("%d")
        hour_block = str(client_timestamp.hour // 4 * 4).zfill(2) + "-" + str(client_timestamp.hour // 4 * 4 + 4).zfill(
            2)

        path = f"devices/{mac}/{year}/{month}/data/{week}/data/{day}/data/{hour_block}/data"
        document_ref = db.collection(path).document("data")

        doc = document_ref.get()
        if doc.exists:
            current_data = doc.to_dict().get('data', [])
        else:
            current_data = []

        current_data.append(data)
        document_ref.set({'data': current_data}, merge=True)

        # hourly data
        aggregated_data = aggregate_hourly_data(db.collection(path).document("data").get().to_dict().get('data'))
        aggregated_data_ref = db.collection(path).document("aggregated")
        aggregated_data_ref.set({'data': aggregated_data}, merge=True)

        # daily data
        hour_block_index = client_timestamp.hour // 4
        aggregated_data_daily = aggregate_data(hour_block_index, aggregated_data, 6)
        path = f"devices/{mac}/{year}/{month}/data/{week}/data/{day}/data"
        aggregated_data_ref = db.collection(path).document("aggregated")
        aggregated_data_ref.set({'data': aggregated_data_daily}, merge=True)

        # weekly data
        day_index = client_timestamp.day // 7
        aggregated_data_weekly = aggregate_data(day_index, aggregated_data_daily, 7)
        path = f"devices/{mac}/{year}/{month}/data/{week}/data"
        aggregated_data_ref = db.collection(path).document("aggregated")
        aggregated_data_ref.set({'data': aggregated_data_weekly}, merge=True)

        # monthly data
        week_index = client_timestamp.month // 5
        aggregated_data_monthly = aggregate_data(week_index, aggregated_data_weekly, 5)
        path = f"devices/{mac}/{year}/{month}/data"
        aggregated_data_ref = db.collection(path).document("aggregated")
        aggregated_data_ref.set({'data': aggregated_data_monthly}, merge=True)

        # yearly data
        month_index = client_timestamp.month - 1
        aggregated_data_yearly = aggregate_data(month_index, aggregated_data_monthly, 12)
        path = f"devices/{mac}/{year}"
        aggregated_data_ref = db.collection(path).document("aggregated")
        aggregated_data_ref.set({'data': aggregated_data_yearly}, merge=True)

        return jsonify({"status": "success", "message": "Data posted to Firestore successfully."}), 200
    except Exception as e:
        print("Error occurred:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500


def aggregate_hourly_data(hourly_block):
    hourly_aggregates = [{} for _ in range(4)]

    for hour_aggregate in hourly_aggregates:
        for key in hourly_block[0]:
            if key not in ['mac', 'timestamp']:
                hour_aggregate[key] = {'min': 0, 'max': 0, 'sum': 0, 'count': 0}

    for entry in hourly_block:
        entry_datetime = datetime.datetime.fromtimestamp(entry['timestamp'])
        hour_index = entry_datetime.hour % 4
        for key, value in entry.items():
            if key in ['mac', 'timestamp']:
                continue
            hour_aggregate = hourly_aggregates[hour_index]
            hour_aggregate[key]['min'] = safe_min(hour_aggregate[key]['min'], value)
            hour_aggregate[key]['max'] = safe_max(hour_aggregate[key]['max'], value)
            hour_aggregate[key]['sum'] += value
            hour_aggregate[key]['count'] += 1

    for hour_aggregate in hourly_aggregates:
        for key in hour_aggregate:
            if 'count' in hour_aggregate[key] and hour_aggregate[key]['count'] > 0:
                hour_aggregate[key]['avg'] = hour_aggregate[key]['sum'] / hour_aggregate[key]['count']
                del hour_aggregate[key]['sum'], hour_aggregate[key]['count']
            else:
                hour_aggregate[key] = {'min': 0, 'avg': 0, 'max': 0}

    return hourly_aggregates


def aggregate_data(time_segment, data_block, array_count):
    aggregates = [{} for _ in range(array_count)]

    for entry in data_block:
        for key, value in entry.items():
            if value['avg'] == 0:
                continue

            if key not in aggregates[time_segment]:
                aggregates[time_segment][key] = {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0}

            aggregates[time_segment][key]['min'] = safe_min(aggregates[time_segment][key]['min'], value.get('min'))
            aggregates[time_segment][key]['max'] = safe_max(aggregates[time_segment][key]['max'], value.get('max'))
            aggregates[time_segment][key]['sum'] = safe_add(aggregates[time_segment][key]['sum'], value.get('avg'))

            if value.get('avg') is not None:
                aggregates[time_segment][key]['count'] += 1

    for key in aggregates[time_segment]:
        if aggregates[time_segment][key]['count'] > 0:
            aggregates[time_segment][key]['avg'] = aggregates[time_segment][key]['sum'] / aggregates[time_segment][key][
                'count']
            del aggregates[time_segment][key]['sum'], aggregates[time_segment][key]['count']
        else:
            aggregates[time_segment][key] = {'min': None, 'max': None, 'avg': None}

    return aggregates


# def aggregate_data(time_segment, data_block, array_count):
#     aggregates = [{} for _ in range(array_count)]
#
#     for entry in data_block:
#         for key, value in entry.items():
#             if key not in aggregates[time_segment]:
#                 aggregates[time_segment][key] = \
#                     {'min': 0, 'max': 0, 'sum': 0, 'count': 0}
#
#                 aggregates[time_segment][key]['min'] = safe_min(aggregates[time_segment][key]['min'], value['min'])
#                 aggregates[time_segment][key]['max'] = safe_max(aggregates[time_segment][key]['max'], value['max'])
#                 aggregates[time_segment][key]['sum'] = safe_add(aggregates[time_segment][key]['sum'], value['avg'])
#                 aggregates[time_segment][key]['count'] += 1
#
#     for key in aggregates[time_segment]:
#         if aggregates[time_segment][key]['count'] > 0:
#             aggregates[time_segment][key]['avg'] = (aggregates[time_segment][key]['sum'] /
#                                                     aggregates[time_segment][key]['count'])
#             del aggregates[time_segment][key]['sum'], aggregates[time_segment][key]['count']
#         else:
#             aggregates[time_segment][key] = {'min': 0, 'max': 0, 'avg': 0}
#
#     return aggregates


def safe_add(a, b):
    if a is None and b is not None:
        return b
    elif a is not None and b is None:
        return a
    elif a is None and b is None:
        return None
    else:
        return a + b


def safe_min(a, b):
    if a is None: return b
    if b is None: return a
    return min(a, b)


def safe_max(a, b):
    if a is None: return b
    if b is None: return a
    return max(a, b)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
