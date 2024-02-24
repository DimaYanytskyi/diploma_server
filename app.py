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
        aggregated_data_daily = aggregate_daily_blocks(hour_block_index, aggregated_data)
        path = f"devices/{mac}/{year}/{month}/data/{week}/data/{day}/data"
        aggregated_data_ref = db.collection(path).document("aggregated")
        aggregated_data_ref.set({'data': aggregated_data_daily}, merge=True)

        # weekly data
        day_index = client_timestamp.day // 7
        aggregated_data_weekly = aggregate_weekly_blocks(day_index, aggregated_data_daily)
        path = f"devices/{mac}/{year}/{month}/data/{week}/data"
        aggregated_data_ref = db.collection(path).document("aggregated")
        aggregated_data_ref.set({'data': aggregated_data_weekly}, merge=True)

        # monthly data
        week_index = client_timestamp.month // 5
        aggregated_data_monthly = aggregate_monthly_blocks(week_index, aggregated_data_weekly)
        path = f"devices/{mac}/{year}/{month}/data"
        aggregated_data_ref = db.collection(path).document("aggregated")
        aggregated_data_ref.set({'data': aggregated_data_monthly}, merge=True)

        return jsonify({"status": "success", "message": "Data posted to Firestore successfully."}), 200
    except Exception as e:
        print("Error occurred:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500


def aggregate_hourly_data(hourly_block):
    hourly_aggregates = [{} for _ in range(4)]

    for hour_aggregate in hourly_aggregates:
        for key in hourly_block[0]:
            if key not in ['mac', 'timestamp']:
                hour_aggregate[key] = {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0}

    for entry in hourly_block:
        entry_datetime = datetime.datetime.fromtimestamp(entry['timestamp'])
        hour_index = entry_datetime.hour % 4
        for key, value in entry.items():
            if key in ['mac', 'timestamp']:
                continue
            hour_aggregate = hourly_aggregates[hour_index]
            hour_aggregate[key]['min'] = min(hour_aggregate[key]['min'], value)
            hour_aggregate[key]['max'] = max(hour_aggregate[key]['max'], value)
            hour_aggregate[key]['sum'] += value
            hour_aggregate[key]['count'] += 1

    for hour_aggregate in hourly_aggregates:
        for key in hour_aggregate:
            if 'count' in hour_aggregate[key] and hour_aggregate[key]['count'] > 0:
                hour_aggregate[key]['avg'] = hour_aggregate[key]['sum'] / hour_aggregate[key]['count']
                del hour_aggregate[key]['sum'], hour_aggregate[key]['count']
            else:
                hour_aggregate[key] = {'min': None, 'avg': None, 'max': None}

    return hourly_aggregates


def aggregate_daily_blocks(hour_block_index, hourly_block):
    daily_aggregates = [{} for _ in range(6)]

    for entry in hourly_block:
        for key, value in entry.items():
            if key not in daily_aggregates[hour_block_index]:
                daily_aggregates[hour_block_index][key] = \
                    {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0}

                daily_aggregates[hour_block_index][key]['min'] = min(daily_aggregates[hour_block_index][key]['min'],
                                                                     value['min'])
                daily_aggregates[hour_block_index][key]['max'] = max(daily_aggregates[hour_block_index][key]['max'],
                                                                     value['max'])
                daily_aggregates[hour_block_index][key]['sum'] += value['avg']
                daily_aggregates[hour_block_index][key]['count'] += 1

    for key in daily_aggregates[hour_block_index]:
        if daily_aggregates[hour_block_index][key]['count'] > 0:
            daily_aggregates[hour_block_index][key]['avg'] = (daily_aggregates[hour_block_index][key]['sum'] /
                                                              daily_aggregates[hour_block_index][key]['count'])
            del daily_aggregates[hour_block_index][key]['sum'], daily_aggregates[hour_block_index][key]['count']
        else:
            daily_aggregates[hour_block_index][key] = {'min': None, 'max': None, 'avg': None}

    return daily_aggregates


def aggregate_weekly_blocks(day, day_block):
    week_aggregates = [{} for _ in range(7)]

    for entry in day_block:
        for key, value in entry.items():
            if key not in week_aggregates[day]:
                week_aggregates[day][key] = \
                    {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0}

                week_aggregates[day][key]['min'] = min(week_aggregates[day][key]['min'], value['min'])
                week_aggregates[day][key]['max'] = max(week_aggregates[day][key]['max'], value['max'])
                week_aggregates[day][key]['sum'] += value['avg']
                week_aggregates[day][key]['count'] += 1

    for key in week_aggregates[day]:
        if week_aggregates[day][key]['count'] > 0:
            week_aggregates[day][key]['avg'] = (week_aggregates[day][key]['sum'] / week_aggregates[day][key]['count'])
            del week_aggregates[day][key]['sum'], week_aggregates[day][key]['count']
        else:
            week_aggregates[day][key] = {'min': None, 'max': None, 'avg': None}

    return week_aggregates


def aggregate_monthly_blocks(week, week_block):
    month_aggregates = [{} for _ in range(5)]

    for entry in week_block:
        for key, value in entry.items():
            if key not in month_aggregates[week]:
                month_aggregates[week][key] = \
                    {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0}

                month_aggregates[week][key]['min'] = min(month_aggregates[week][key]['min'], value['min'])
                month_aggregates[week][key]['max'] = max(month_aggregates[week][key]['max'], value['max'])
                month_aggregates[week][key]['sum'] += value['avg']
                month_aggregates[week][key]['count'] += 1

    for key in month_aggregates[week]:
        if month_aggregates[week][key]['count'] > 0:
            month_aggregates[week][key]['avg'] = (month_aggregates[week][key]['sum']
                                                  / month_aggregates[week][key]['count'])
            del month_aggregates[week][key]['sum'], month_aggregates[week][key]['count']
        else:
            month_aggregates[week][key] = {'min': None, 'max': None, 'avg': None}

    return month_aggregates


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
