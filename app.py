import datetime
import json
import os

from flask import Flask, request, jsonify
from flask_mysqldb import MySQL
import pymysql
import pymysql.cursors

app = Flask(__name__)

app.config['MYSQL_HOST'] = 'az479382.mysql.tools'
app.config['MYSQL_USER'] = 'az479382_site'
app.config['MYSQL_PASSWORD'] = 'x9vT68P7Uf'
app.config['MYSQL_DB'] = 'az479382_db'
app.config['MYSQL_CURSORCLASS'] = 'DictCursor'

mysql = MySQL(app)


@app.route('/post-data', methods=['POST'])
def post_data():
    try:
        data = request.json
        mac = data.get('mac')
        timestamp = data.get('timestamp')

        client_timestamp = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)

        year = client_timestamp.strftime("%Y")
        month = client_timestamp.strftime("%m")
        week = str(client_timestamp.isocalendar()[1])
        day = client_timestamp.strftime("%d")
        hour_block = str(client_timestamp.hour // 4 * 4).zfill(2) + "-" + str(client_timestamp.hour // 4 * 4 + 4).zfill(
            2)

        # Insert raw data
        cursor = mysql.connection.cursor()
        cursor.execute("SELECT DeviceID FROM Devices WHERE MACAddress = %s", (mac,))
        device = cursor.fetchone()

        if not device:
            cursor.execute("INSERT INTO Devices (MACAddress) VALUES (%s)", (mac,))
            mysql.connection.commit()
            cursor.execute("SELECT LAST_INSERT_ID() AS DeviceID")
            device = cursor.fetchone()

        device_id = device['DeviceID']

        cursor.execute(
            "INSERT INTO RawData (DeviceID, Timestamp, Data) VALUES (%s, %s, %s)",
            (device_id, client_timestamp, json.dumps(data))
        )
        mysql.connection.commit()

        # Perform aggregation
        aggregate_data(device_id, client_timestamp)

        return jsonify({"status": "success", "message": "Data posted to MySQL successfully."}), 200

    except Exception as e:
        print("Error occurred:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500


def aggregate_data(device_id, timestamp):
    cursor = mysql.connection.cursor()

    # Aggregate hourly data
    aggregate_hourly_data(cursor, device_id, timestamp)

    # Aggregate daily data
    aggregate_periodic_data(cursor, device_id, timestamp, 'hour', 6, 'day', 24)

    # Aggregate weekly data
    aggregate_periodic_data(cursor, device_id, timestamp, 'day', 7, 'week', 7)

    # Aggregate monthly data
    aggregate_periodic_data(cursor, device_id, timestamp, 'week', 4, 'month', 4)

    # Aggregate yearly data
    aggregate_periodic_data(cursor, device_id, timestamp, 'month', 12, 'year', 12)

    mysql.connection.commit()


def aggregate_hourly_data(cursor, device_id, timestamp):
    hour_start = timestamp.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + datetime.timedelta(hours=1)

    cursor.execute("""
        SELECT Data FROM RawData
        WHERE DeviceID = %s AND Timestamp >= %s AND Timestamp < %s
    """, (device_id, hour_start, hour_end))

    raw_data = [json.loads(row['Data']) for row in cursor.fetchall()]
    aggregated_data = calculate_aggregates(raw_data)

    cursor.execute("""
        INSERT INTO AggregatedData (DeviceID, PeriodType, PeriodStart, Data)
        VALUES (%s, 'hour', %s, %s)
        ON DUPLICATE KEY UPDATE Data = %s
    """, (device_id, hour_start, json.dumps(aggregated_data), json.dumps(aggregated_data)))


def aggregate_periodic_data(cursor, device_id, timestamp, from_period, from_count, to_period, to_count):
    period_start = timestamp.replace(minute=0, second=0, microsecond=0)
    if from_period == 'hour':
        period_start = period_start.replace(hour=(period_start.hour // from_count) * from_count)
    elif from_period == 'day':
        period_start = period_start - datetime.timedelta(days=(period_start.weekday() % from_count))
    elif from_period == 'week':
        period_start = period_start.replace(day=1)
    elif from_period == 'month':
        period_start = period_start.replace(month=((period_start.month - 1) // from_count) * from_count + 1, day=1)

    cursor.execute("""
        SELECT Data FROM AggregatedData
        WHERE DeviceID = %s AND PeriodType = %s AND PeriodStart >= %s AND PeriodStart < %s
    """, (device_id, from_period, period_start, period_start + datetime.timedelta(**{to_period: to_count})))

    raw_data = [json.loads(row['Data']) for row in cursor.fetchall()]
    aggregated_data = calculate_aggregates(raw_data)

    cursor.execute("""
        INSERT INTO AggregatedData (DeviceID, PeriodType, PeriodStart, Data)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE Data = %s
    """, (device_id, to_period, period_start, json.dumps(aggregated_data), json.dumps(aggregated_data)))


def calculate_aggregates(data_block):
    aggregates = {}

    for entry in data_block:
        for key, value in entry.items():
            if key in ['mac', 'timestamp']:
                continue

            if key not in aggregates:
                aggregates[key] = {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0}

            aggregates[key]['min'] = min(aggregates[key]['min'], value)
            aggregates[key]['max'] = max(aggregates[key]['max'], value)
            aggregates[key]['sum'] += value
            aggregates[key]['count'] += 1

    for key in aggregates:
        aggregates[key]['avg'] = aggregates[key]['sum'] / aggregates[key]['count'] if aggregates[key][
                                                                                          'count'] > 0 else None
        del aggregates[key]['sum'], aggregates[key]['count']

    return aggregates


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)


"""import datetime

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
                hour_aggregate[key] = {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0}

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
                hour_aggregate[key] = {'min': None, 'max': None, 'avg': None}

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
    if a is None:
        return b
    if b is None:
        return a
    return min(a, b)


def safe_max(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
    """
