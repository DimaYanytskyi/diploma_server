import datetime
import json
import os
from dateutil.relativedelta import relativedelta
from flask import Flask, request, jsonify
from flask_mysqldb import MySQL
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# MySQL configurations
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
        hour_block = str(client_timestamp.hour // 4 * 4).zfill(2) + "-" + str(client_timestamp.hour // 4 * 4 + 4).zfill(2)

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
        logging.error(f"Error occurred: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

def aggregate_data(device_id, timestamp):
    cursor = mysql.connection.cursor()

    # Aggregate hourly data
    aggregate_hourly_data(cursor, device_id, timestamp)

    # Aggregate daily data
    aggregate_periodic_data(cursor, device_id, timestamp, 'hour', 6, 'days', 1)

    # Aggregate weekly data
    aggregate_periodic_data(cursor, device_id, timestamp, 'day', 7, 'weeks', 1)

    # Aggregate monthly data
    aggregate_periodic_data(cursor, device_id, timestamp, 'week', 4, 'months', 1)

    # Aggregate yearly data
    aggregate_periodic_data(cursor, device_id, timestamp, 'month', 12, 'years', 1)

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

    if to_period in ['days', 'weeks']:
        period_end = period_start + datetime.timedelta(**{to_period: to_count})
    elif to_period == 'months':
        period_end = period_start + relativedelta(months=to_count)
    elif to_period == 'years':
        period_end = period_start + relativedelta(years=to_count)

    cursor.execute("""
        SELECT Data FROM AggregatedData
        WHERE DeviceID = %s AND PeriodType = %s AND PeriodStart >= %s AND PeriodStart < %s
    """, (device_id, from_period, period_start, period_end))

    raw_data = [json.loads(row['Data']) for row in cursor.fetchall()]
    logging.debug(f"Aggregating data for {to_period}: {raw_data}")  # Add debug logging
    aggregated_data = calculate_aggregates(raw_data)
    logging.debug(f"Aggregated data for {to_period}: {aggregated_data}")  # Add debug logging

    cursor.execute("""
        INSERT INTO AggregatedData (DeviceID, PeriodType, PeriodStart, Data)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE Data = %s
    """, (device_id, to_period.rstrip('s'), period_start, json.dumps(aggregated_data), json.dumps(aggregated_data)))

def calculate_aggregates(data_block):
    aggregates = {}

    for entry in data_block:
        for key, value in entry.items():
            if key in ['mac', 'timestamp']:
                continue

            # Ensure the value is numeric and not a dictionary
            if isinstance(value, dict):
                continue

            if key not in aggregates:
                aggregates[key] = {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0}

            aggregates[key]['min'] = min(aggregates[key]['min'], value)
            aggregates[key]['max'] = max(aggregates[key]['max'], value)
            aggregates[key]['sum'] += value
            aggregates[key]['count'] += 1

    for key in aggregates:
        if aggregates[key]['count'] > 0:
            aggregates[key]['avg'] = aggregates[key]['sum'] / aggregates[key]['count']
        else:
            aggregates[key]['avg'] = None
        del aggregates[key]['sum'], aggregates[key]['count']

    return aggregates

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
