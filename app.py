import datetime
import json
import os
from flask import Flask, request, jsonify
from flask_mysqldb import MySQL
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)

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

        update_aggregated_data(client_timestamp.date(), device_id)

        return jsonify({"status": "success", "message": "Data posted to MySQL successfully."}), 200

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


def update_aggregated_data(date, device_id):
    get_hourly_averages(date, device_id)


def get_hourly_averages(date_requested, device_id):
    cursor = mysql.connection.cursor()
    query = """
    SELECT 
        HOUR(Timestamp) as Hour, 
        AVG(Data) as AverageData 
    FROM 
        RawData 
    WHERE 
        DeviceID = %s AND 
        DATE(Timestamp) = %s 
    GROUP BY 
        HOUR(Timestamp);
    """

    cursor.execute(query, (device_id, date_requested))
    results = cursor.fetchall()

    for result in results:
        hour = result['Hour']
        average_data = result['AverageData']
        upsert_query = """
        INSERT INTO AggregatedData (DeviceID, Date, Hour, AverageData) 
        VALUES (%s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE 
        AverageData = VALUES(AverageData);
        """
        cursor.execute(upsert_query, (device_id, date_requested, hour, average_data))

    mysql.connection.commit()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
