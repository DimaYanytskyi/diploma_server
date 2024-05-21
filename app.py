import datetime
import json
import os
from datetime import timedelta
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

        # Insert individual parameters into the new RawData table
        cursor.execute(
            "INSERT INTO RawData (DeviceID, Timestamp, TVOC, Light, BmpTemp, Humidity, Moisture, BmpAltitude, "
            "BmpPressure, Temperature) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (device_id, client_timestamp, data.get('tvoc'), data.get('light'), data.get('bmpTemp'), data.get('humidity'), data.get('moisture'), data.get('bmpAltitude'), data.get('bmpPressure'), data.get('temperature'))
        )
        mysql.connection.commit()

        return jsonify({"status": "success", "message": "Data posted to MySQL successfully."}), 200

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
