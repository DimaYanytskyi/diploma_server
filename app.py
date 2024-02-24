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
        print("Received request data:", data)
        collection_id = data.get('mac', 'defaultCollection')
        document_id = data.get('documentId', 'defaultDatetime')

        print(f"Using collection ID: {collection_id} and document ID: {document_id}")

        res = db.collection(collection_id).document(document_id).set({
            'data': data
        })

        print(res)

        return jsonify({"status": "success", "message": "Data posted to Firestore successfully."}), 200
    except Exception as e:
        print("Error occurred:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
