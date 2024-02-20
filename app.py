import datetime

from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, firestore
import os
import json

service_account_info = json.loads(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))
cred = credentials.Certificate(service_account_info)
firebase_admin.initialize_app(cred)

app = Flask(__name__)

db = firestore.client()


@app.route('/post-data', methods=['POST'])
def post_data():
    try:
        data = request.json
        collection_id = data.get('mac', 'defaultCollection')
        document_id = data.get('documentId', 'defaultDocument')

        doc_ref = db.collection(collection_id).document(document_id)

        doc_ref.set({
            'timestamp': datetime.time(),
            'data': data
        })

        return jsonify({"status": "success", "message": "Data posted to Firestore successfully."}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
