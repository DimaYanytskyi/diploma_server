from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials
from google.cloud import firestore
from google.cloud.firestore import SERVER_TIMESTAMP
import os
import json

service_account_info = json.loads(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))

cred = credentials.Certificate(service_account_info)
firebase_admin.initialize_app(cred)

db = firestore.Client()

app = Flask(__name__)


@app.route('/post-data', methods=['POST'])
def post_data():
    data = request.json
    collection_id = data.get('mac', 'defaultCollection')
    document_id = data.get('documentId', 'defaultDocument')

    doc_ref = db.collection(collection_id).document(document_id)
    doc_ref.set({
        'timestamp': SERVER_TIMESTAMP,
        'data': data
    })

    return jsonify({"status": "success", "message": "Data posted to Firestore successfully."}), 200


if __name__ == '__main__':
    app.run(debug=True)
