from flask import Flask, request, jsonify
from firebase_admin import firestore
from google.cloud import firestore
from google.oauth2 import service_account

key_path = "dip.json"

credentials = service_account.Credentials.from_service_account_file(key_path)
db = firestore.Client(credentials=credentials, project=credentials.project_id)

app = Flask(__name__)


@app.route('/post-data', methods=['POST'])
def post_data():
    data = request.json
    collection_id = data.get('mac', 'defaultCollection')
    document_id = data.get('documentId', 'defaultDocument')

    doc_ref = db.collection(collection_id).document(document_id)
    doc_ref.set({
        'timestamp': firestore.FieldValue.server_timestamp(),
        'data': data
    })

    return jsonify({"status": "success", "message": "Data posted to Firestore successfully."}), 200


if __name__ == '__main__':
    app.run(debug=True)
