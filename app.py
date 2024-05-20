import os
import ydb
import requests
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
load_dotenv()

VERSION = "1.1"

DB_ENDPOINT = os.getenv("DB_ENDPOINT", "grpc://ydb.serverless.yandexcloud.net:2135")
DB_PATH = os.getenv("DB_PATH", "/ru-central1/b1gXXXXXXXXXX/etnXXXXXXXXXX")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "/path/to/credentials.json")


driver_config = ydb.DriverConfig(
    DB_ENDPOINT,
    DB_PATH,
    credentials=ydb.iam.ServiceAccountCredentials.from_file(CREDENTIALS_FILE,
                                                            iam_endpoint=None, iam_channel_credentials=None)
)
driver = ydb.Driver(driver_config)
driver.wait(fail_fast=True, timeout=5)


def get_instance_name():
    try:
        response = requests.get('http://169.254.169.254/latest/meta-data/instance-id')
        if response.status_code == 200:
            return response.text
    except Exception as e:
        print(f"Error getting instance name: {e}")
    return "Unknown"


def get_next_id():
    query = """
    SELECT id FROM counters WHERE name = "guestbook_cnt";
    UPDATE counters SET id = id + 1 WHERE name = "guestbook_cnt";
    """
    result = execute_query(query)
    return result[0].rows[0]['id']


def execute_query(query, params=None):
    session = driver.table_client.session().create()
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
        query, params, commit_tx=True
    )
    return result_sets


@app.route('/')
def index():
    return "This is a guestbook server. Please, use my API."


@app.route('/health', methods=['GET'])
def health():
    return jsonify(status="ok"), 200


@app.route('/api/entries', methods=['GET'])
def get_entries():
    query = "SELECT id, name, message FROM guestbook"
    result_set = execute_query(query)
    entries = [{"id": row["id"], "name": row["name"], "message": row["message"]} for row in result_set[0].rows]
    return jsonify(entries)


@app.route('/api/sign', methods=['POST'])
def post_entry():
    try:
        data = request.get_json()
        entry_id = get_next_id()
        name = data['name']
        message = data['message']
        query = f'''INSERT INTO guestbook (id, name, message) VALUES ({entry_id}, "{name}", "{message}");'''
        execute_query(query)
        return jsonify({'status': 'success', 'message': 'Entry added successfully'}), 201
    except KeyError as e:
        return jsonify({'status': 'error', 'message': f'Missing parameter: {e}'}), 400
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/instance', methods=['GET'])
def instance_name():
    instance_name = get_instance_name()
    return jsonify({'instance_name': instance_name})
    
    
@app.route('/api/version', methods=['GET'])
def version():
    return jsonify({'version': VERSION})


if __name__ == '__main__':
    app.run(host="0.0.0.0")
