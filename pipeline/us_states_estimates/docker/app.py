from flask import Flask, request
from main import estimate_and_sync
from threading import Thread

app = Flask(__name__)

@app.route('/', methods=['GET'])
def main():
    thread = Thread(target=estimate_and_sync, args=())
    thread.start()
    return "OK"

if __name__ == '__main__':

    # Used when running locally only. When deploying to Cloud Run,
    # a webserver process such as Gunicorn will serve the app.
    app.run(host='localhost', port=8080, debug=True)
