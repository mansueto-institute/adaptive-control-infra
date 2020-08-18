from flask import Flask, request
from main import *

app = Flask(__name__)

@app.route('/', methods=['GET'])
def main():
    estimate_and_sync()
    return "OK"

if __name__ == '__main__':

    # Used when running locally only. When deploying to Cloud Run,
    # a webserver process such as Gunicorn will serve the app.
    app.run(host='localhost', port=8080, debug=True)
