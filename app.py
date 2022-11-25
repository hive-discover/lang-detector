import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient

from detector import TextLangs, get_post_text

app = Flask(__name__)
CORS(app)

# Get MongoDB Client
mongo_client = MongoClient(os.environ.get("MONGO_URI", None))

def detect_text(text : str, filter : str = None):
    text_lang = TextLangs(text)
    return {"text" : text_lang.get_detected_text(filter), "langs" : text_lang.get_detected_langs()}

@app.route("/ping")
def ping():
    return jsonify({"status": "ok"}), 200

@app.route("/text", methods=["POST"])
def text_post():
    # Validate request
    if not request.is_json:
        return jsonify({"error": "Invalid request: JSON format required"}), 400
    if "text" not in request.json:
        return jsonify({"error": "Invalid request: 'text' field required"}), 400
    if not isinstance(request.json["text"], str):
        return jsonify({"error": "Invalid request: 'text' field must be a string"}), 400
    
    request.json["filter"] = request.json.get("filter", None)
    
    # Detect Text
    return jsonify(detect_text(request.json["text"], request.json["filter"])), 200

@app.route("/<string:collection>/<author>/<string:permlink>", methods=["GET"])
def post_get_by_authorperm(collection, author, permlink):
    # Get Text from from MongoDB
    post = mongo_client.hive[collection].find_one({"author": author, "permlink": permlink}, {"title": 1, "body": 1})
    if post is None:
        return jsonify({"error": "Post not found"}), 404
    
    # Detect Text
    return jsonify(detect_text(get_post_text(post), request.args.get("filter", None))), 200
    

@app.route("/<string:collection>/<int:id>", methods=["GET"])
def post_get_by_id(collection, id : int):
    # Get Text from from MongoDB
    post = mongo_client.hive[collection].find_one({"_id": id}, {"title": 1, "body": 1})
    if post is None:
        return jsonify({"error": "Post not found"}), 404

    # Detect Text
    return jsonify(detect_text(get_post_text(post), request.args.get("filter", None))), 200


def main():
    app.run(host="0.0.0.0", port=8080)

if __name__ == "__main__":
    main()
