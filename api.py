from flask import Flask, request, jsonify
from flask_cors import CORS
from opensearchpy.exceptions import NotFoundError

from config import *
from detector import TextLangs, get_post_text

app = Flask(__name__)
CORS(app)

os_client = get_opensearch_client()

# Post-Body looks like this:
# {
#     "text" : "..."
# }
@app.route("/", methods=["POST"])
def main_post():
    data = request.get_json()
    if "text" not in data:
        return jsonify({"error": "Missing text"}), 400

    text_lang = TextLangs(data["text"])
    filter = None if "filter" not in data else data["filter"]
    return jsonify({"text" : text_lang.get_detected_text(filter), "langs" : text_lang.get_detected_langs()}), 200

# Query-String has to contain id OR author&permlink
@app.route("/", methods=["GET"])
def main_get():
    # Get query parameters
    id = request.args.get("id", default = None, type = str)
    author = request.args.get("author", default = None, type = str)
    permlink = request.args.get("permlink", default = None, type = str)
    filter = request.args.get("filter", default = None, type = str)

    # validate form
    if id is None and (author is None or permlink is None):
        return "No id or author&permlink given", 400

    # Prepare Search Query for OpenSearch
    query = {
        "bool": {
            "must": []
        }
    }
    if id is not None:
        query["bool"]["must"].append({"ids": {"values": [id]}})
    else:
        query["bool"]["must"].append({"term": {"author": author}})
        query["bool"]["must"].append({"term": {"permlink": permlink}})

    # Search for Post
    post = os_client.search(index="hive-posts", body={
        "query": query,
        "_source": {
            "includes": ["text_title", "text_body"]
        }
    })

    # Check if Post was found
    post = post["hits"]["hits"]
    if len(post) == 0:
        return "No post found", 404
    post = post[0]["_source"]

    # We got the Post
    text_lang = TextLangs(get_post_text(post))
    return jsonify({"text" : text_lang.get_detected_text(filter), "langs" : text_lang.get_detected_langs()}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

