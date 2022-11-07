import time
from config import *
from detector import TextLangs, get_post_text
import requests

BATCH_SIZE = 25
TOTAL_POSTS_FOUND = 0 # Amount of posts which are got no lang detection (setted by get_raw_posts)
WORKER_FIND_QUERY = {
        "size" : str(BATCH_SIZE),
        "query" : {
            "bool" : {
                "must" : [
                    {"exists" : {"field" : "text_title"}},
                    {"exists" : {"field" : "text_body"}},
                
                    {"nested" : {
                        "path" : "jobs",
                        "query" : {
                            "bool" : {
                                "must_not" : [
                                    {"term" : {"jobs.lang_detected" : True}},                          
                                ],                       
                            }
                        }
                    }}
                ]
            }
        },
        "_source" : {
            "includes" : ["text_title", "text_body"]
        }
    }

os_client = get_opensearch_client()


def get_raw_posts() -> list:
    '''Get Posts from OpenSearch'''
    global TOTAL_POSTS_FOUND
    search = os_client.search(index="hive-posts", body=WORKER_FIND_QUERY, timeout="60s")
    TOTAL_POSTS_FOUND = search["hits"]["total"]["value"]
    return search["hits"]["hits"]

def work_on_batch() -> int:
    '''Work on a batch of posts and return the number of processed posts'''
    # Detect langs for each hit and add the result to the bulk-update
    bulk_obj = [] 
    for hit in get_raw_posts():
        text_lang = TextLangs(get_post_text(hit["_source"]))
        bulk_obj.append({"update" : {"_index": hit["_index"], "_id" : hit["_id"]}})
        bulk_obj.append({"doc" : {"language" : text_lang.get_detected_langs(), "jobs" : {"lang_detected" : True}}})
    
    # Send Bulk Request
    if len(bulk_obj) > 0:
        res = os_client.bulk(body=bulk_obj, refresh="wait_for", timeout="60s")
        return len(res["items"])

    return 0

def send_heartbeat(elapsed_time : int = 0) -> None:
    params = {'msg': 'OK', 'ping' : elapsed_time}

    if HEARTBEAT_URL is not None:
        try:
            requests.get(HEARTBEAT_URL, params=params)
        except Exception as e:
            print("Error sending heartbeat: {}".format(e))

def run():
    while True:
        # Work on a batch
        start_time = time.time()
        counter = work_on_batch()
        elapsed_time = time.time() - start_time
        if counter > 0:
            print("Processed {}/{} posts in {:.2f} seconds".format(counter, TOTAL_POSTS_FOUND, elapsed_time))

        send_heartbeat(elapsed_time * 1000)  

        # Sleep only when we have not to much posts to process
        if TOTAL_POSTS_FOUND == counter:
            time.sleep(10)

if __name__ == "__main__":
    run()