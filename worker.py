import os
import time
import requests
from random import choice, randint
from pymongo import MongoClient, UpdateOne

from detector import TextLangs, get_post_text

# Get MongoDB Client
mongo_client = MongoClient(os.environ.get("MONGO_URI", None))

HEARTBEAT_URL = os.environ.get("HEARTBEAT_URL", None)
BATCH_SIZE = os.environ.get("BATCH_SIZE", 25)

total_posts_found = 0

def send_heartbeat(elapsed_time : int = 0) -> None:
    params = {'msg': 'OK', 'ping' : elapsed_time}

    if HEARTBEAT_URL is not None:
        try:
            requests.get(HEARTBEAT_URL, params=params)
        except Exception as e:
            print("Error sending heartbeat: {}".format(e))

def get_posts_to_process(target : str) -> list:
    '''Get Comments/Replies from MongoDB'''
    cursor = mongo_client.hive[target].find(
        {
            "jobs.lang_detected" : {"$ne" : True},
        }, 
        {"title" : 1, "body" : 1, "_id" : 1}
    )

    global total_posts_found
    if total_posts_found > (BATCH_SIZE * 4):
        cursor.skip(randint(0, total_posts_found - (BATCH_SIZE)))
    cursor.limit(BATCH_SIZE)

    total_posts_found = cursor.count()

    return list(cursor)

def do_work() -> int:
    # Get Posts to work on
    target = choice(["comments", "replies"])
    posts = get_posts_to_process(target)

    # Detect langs for each hit and add the result to the bulk-update
    bulk_updates = []
    for post in posts:
        text_lang = TextLangs(get_post_text(post))
        bulk_updates.append(UpdateOne(
            {"_id" : post["_id"]},
            {"$set" : {"jobs.lang_detected" : True, "language" : text_lang.get_detected_langs()}}
        ))

    # Send Bulk Request
    if len(bulk_updates) > 0:
        res = mongo_client.hive[target].bulk_write(bulk_updates, ordered=False)
        return res.modified_count, target

    return 0, target


def run():
    global total_posts_found
    while True:
        # Work on a batch
        start_time = time.time()
        counter, target = do_work()
        elapsed_time = time.time() - start_time
        
        # Logging
        if counter > 0:
            print("Processed {}/{} {} in {:.2f} seconds".format(counter, total_posts_found, target, elapsed_time))
        send_heartbeat(elapsed_time * 1000)  

        # Sleep only when we have not to much posts to process
        if counter < BATCH_SIZE:
            time.sleep(5)
        


if __name__ == "__main__":
    run()

