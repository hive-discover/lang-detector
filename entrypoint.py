import time
from threading import Thread

from app import main
from worker import run

if __name__ == '__main__':
    api_process = Thread(target=main, name="API Thread", daemon=True)
    worker_process = Thread(target=run, name="Worker Thread", daemon=True)
    print("Starting both Threads")

    api_process.start()
    worker_process.start()
    print("Both Threads started")

    while True:
        time.sleep(1)

        if not api_process.is_alive():
            print("API Thread died")
            break

        if not worker_process.is_alive():
            print("Worker Thread died")
            break

    print("Exiting main thread")



