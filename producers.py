import os
import sys
import time
import threading
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from more_itertools import chunked

bootstrap_servers = 'localhost:9092'


def stream_file(path, topic):
    from confluent_kafka import Producer
    batch_size = 50
    print(f"[{topic}] Starting stream from {path}")
    producer = Producer({'bootstrap.servers': bootstrap_servers})

    with open(path, 'r') as f:
        for chunk in chunked(f, batch_size):
            lines = [line.strip() for line in chunk if line.strip()]
            if lines:
                message = "\n".join(lines)
                producer.produce(topic=topic, value=message.encode('utf-8'))
                producer.poll(0)
                time.sleep(0.1)

    producer.flush()
    print(f"[{topic}] Finished streaming.")


def run_producers(paths, topics):

    # pornesc cate un thread pentru produceri ca sa faca streaming fiecare pentru cate un fisier
    threads = []
    for path, topic in zip(paths, topics):
        t = threading.Thread(target=stream_file, args=(path, topic))
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    print("All streams finished.")
