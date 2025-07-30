import os
import sys
import time
import threading
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient


bootstrap_servers = 'localhost:9092'


def stream_file(path, topic):
    batch_size=50
    print(f"[{topic}] Starting stream from {path}")
    producer = Producer({'bootstrap.servers': bootstrap_servers})

    # TODO: sa vad daca pot sa fac altfel batch urile
    # TODO: batchuri mai mici ca sa nu se termine streamingul asa repede
    batch = []
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                batch.append(line)

            # trimit pe topicul corespunzator
            if len(batch) >= batch_size:
                message = "\n".join(batch)
                producer.produce(topic=topic, value=message.encode('utf-8'))
                producer.poll(0)
                batch = []
                time.sleep(0.1) # delay pt a simula flux de date

    if batch:
        message = "\n".join(batch)
        producer.produce(topic=topic, value=message.encode('utf-8'))
        producer.poll(0)

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
