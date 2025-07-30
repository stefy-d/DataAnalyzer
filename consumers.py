from confluent_kafka import Consumer, KafkaError, KafkaException
import threading
import sys


config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'stream_data',
    'auto.offset.reset': 'earliest'
}

def start_consumer(topic):
    print(f"Start consumer {topic}")

    consumer = Consumer(config)
    consumer.subscribe([topic])

    # le folosesc ca sa identific cand nu mai primesc date
    idle_count = 0
    max_idle = 10

    try:
        while True:

            # ascult mesajele primite pe topic
            msg = consumer.poll(1.0)
            if msg is None:
                idle_count += 1
                if idle_count >= max_idle:
                    print(f"[{topic}] Exit")
                    break
                continue

            idle_count = 0 

            if msg.error():
                raise KafkaException(msg.error())

            # TODO: pornesc job care sa analizeze datele primite constant
            raw = msg.value().decode('utf-8')
            lines = raw.split('\n')
            

    except KeyboardInterrupt:
        print(f"[{topic}] Stopping...")
    finally:
        consumer.close()



def run_consumers(topics):

    # pornesc cate un thread pentru consumeri care sa primeasca mesajele fiecare pe cate un topic
    threads = []
    for topic in topics:
        t = threading.Thread(target=start_consumer, args=(topic,))
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()
    
    print("All data received.")