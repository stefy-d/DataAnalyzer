from confluent_kafka import Consumer, KafkaError, KafkaException
from pyspark.sql import SparkSession
import os
from io import StringIO

def start_consumer(topic):
    print(f"Start consumer {topic}")

    # pt fiecare consumer/topic initializez cate o sesiune spark
    spark = SparkSession.builder \
        .appName(f"KafkaStreamingAnalysis_{topic}") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'stream_data',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])

    idle_count = 0
    max_idle = 10

    try:
        while True:
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

            raw = msg.value().decode('utf-8')
            if not raw.strip():
                continue

           

    except KeyboardInterrupt:
        print(f"[{topic}] Stopping...")
    finally:
        consumer.close()
