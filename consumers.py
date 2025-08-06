from confluent_kafka import Consumer, KafkaError, KafkaException
from pyspark.sql import SparkSession
import os
from io import StringIO

from schema import schema_dispatcher
from analyze import function_dispatcher
    

def start_consumer(topic):
    print(f"Start consumer {topic}")

    # pt fiecare consumer/topic initializez cate o sesiune spark
    spark = SparkSession.builder \
        .appName(f"KafkaStreamingAnalysis_{topic}") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-hadoop:7.17.10") \
        .getOrCreate()
    
    schema = schema_dispatcher.get(topic)
    df_total = spark.createDataFrame([], schema)

    func = function_dispatcher.get(topic)

    #df_total.printSchema()

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
            
            #print(raw)
            if not raw.strip():
                continue

            lines = raw.strip().split("\n")

            # TODO: sa ma uit cum se constr aici asta
            rdd = spark.sparkContext.parallelize(lines)

            df_batch = spark.read \
                .schema(schema) \
                .option("header", "false") \
                .csv(rdd)

            #df_batch.show()
           
            df_total = df_total.unionByName(df_batch)

            func(df_total)

    except KeyboardInterrupt:
        print(f"[{topic}] Stopping...")
    finally:
        consumer.close()
        #df_total.show(25)
        spark.stop()

