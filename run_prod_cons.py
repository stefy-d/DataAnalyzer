import os
from multiprocessing import Process
from producers import run_producers
from consumers import start_consumer  

files_folder = "processed_files"
paths = []
topics = []

for file_name in os.listdir(files_folder):
    file_path = os.path.join(files_folder, file_name)
    paths.append(file_path)
    topics.append(file_name)

# pornesc procesul care mi porneste producerii
p_producer = Process(target=run_producers, args=(paths, topics))
p_producer.start()

# pentru consumeri fac cate un proces pentru fiecare 
consumer_processes = []
for topic in topics:
    p = Process(target=start_consumer, args=(topic,))
    p.start()
    consumer_processes.append(p)

p_producer.join()

for p in consumer_processes:
    p.join()

print("Finished all.")
