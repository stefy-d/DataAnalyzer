import os
from multiprocessing import Process
from producers import run_producers
from consumers import run_consumers

files_folder = "processed_files"
paths = []
topics = []

for file_name in os.listdir(files_folder):
    file_path = os.path.join(files_folder, file_name)
    paths.append(file_path)
    topics.append(file_name)

# TODO: sa vad dc se poate altfel

# pornesc cate un proces pt produceri/consumeri
p1 = Process(target=run_producers, args=(paths, topics))
p2 = Process(target=run_consumers, args=(topics,))

p1.start()
p2.start()

p1.join()
p2.join()

print("Finished all.")
