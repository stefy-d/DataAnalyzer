import os
import subprocess

input_folder = "input"

for file in os.listdir(input_folder):
    input_path = os.path.join(input_folder, file)
    subprocess.run(["python3", "process_data.py", input_path])