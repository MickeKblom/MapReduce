import os
import math
import threading
from worker import Worker

class Driver:
    def __init__(self, input_files_directory, intermediate_files_directory, output_files_directory, num_map_tasks, num_reduce_tasks):
        self.input_files_directory = input_files_directory
        self.intermediate_files_directory = intermediate_files_directory
        self.output_files_directory = output_files_directory
        self.map_tasks_count = num_map_tasks  # Initialize mapper tasks count
        self.reduce_tasks_count = num_reduce_tasks  # Initialize reducer tasks count
        self.task_queue = []
        self.task_lock = threading.Lock()
        self.task_queue_lock = threading.Lock()
        self.task_completion_lock = threading.Lock()
        self.terminate_flag = False  
        print("task_queue created with address:", id(self.task_queue))  # memory address of task_queue for debugging

        
    def read_input_data(self):
        num_lines_per_file = []
        # Read data from each input file in the directory
        for filename in os.listdir(self.input_files_directory):
            file_path = os.path.join(self.input_files_directory, filename)
            with open(file_path, "r") as file:
                num_lines = sum(1 for _ in file)  # Count the number of lines in the file
                num_lines_per_file.append((filename, num_lines)) 
        return num_lines_per_file
     
    def generate_mapping_tasks(self, num_lines_per_file, num_mapper_workers):
        # Divide the data into tasks for mappers
        mapper_tasks = []  # Tuple containing line indexes and filename for each mapper
        for file_name, num_lines in num_lines_per_file:
            if num_lines <= num_mapper_workers:
                # If there are fewer lines than mappers, assign one mapper to handle all lines
                mapper_tasks.append(("map", 0, num_lines, file_name))
            else:
                chunk_size = math.ceil(num_lines / num_mapper_workers)
                start_index = 0
                for i in range(num_mapper_workers):
                    end_index = min(start_index + chunk_size, num_lines)
                    task = ("map", start_index, end_index, file_name)
                    self.add_to_queue(task) 
                    mapper_tasks.append(task)
                    start_index = end_index
        return mapper_tasks

    def add_to_queue(self,task):
        with self.task_lock:
            self.task_queue.append(task)
            print("task ", task, "added to queue with address:", id(self.task_queue))  
            
    def request_task(self):
        with self.task_queue_lock:
            if self.task_queue:
                print("task_queue accessed with address:", id(self.task_queue), " Next task: ", self.task_queue[0])
                return self.task_queue.pop(0)
        # If the task queue is empty or the lock couldnt be acquired, return None
        return None

    def task_completed(self, task):
        with self.task_completion_lock:
            if task['task_type'] == 'map':
                self.map_tasks_count -= 1
            elif task['task_type'] == 'reduce':
                self.reduce_tasks_count -= 1
            print("Map tasks remaining: ", self.map_tasks_count, ", Reduce tasks remaining:", self.reduce_tasks_count)  

    def all_tasks_completed(self):
        with self.task_completion_lock:
            return self.map_tasks_count == 0 and self.reduce_tasks_count == 0

    def shutdown(self):
        with self.task_completion_lock:
            self.terminate_flag = True

    def is_terminated(self):
        with self.task_completion_lock:
            return self.terminate_flag

                
    def start(self, num_reduce_tasks):
        # Start workers
        worker_threads = []
        for i in range(num_reduce_tasks):
            worker = Worker(worker_id=i,
                            input_files_directory=self.input_files_directory,
                            intermediate_files_directory=self.intermediate_files_directory,
                            output_files_directory = self.output_files_directory,
                            num_reduce_tasks=num_reduce_tasks,
                            server_address="http://localhost:3000",
                            driver=self)
            thread = threading.Thread(target=worker.request_task_periodically)
            worker_threads.append(thread)
            thread.start()
            print("Thread started")

    
