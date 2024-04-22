import requests
import time
import os


class Worker():
    def __init__(self, worker_id, input_files_directory, intermediate_files_directory, output_files_directory, num_reduce_tasks, server_address, driver):
        self.worker_id = worker_id
        self.input_files_directory = input_files_directory
        self.intermediate_files_directory = intermediate_files_directory
        self.output_files_directory = output_files_directory
        self.num_reduce_tasks = num_reduce_tasks
        self.server_address = server_address  # Address of the HTTP server
        self.driver = driver 
   
    def execute_task(self,task):
        if task[0]=="map":
            self.execute_mapping_task(task)
        if task[0]=="reduce":
            self.execute_reduce_task(task)

    def execute_mapping_task(self, task):
        task_type, start_index, end_index, filename = task
        print("Executing task: ", task)

        # Fetch data
        input_file_path = os.path.join(self.input_files_directory, filename)
        intermediate_files_directory = self.intermediate_files_directory
        input_file_name = os.path.splitext(os.path.basename(filename))[0] 

        # create a folder for each book within intermediate_files_directory 
        intermediate_folder_path = os.path.join(intermediate_files_directory, input_file_name)
        os.makedirs(intermediate_folder_path, exist_ok=True)

        with open(input_file_path, "r") as file:
            lines = file.readlines()[start_index:end_index]
            for line in lines:
                words = line.split()
                for word in words:
                    # Keep only alphabetical characters
                    cleaned_word = ''.join(filter(str.isalpha, word.lower()))
                    # check if the cleaned word is not empty and calculate the bucket ID
                    if cleaned_word:
                        current_bucket_id = ord(cleaned_word[0]) % self.num_reduce_tasks
                        intermediate_file_name = f"mr-{self.worker_id}-{current_bucket_id}.txt"
                        intermediate_file_path = os.path.join(intermediate_folder_path, intermediate_file_name)
                        with open(intermediate_file_path, "a") as intermediate_file:
                            intermediate_file.write(cleaned_word + "\n")

        book = os.path.splitext(filename)[0]
        reduce_task = ('reduce', book, intermediate_file_name)
        self.notify_task_completion('map')
        try:
            response = requests.post(f'{self.server_address}/map', json=reduce_task)
            if response.status_code == 200:
                return response.json()
            else:
                return {'error': 'Failed to execute task'}
        except requests.RequestException as e:
            print(f"Error executing task: {e}")
            return {'error': 'Failed to execute task'}

    def execute_reduce_task(self, task):
        print("Reducing task:", task)
        task_type, book, filename = task
        final_output_file_name = f"out-{book}.txt"
        final_output_file_path = os.path.join(self.output_files_directory, final_output_file_name)
        
        # Process intermediate files and calculate word frequencies
        word_counts = {}
        intermediate_files_directory = os.path.join(self.intermediate_files_directory, book)
        for intermediate_file in os.listdir(intermediate_files_directory):
            if intermediate_file.startswith("mr-"):  
                intermediate_file_path = os.path.join(intermediate_files_directory, intermediate_file)
                with open(intermediate_file_path, "r") as file:
                    for line in file:
                        word = line.strip()
                        word_counts[word] = word_counts.get(word, 0) + 1
        
        # Write word counts to the final output file
        with open(final_output_file_path, "w") as final_output_file:
            for word, count in word_counts.items():
                final_output_file.write(f"{word} {count}\n")
        self.notify_task_completion('reduce')
        print(f"Reducer Worker {self.worker_id} finished task {task}")

    def request_task_periodically(self):
        while True:
            try:
                response = requests.get(f'{self.server_address}/request_task')
                if response.status_code == 200:
                    task = response.json()
                    if task != {'message': 'Failed to get task'}:
                        self.execute_task(task)
                    else:
                        print("No task available")
                elif response.status_code == 400:
                    print("No driver.")
                    break
                else:
                    print("Failed to get task:", response.status_code)
                
            except requests.RequestException as e:
                print("Error requesting task:", e)
            
            time.sleep(3)  # Adjust the sleep duration as needed

            
    def notify_task_completion(self, task_type):
        try:
            response = requests.post(f'{self.server_address}/task_completed', json={'task_type': task_type})
            if response.status_code != 200:
                print('Failed to notify task completion to driver')
        except requests.RequestException as e:
            print(f"Error notifying task completion: {e}")
