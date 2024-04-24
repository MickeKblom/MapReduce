A bit about the design and structure

Main:
The main script has the endpoints and manages the HTTP requests.
Once a task is requested from the worker , the request is received in the endpoint and the first item in the task queue is given to the worker.
Once a worker reports a completed task it is received in the endpoint and the corresponding task counter in driver is incremented. 
If the task happens to be a completed mapping, a new reduction task is added to the task queue. 
Driver:
The driver manages the workflow. It generates the workers and their tasks. 

Initially, the driver automatically generates the mapping tasks based on the inputs (number of books and lines)

NOTE, the number of worker threads it generates are the same as the number of reduce tasks. The worker threads are initialized to periodically execute a function to that request available tasks from driver(given that a worker is not busy executing a task). 
Once the initial tasks are created, it manages just the delegation of tasks.

Worker:
The worker is executing the tasks. The mapping task is basically just separating the words in the texts. The reduction task is counting the occcurrences of each word (note, here room for improvement in terms of efficiency)

How to run code: 
Please define the project folder where there should be
The necessary python files are driver.py, worker.py and main_script.py 
The folders are inputs, intermediate_files and output_files. The text files to process are placed in “inputs”. “Intermediate files” and “output files” should be empty (fills up with the mapped tasks and reduced tasks, respectively)

To execute, open main_script.py and define the project_folder directory and the number of map and reduce tasks. This is indicated in the bottom of main with comments. 
