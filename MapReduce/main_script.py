import http.server
import socketserver
import threading
import json
import time
import os
from driver import Driver

# HTTP request handler
class TaskHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global driver
        if self.path == '/request_task':
            if driver:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                task = driver.request_task()
                if task:
                    self.wfile.write(json.dumps(task).encode())
                else:
                    self.wfile.write(json.dumps({'message': 'Failed to get task'}).encode())
            else:
                self.wfile.write(json.dumps({'message': 'Driver not initialized. Terminating worker...'}).encode())
                # Terminate worker if no driver exists
                self.server.shutdown()  # Shutdown the HTTP server
                return  # Exit the do_GET method
        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b'Not Found')

    def do_POST(self):
        if self.path == '/map':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data)
            global driver
            if driver:
                task = data
                if task:
                    driver.add_to_queue(task)
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(task).encode())
                else:
                    self.send_response(400)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({'message': 'Task not provided'}).encode())
            else:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'message': 'Driver not initialized'}).encode())
        
        elif self.path == '/task_completed':
                    content_length = int(self.headers['Content-Length'])
                    post_data = self.rfile.read(content_length)
                    data = json.loads(post_data)
                    task_type = data
                    print("TASK COMPLETED", task_type) 
                    driver.task_completed(task_type)
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({'message': 'Task completion acknowledged'}).encode())
        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b'Not Found')

def run_http_server():
    # Start the HTTP server
    with socketserver.TCPServer(("localhost", 3000), TaskHandler) as httpd:
        print("HTTP server started")
        httpd.serve_forever()

def main():
    global driver
    num_map_tasks = 5   # CHANGE THIS TO YOUR LIKING
    num_reduce_tasks = 4    # CHANGE THIS TO YOUR LIKING
    project_folder="/home/mikku/Documents/Python/" # CHANGE THIS TO YOUR LIKING
    
    # Get the list of files in the testerfolder directory
    input_files_directory = os.path.join(project_folder, "inputs")
    intermediate_files_directory = os.path.join(project_folder, "intermediate_files")
    output_files_directory = os.path.join(project_folder, "output_files")
    input_files = os.listdir(input_files_directory)
    

    # Count the number of input files
    num_input_files = len(input_files)

    # Create the Driver object with the calculated number of map and reduce tasks
    driver = Driver(input_files_directory, intermediate_files_directory, output_files_directory, num_map_tasks * num_input_files, num_reduce_tasks * num_input_files)

    # Start the HTTP server in a separate thread
    http_server_thread = threading.Thread(target=run_http_server)
    http_server_thread.start()
 
    # Start the driver's tasks
    lines_per_input_data = driver.read_input_data()
    tasks = driver.generate_mapping_tasks(lines_per_input_data, num_map_tasks)
    driver.start(num_reduce_tasks)
    
        # Check if all tasks are completed and shutdown the driver
    while driver and not driver.is_terminated():
        if driver.all_tasks_completed():
            print("ALL TASKS FINISHED, SHUTTING DOWN DRIVER")
            driver.shutdown()
            driver = None
        time.sleep(1)  # Sleep for a while before checking again
    
if __name__ == '__main__':
    main()
