import socket
import os
import shutil
import random
import time
import glob
from threading import Thread

BUFFER_SIZE = 4096
NODE_IP = '0.0.0.0'
NODE_PORT = 9000
SEPARATOR = ' '
ROOT_DIRECTORY = "/home/vagrant/dfs"
REPLICATION_FACTOR = 3

datanodes = []
# Array to store which nodes is alive
alive_nodes = []

# Dicrionary that represent filestructure of the DFS in the way (path_to_file):[nodes_where_it_is_stored]
files_map = dict()
# To store filesizes stored in the DFS
files_size = dict()

# Function To process commands from the clien 
def process_command(command, client_socket, address):
    args = command.split(' ')
    fs_command = args[0]

    if fs_command == "init":
        # Remove everything in the root directory of the name node which is identical to DFS structure
        shutil.rmtree(ROOT_DIRECTORY, ignore_errors=True)
        space = 0
        os.mkdir(ROOT_DIRECTORY)
        # Pass command to all nodes to perform init
        for node in alive_nodes:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((node.split(':')[0], int(node.split(':')[1])))
            sock.settimeout(None)
            sock.send(fs_command.encode())
            response = ""
            while response == "":
                response = sock.recv(BUFFER_SIZE).decode()
            # Response also how many free space available
            space += int(response)
            sock.close()
        client_socket.send(f"Initiated. Space free: {space/REPLICATION_FACTOR}".encode())
        return


    if fs_command == "usage":
        # Return how many space was used 
        space = 0
        for node in alive_nodes:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((node.split(':')[0], int(node.split(':')[1])))
            sock.settimeout(None)
            sock.send(fs_command.encode())
            response = ""
            while response == "":
                response = sock.recv(BUFFER_SIZE).decode()
            space += int(response)
            sock.close()
        client_socket.send(f"Used: {space/REPLICATION_FACTOR}".encode())
        return

    if fs_command == "info":
        path = args[1]
        # Check the existence of such file if doesn't exist return error message
        if not os.path.isfile(ROOT_DIRECTORY+path):
            client_socket.send("File does not exist".encode())
            return
        # Return to the client a sufficient info about file
        response = str(files_size.get(path))
        response = response + 'B\n' + " ".join(files_map.get(path))
        client_socket.send(response.encode())
        return

    if fs_command == "mkdir":
        # Make directory in the namenode memory 
        directory_to_create = args[1]
        directory_where_create = directory_to_create[:directory_to_create.rfind('/')]
        # Check the base directory on existence
        if not os.path.isdir(ROOT_DIRECTORY + directory_where_create):
            client_socket.send(f"{directory_where_create} does not exist".encode())
            return
        os.mkdir(ROOT_DIRECTORY + directory_to_create)
        client_socket.send(f"{directory_to_create} created".encode())
        return

    if fs_command == "ls":
        # List all files in current directory
        directory_to_list = args[1]
        # Check existence of requested directory
        if not os.path.isdir(ROOT_DIRECTORY + directory_to_list):
            client_socket.send(f"{directory_to_list} does not exist".encode())
            return
        files = os.listdir(ROOT_DIRECTORY + directory_to_list)
        result = ". " + " ".join(f for f in files)
        client_socket.send(result.encode())
        return

    if fs_command == "cp":
        # Copies the specified file 
        # cp what_to_copy where_to_copy(including new_filename)
        path = args[2]
        file = args[1]
        file_name = file.split('/')[-1]
        # Check if file we want to copy exists
        if not os.path.isfile(ROOT_DIRECTORY+file):
            client_socket.send(f"{file} does not exist".encode())
            return
        # Check the directory where wanted to copy on existence 
        if not os.path.isdir(ROOT_DIRECTORY + path):
            client_socket.send(f"{path} does not exist".encode())
            return
        # Check file wanted to copy in doesn't already exists
        if os.path.isfile(ROOT_DIRECTORY+args[2] + '/' + file_name):
            client_socket.send("File you want to copy to already exists".encode())
            return
        # Create new directory in the namenode
        os.mknod(ROOT_DIRECTORY + path + '/' + file_name)
        # Take all nodes where copied file exist
        nodes = files_map.get(file)
        copied = nodes.copy()
        # Update the filename and new location 
        files_map.update({path+ '/' + file_name:copied})
        files_size.update({path+ '/' + file_name:files_size.get(path+ '/' + file_name)})
        # Command to all todes
        comm = f"{command}"
        client_socket.send("Starting".encode())
        for node in nodes:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((node.split(':')[0], int(node.split(':')[1])))
            sock.settimeout(None)
            sock.send(comm.encode())
            sock.close()
        client_socket.send("File copied".encode())
        return

    if fs_command == "mv":
        # Move file
        file = args[1]
        dir_where_mv = args[2]
        dir_where_mv = dir_where_mv[:dir_where_mv.rfind('/')]
        file_name = file.split('/')[-1]
        # All needed checks on existence
        if not os.path.isfile(ROOT_DIRECTORY+file):
            client_socket.send(f"{file} does not exist".encode())
            return
        if not os.path.isdir(ROOT_DIRECTORY + dir_where_mv):
            client_socket.send(f"{dir_where_mv} does not exist".encode())
            return
        if os.path.isfile(ROOT_DIRECTORY+args[2]):
            client_socket.send("File with such name already exists".encode())
            return
        # Make new directory to move file
        os.mknod(ROOT_DIRECTORY + args[2] + '/' + file_name)
        file = args[1]
        # Remove old directory
        os.remove(ROOT_DIRECTORY + file)
        # Prompt command to all nodes containing the file to move 
        nodes = files_map.get(file)
        copied = nodes.copy()
        files_map.update({args[2]: copied})
        comm = f"{command}"
        client_socket.send("Starting".encode())
        for node in nodes:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((node.split(':')[0], int(node.split(':')[1])))
            sock.settimeout(None)
            sock.send(comm.encode())
            sock.close()
        client_socket.send("File moved".encode())
        return

    if fs_command == "put":
        # Upload file from the client 
        file_name = args[1]
        dir_where_put = args[2]
        if (dir_where_put[-1]!="/"):
            full_name = dir_where_put+'/'+file_name
        else:
             full_name = dir_where_put+file_name
        # All needed checks
        if not os.path.isdir(ROOT_DIRECTORY + dir_where_put):
            client_socket.send(f"{dir_where_put} does not exist".encode())
            return
        if os.path.isfile(ROOT_DIRECTORY+dir_where_put+'/'+file_name):
            client_socket.send("File already exists".encode())
            return
        if len(alive_nodes) < REPLICATION_FACTOR:
            client_socket.send("Number of alive data nodes is less than replication factor".encode())
            return
        # Create new directory on the namenode and chose the nodes to replicate in 
        os.mknod(ROOT_DIRECTORY + full_name)
        nodes_to_store = random.sample(alive_nodes, REPLICATION_FACTOR)
        files_map.update({full_name: nodes_to_store})
        temp = nodes_to_store.copy()
        # Send all nodes to replicate in to the randomly chosed one for future replication 
        node_to_send = random.choice(temp)
        temp.remove(node_to_send)
        client_socket.send("Starting".encode())
        # Connect the datanode and send command
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        comm = f"{address}@{command}"
        sock.settimeout(10)
        sock.connect((node_to_send.split(':')[0], int(node_to_send.split(':')[1])))
        sock.settimeout(None)
        sock.send(comm.encode())
        response = sock.recv(BUFFER_SIZE).decode()
        # Get the filesize from datanode stored file
        files_size.update({full_name:int(response.split(' ')[1])})
        for i in temp:
            print("Sending message to datanode to open port")
            comm = f"@replicating {full_name}"
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            temp_sock.settimeout(10)
            temp_sock.connect((i.split(':')[0], int(i.split(':')[1])))
            temp_sock.settimeout(None)
            temp_sock.send(comm.encode())
            response = ""
            while response == "":
                response = temp_sock.recv(BUFFER_SIZE).decode()
            temp_sock.close()

            print("Sending message to datanode to replicate the file")
            comm =f"{(i.split(':')[0], int(i.split(':')[1]))}@get {full_name}"
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            temp_sock.settimeout(10)
            temp_sock.connect((node_to_send.split(':')[0], int(node_to_send.split(':')[1])))
            temp_sock.settimeout(None)
            temp_sock.send(comm.encode())
            response = ""
            while response == "":
                response = temp_sock.recv(BUFFER_SIZE).decode()
        sock.close()
        client_socket.send("File uploaded".encode())
        return

    if fs_command == "get":
        # Return to the client requested file 
        file_name = args[1]
        # Check if exist
        if not os.path.isfile(ROOT_DIRECTORY+file_name):
            print(ROOT_DIRECTORY+file_name)
            client_socket.send(f"{file_name} does not exist".encode())
            return
        
        nodes = [ip for ip in files_map[file_name] if ip in alive_nodes]
        
        if len(nodes) == 0:
            client_socket.send(f"No alive nodes contain file".encode())
            return

        client_socket.send("Starting".encode())
        node = random.choice(nodes)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        comm = f"{address}@{command}"
        sock.connect((node.split(':')[0], int(node.split(':')[1])))
        sock.send(comm.encode())
        sock.close()
        return

    if fs_command == "rm":
        # Remove file or empty directory
        deletion_file = args[1]
        # Checks
        if os.path.isdir(ROOT_DIRECTORY + deletion_file):
            if len(os.listdir(ROOT_DIRECTORY + deletion_file)) == 0:
                os.rmdir(ROOT_DIRECTORY + deletion_file)
                client_socket.send("Directory removed".encode())
                return
            else:
                client_socket.send("Directory is not empty use rmrf instead".encode())
                return
        else:
            if os.path.isfile(ROOT_DIRECTORY + deletion_file):
                nodes_file_stored = files_map.get(deletion_file)
                # Remove in all nodes stores it
                for node in nodes_file_stored:
                    if not node in alive_nodes:
                        continue
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(10)
                    comm = f"{address}@{command}"
                    sock.connect((node.split(':')[0], int(node.split(':')[1])))
                    sock.send(comm.encode())
                    sock.close()
                os.remove(ROOT_DIRECTORY + deletion_file)
                client_socket.send("File removed".encode())
                return
            else:
                client_socket.send(f"{deletion_file} does not exist".encode())
                return

    if fs_command == "rmrf":
        # Remove directory with files
        deletion_file = args[1]
        # Promote to all datanodes
        if os.path.isdir(ROOT_DIRECTORY + deletion_file):
            if len(os.listdir(ROOT_DIRECTORY + deletion_file)) == 0:
                os.rmdir(ROOT_DIRECTORY + deletion_file)
                client_socket.send("Directory removed".encode())
                return
            else:
                for node in alive_nodes:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(10)
                    comm = f"{address}@{command}"
                    sock.connect((node.split(':')[0], int(node.split(':')[1])))
                    sock.send(comm.encode())
                    sock.close()
                shutil.rmtree(ROOT_DIRECTORY + deletion_file, ignore_errors=True)
                client_socket.send("Files removed".encode())
                return
        else:
            if os.path.isfile(ROOT_DIRECTORY + deletion_file):
                client_socket.send("It is usual file, use rm instead".encode())
                return
            else:
                client_socket.send(f"{deletion_file} does not exist".encode())
                return

    if fs_command == "cd":
        # Allow client to change directory
        directory_to_go = args[1]
        if not os.path.isdir(ROOT_DIRECTORY + directory_to_go):
            client_socket.send("Directory does not exist".encode())
            return
        client_socket.send("Success".encode())
        return

def ping_datanodes():
    # Check is the node alive
    global alive_nodes
    global files_map
    while True:
        for node in datanodes:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            comm = "Yep"
            result = sock.connect_ex((node.split(':')[0], int(node.split(':')[1])))
            sock.settimeout(None)
            if result == 0:
                sock.send(comm.encode())
                if not node in alive_nodes:
                    alive_nodes.append(node)
                print(f"{node} is connected")
            else:
                print(f"{node} is dead\n")
            sock.close()
        
        time.sleep(30)

if __name__ == "__main__":
    #Potential datanodes
    for i in range(11,21):
        datanodes.append(f'10.0.0.{i}:8042')
    # Listen for client connects
    master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master.bind((NODE_IP, NODE_PORT))
    master.listen(1)
    # Pereodically chech the datanodes to detect failed ones
    Thread(target=ping_datanodes).start()
    # Process comands that came from client and send it further to the datanodes if needed
    while True:
        # Accept command
        client_socket, address = master.accept()
        bytes_read = client_socket.recv(BUFFER_SIZE)
        if not bytes_read:
            continue
        command = bytes_read.decode()
        # Print to ensure command received correctly 
        print(command)
        process_command(command, client_socket, address)
        client_socket.close()
