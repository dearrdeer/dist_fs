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
ROOT_DIRECTORY = "/run/media/ravioo/disk/Download/DS_NAME"
REPLICATION_FACTOR = 2

datanodes = ["localhost:8041", "localhost:8042", "localhost:8043", "localhost:8044"]
alive_nodes = []

files_map = dict()
files_size = dict()
def process_command(command, client_socket, address):
    args = command.split(' ')
    fs_command = args[0]

    if fs_command == "init":
        files = glob.glob(ROOT_DIRECTORY + '/*')
        space = 0
        for f in files:
            os.remove(f)
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
   
        client_socket.send(f"Initiated. Space free: {space}".encode())
        return
    if fs_command == "usage":
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

        client_socket.send(f"Used: {space}".encode())
        return
    if fs_command == "info":
        path = args[1]
        if not os.path.isfile(ROOT_DIRECTORY+path):
            client_socket.send("File does not exist".encode())
            return
        response = str(files_size.get(path))
        response = response + 'B\n' + " ".join(files_map.get(path))
        client_socket.send(response.encode())
        return

    if fs_command == "mkdir":
        directory_to_create = args[1]
        directory_where_create = directory_to_create[:directory_to_create.rfind('/')]

        if not os.path.isdir(ROOT_DIRECTORY + directory_where_create):
            client_socket.send(f"{directory_where_create} does not exist".encode())
            return

        os.mkdir(ROOT_DIRECTORY + directory_to_create)

        client_socket.send(f"{directory_to_create} created".encode())
        return

    if fs_command == "ls":
        directory_to_list = args[1]

        if not os.path.isdir(ROOT_DIRECTORY + directory_to_list):
            client_socket.send(f"{directory_to_list} does not exist".encode())
            return

        files = os.listdir(ROOT_DIRECTORY + directory_to_list)
        result = ". " + " ".join(f for f in files)
        client_socket.send(result.encode())
        return

    if fs_command == "cp":
        path = args[2]
        file = args[1]
        file_name = file.split('/')[-1]

        if not os.path.isdir(ROOT_DIRECTORY + path):
            client_socket.send(f"{path} does not exist".encode())
            return
        if os.path.isfile(ROOT_DIRECTORY+args[2] + '/' + file_name):
            client_socket.send("File you want to copy to already exists".encode())
            return

        os.mknod(ROOT_DIRECTORY + path + '/' + file_name)
        nodes = files_map.get(file)
        copied = nodes.copy()
        files_map.update({path+ '/' + file_name:copied})
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
        file = args[1]
        dir_where_mv = args[2]
        dir_where_mv = dir_where_mv[:dir_where_mv.rfind('/')]
        file_name = file.split('/')[-1]

        if not os.path.isfile(ROOT_DIRECTORY+file):
            client_socket.send(f"{file} does not exist".encode())
            return

        if not os.path.isdir(ROOT_DIRECTORY + dir_where_mv):
            client_socket.send(f"{dir_where_mv} does not exist".encode())
            return

        if os.path.isfile(ROOT_DIRECTORY+args[2]):
            client_socket.send("File with such name already exists".encode())
            return

        os.mknod(ROOT_DIRECTORY + args[2] + '/' + file_name)
        file = args[1]
        os.remove(ROOT_DIRECTORY + file)

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
        file_name = args[1]
        dir_where_put = args[2]
        if (dir_where_put[-1]!="/"):
            full_name = dir_where_put+'/'+file_name
        else:
             full_name = dir_where_put+file_name

        if not os.path.isdir(ROOT_DIRECTORY + dir_where_put):
            client_socket.send(f"{dir_where_put} does not exist".encode())
            return

        if os.path.isfile(ROOT_DIRECTORY+dir_where_put+'/'+file_name):
            client_socket.send("File already exists".encode())
            return

        if len(alive_nodes) < REPLICATION_FACTOR:
            client_socket.send("Number of alive data nodes is less than replication factor".encode())
            return

        os.mknod(ROOT_DIRECTORY + full_name)
        nodes_to_store = random.sample(alive_nodes, REPLICATION_FACTOR)
        files_map.update({full_name: nodes_to_store})
        temp = nodes_to_store.copy()
        node_to_send = random.choice(temp)
        temp.remove(node_to_send)

        client_socket.send("Starting".encode())

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)

        comm = f"{address}@{command}"
        sock.settimeout(10)
        sock.connect((node_to_send.split(':')[0], int(node_to_send.split(':')[1])))
        sock.settimeout(None)
        sock.send(comm.encode())
        response = sock.recv(BUFFER_SIZE).decode()
        print(response)
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
            print(response)

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
        print(args)
        file_name = args[1]
        print(files_map)

        if not os.path.isfile(ROOT_DIRECTORY+file_name):
            print(ROOT_DIRECTORY+file_name)
            client_socket.send(f"{file_name} does not exist".encode())
            return

        client_socket.send("Starting".encode())
        node = random.choice(files_map[file_name])
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        comm = f"{address}@{command}"
        sock.connect((node.split(':')[0], int(node.split(':')[1])))
        sock.send(comm.encode())
        sock.close()

    if fs_command == "rm":
        deletion_file = args[1]
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

                for node in nodes_file_stored:
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
        deletion_file = args[1]
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
        directory_to_go = args[1]
        print(directory_to_go)
        if not os.path.isdir(ROOT_DIRECTORY + directory_to_go):
            client_socket.send("Directory does not exist".encode())
            return
        client_socket.send("Success".encode())
        return

def ping_datanodes():
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
        print(alive_nodes)
        time.sleep(30)

if __name__ == "__main__":
    master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master.bind((NODE_IP, NODE_PORT))
    master.listen(1)

    Thread(target=ping_datanodes).start()
    print(alive_nodes)
    while True:
        client_socket, address = master.accept()
        bytes_read = client_socket.recv(BUFFER_SIZE)

        if not bytes_read:
            continue

        command = bytes_read.decode()
        print(command)

        process_command(command, client_socket, address)
        client_socket.close()