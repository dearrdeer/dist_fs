import socket
import os
import shutil
import random

BUFFER_SIZE = 4096
NODE_IP = '0.0.0.0'
NODE_PORT = 9000
SEPARATOR = ' '
ROOT_DIRECTORY = "/home/ayaz/PycharmProjects/dist_fs/dfs"
REPLICATION_FACTOR = 3

datanodes = ["localhost:8041", "localhost:8042", "localhost:8043", "localhost:8044"]
alive_nodes = []

files_map = dict()

def process_command(command, client_socket, address):
    args = command.split(' ')
    fs_command = args[0]

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
        result = " ".join(f for f in files)
        client_socket.send(result.encode())
        return

    if fs_command == "cp":
        path = args[2]
        path = path[:path.rfind('/')]
        print(path)
        file_to_store=(args[2].split('/'))[-1]
        print(file_to_store)

        if not os.path.isdir(ROOT_DIRECTORY + path):
            client_socket.send(f"{path} does not exist".encode())
            return
        if os.path.isfile(ROOT_DIRECTORY+args[2]):
            client_socket.send("File you want to copy to already exists".encode())
            return

        file = args[1]
        file_name = file.split('/')[-1]
        os.mknod(ROOT_DIRECTORY + path + '/' + file_name)
        nodes = files_map.get(file)
        copied = nodes.copy()
        files_map.update({path:copied})
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

        if not os.path.isfile(ROOT_DIRECTORY+file):
            client_socket.send(f"{file} does not exist".encode())
            return

        if not os.path.isdir(ROOT_DIRECTORY + dir_where_mv):
            client_socket.send(f"{dir_where_mv} does not exist".encode())
            return

        if os.path.isfile(ROOT_DIRECTORY+args[2]):
            client_socket.send("File with such name already exists".encode())
            return

        os.mknod(ROOT_DIRECTORY + args[2])
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

        os.mknod(ROOT_DIRECTORY + dir_where_put + '/' + file_name)
        nodes_to_store = random.sample(alive_nodes, REPLICATION_FACTOR)
        files_map.update({full_name: nodes_to_store})
        temp = nodes_to_store.copy()
        node_to_send = random.choice(temp)
        temp.remove(node_to_send)

        client_socket.send("Starting".encode())

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        nodes = "$".join(temp)

        comm = f"{address}@{command}@{nodes}"
        sock.connect((node_to_send.split(':')[0], int(node_to_send.split(':')[1])))
        sock.send(comm.encode())
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
    alive_nodes.clear()
    for node in datanodes:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        comm = "Yep"
        result = sock.connect_ex((node.split(':')[0], int(node.split(':')[1])))
        sock.settimeout(None)
        if result == 0:
            sock.send(comm.encode())
            response = sock.recv(BUFFER_SIZE)
            print(response.decode())
            alive_nodes.append(node)
        else:
            print(f"{node} is dead\n")
        sock.close()

if __name__ == "__main__":
    master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master.bind((NODE_IP, NODE_PORT))
    master.listen(1)

    ping_datanodes()

    while True:
        client_socket, address = master.accept()
        bytes_read = client_socket.recv(BUFFER_SIZE)

        if not bytes_read:
            continue

        command = bytes_read.decode()
        print(command)

        process_command(command, client_socket, address)
        client_socket.close()