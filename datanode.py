import os
import sys
import socket
import shutil
import time

BUFFER_SIZE = 4096
NODE_IP = '0.0.0.0'
NODE_PORT = 8041
SEPARATOR = ' '

CLIENT_PORT = 9999
REP_PORT = 9876
ROOT_PATH = "/home/ayaz/PycharmProjects/dist_fs/data_node/datanode1"


def make_dir(path):
    # Just create all absent directories specified by namenode
    creation_path = ROOT_PATH + path
    # Return if there is already such directory
    if os.path.isdir(creation_path):
        return
    os.makedirs(creation_path)


def rm_dir(path):
    os.remove(ROOT_PATH + path)


def rm_dirs(path):
    os.system('rm -rf ' + ROOT_PATH + path)


def cp(name, path):
    path = path[:path.rfind('/')]
    make_dir(path)
    full_path = ROOT_PATH + path
    shutil.copy2(ROOT_PATH+name, full_path)


# Datanode believes namenode in the correctness of the directory
def put(client_socket, path, filename):
    # If there is no specified by namenode directory datanode create such
    make_dir(path)
    file = ROOT_PATH + path + '/' + filename
    # We write in the binary form
    with open(file, 'wb') as f:
        while True:
            bytes_read = client_socket.recv(BUFFER_SIZE)
            if not bytes_read:
                break
            f.write(bytes_read)
    # Send a message of completeness and close connection
    client_socket.send("File received".encode())
    client_socket.close()

def get_replication(path, sock):
    temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    temp_sock.bind(('0.0.0.0', CLIENT_PORT))
    temp_sock.listen(1)

    sock.send("Port opened".encode())

    datanode, address = temp_sock.accept()
    dirs = path[:path.rfind('/')]
    make_dir(dirs)

    with open(ROOT_PATH+path, "wb") as f:
        while True:
            bytes_read = datanode.recv(BUFFER_SIZE)
            if not bytes_read:
                break
            f.write(bytes_read)

    print("File replicated")
    datanode.close()

def mv(name, path):
    path = path[:path.rfind('/')]
    make_dir(path)
    full_path = ROOT_PATH + path
    shutil.move(name, full_path)

def get(client_socket, path):
    with open(ROOT_PATH + path, "rb") as f:
        while True:
            # read the bytes from the file
            bytes_read = f.read(BUFFER_SIZE)
            if not bytes_read:
                # file transmitting is done
                break

            client_socket.send(bytes_read)
    # Close the connection
    client_socket.close()


if __name__ == "__main__":
    node = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    node.bind((NODE_IP, NODE_PORT))
    node.listen(1)

    while True:
        master_socket, address = node.accept()
        print(f"Got request from master")
        command = master_socket.recv(BUFFER_SIZE).decode()
        print(f"Command from master {command}")

        if command == 'Yep':
            continue
        # We receive from namenode command in format: ('client_ip', client_port)@"command@datanode1$datanode2$"
        args = command.split('@')
        address = ""
        command = ""
        nodes = ""

        if len(args) == 1:
            command = args[0]
        elif len(args) == 2:
            address = args[0]
            command = args[1]
        else:
            address = args[0]
            command = args[1]
            nodes = args[2]

        client_ip = address[2:address.rfind("'")]
        print(client_ip)
        print(command)
        print(nodes)

        # Connect to the client
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        type = command.split(' ')[0]

        if type == 'put' or type == 'get':
            client.settimeout(10)
            client.connect((client_ip, CLIENT_PORT))
            client.settimeout(None)

        # Operate according to the command received
        if type == 'cp':
            path = command.split(' ')[2]
            name = command.split(' ')[1]
            cp(name, path)
            master_socket.send("complete".encode())
        if type == 'mv':
            path = command.split(' ')[2]
            name = command.split(' ')[1]
            mv(name, path)
            master_socket.send("complete".encode())
        if type == 'put':
            path = command.split(' ')[2]
            name = command.split(' ')[1]
            put(client, path, name)
            master_socket.send("complete".encode())

        if type == 'replicating':
            file = command.split(' ')[1]
            get_replication(file, master_socket)
        if type == 'get':
            com = command.split(' ')[1]
            get(client, com)
            master_socket.send("complete".encode())

        if type == 'mkdir':
            com = command.split(' ')[1]
            make_dir(client, com)

        if type == 'rm':
            com = command.split(' ')[1]
            rm_dir(com)
            master_socket.send("complete".encode())
        if type == 'rmrf':
            com = command.split(' ')[1]
            rm_dirs(com)
            master_socket.send("complete".encode())

        client.close()
        master_socket.close()