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


def mv(name, path):
    path = path[:path.rfind('/')]
    make_dir(path)
    full_path = ROOT_PATH + path
    shutil.move(name, full_path)


def replicate(file, datanodes):
    nodes = datanodes.split('$')

    for node in nodes:
        temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp_sock.settimeout(10)
        temp_sock.connect((node.split(':')[0],int(node.split(':')[1])))
        temp_sock.settimeout(None)
        comm = f"replicating {file}"
        temp_sock.send(comm.encode())
        time.sleep(1)
        response = temp_sock.recv(BUFFER_SIZE).decode()

        if response == "Ready":
            with open(ROOT_PATH + file, "rb") as f:
                while True:
                    # read the bytes from the file
                    bytes_read = f.read(BUFFER_SIZE)
                    if not bytes_read:
                        # file transmitting is done
                        break

                    temp_sock.send(bytes_read)

        temp_sock.close()


def get_replication(file, sock):
    sock.send("Ready".encode())
    make_dir(file[:file.rfind('/')])
    time.sleep(1)
    time.sleep(1)
    with open(ROOT_PATH + file, "wb") as f:
        while True:
            bytes_read = sock.recv(BUFFER_SIZE)
            if not bytes_read:
                break
            f.write(bytes_read)
        # Send a message of completeness and close connection
    sock.close()

def get(client_socket, filename):
    with open(ROOT_PATH + filename, "rb") as f:
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
        master_socket.send("Command received".encode())
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
        if type == 'mv':
            path = command.split(' ')[2]
            name = command.split(' ')[1]
            mv(name, path)
        if type == 'put':
            path = command.split(' ')[2]
            name = command.split(' ')[1]
            put(client, path, name)
            replicate(path+name, nodes)
        if type == 'replicating':
            file = command.split(' ')[1]
            get_replication(file, master_socket)
        if type == 'get':
            com = command.split(' ')[1]
            get(client, com)

        if type == 'mkdir':
            com = command.split(' ')[1]
            make_dir(client, com)

        if type == 'rm':
            com = command.split(' ')[1]
            rm_dir(com)
        if type == 'rmrf':
            com = command.split(' ')[1]
            rm_dirs(com)

        client.close()