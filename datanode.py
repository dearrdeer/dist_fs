import os
import sys
import socket

BUFFER_SIZE = 4096
NODE_IP = '0.0.0.0'
NODE_PORT = 8042
SEPARATOR = ' '

CLIENT_PORT = 9999
ROOT_PATH = '/home/ayaz/PycharmProjects/dist_fs/data_node/data'

def make_dir(path):
    # Just create all absent directories specified by namenode
    creation_path = ROOT_PATH+path
    if os.path.isdir(creation_path):
        return
    os.makedirs(creation_path)



def put(client_socket, path):
    make_dir(path[:path.rfind('/')])
    file = ROOT_PATH+path
    with open(file, 'wb') as f:
        while True:
            bytes_read = client_socket.recv(BUFFER_SIZE)
            if not bytes_read:
                break
            f.write(bytes_read)

    client_socket.send("File received".encode())
    client_socket.close()

def get(client_socket, filename):
    with open(filename, "rb") as f:
        while True:
            # read the bytes from the file
            bytes_read = f.read(BUFFER_SIZE)
            if not bytes_read:
                # file transmitting is done
                break
            # we use sendall to assure transimission in
            # busy networks
            client_socket.send(bytes_read)

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

        address,command = command.split('@')

        client_ip = address[2:address.rfind("'")]
        print(client_ip)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((client_ip, CLIENT_PORT))
        sock.settimeout(None)

        type = command.split(' ')[0]

        if type == 'put':
            com = command.split(' ')[2]
            put(sock, com)

        if type == 'get':
            get(sock, command)

        sock.close()