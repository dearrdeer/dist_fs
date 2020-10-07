import os
import sys
import socket

BUFFER_SIZE = 4096
NODE_IP = '0.0.0.0'
NODE_PORT = 8042
SEPARATOR = ' '

CLIENT_PORT = 9999
ROOT_PATH = '/run/media/ravioo/disk/Download/DS_P2/dist_fs/data'

def make_dir(path):
    # Just create all absent directories specified by namenode
    creation_path = ROOT_PATH+path
    # Return if there is already such directory 
    if os.path.isdir(creation_path):
        return
    os.makedirs(creation_path)

def rm_dir(path):
    os.remove(ROOT_PATH+path)
def rm_dirs(path):
    os.system('rm -rf ' + ROOT_PATH + path)


# Datanode believes namenode in the correctness of the directory
def put(client_socket, path, filename):

    # If there is no specified by namenode directory datanode create such
    make_dir(path)
    file = ROOT_PATH+path+filename
    # We write in the binary form 
    with open(file, 'wb') as f:
        while True:
            bytes_read = client_socket.recv(BUFFER_SIZE)
            print(bytes_read)
            if not bytes_read:
                break
            f.write(bytes_read)
    # Send a message of completeness and close connection
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
        # We receive from namenode command in format: ('client_ip', client_port)@"command"
        address,command,nodes = command.split('@')
        client_ip = address[2:address.rfind("'")]
        print(client_ip)
        print(command)

        # Connect to the client
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((client_ip, CLIENT_PORT))
        client.settimeout(3)

        # Operate according to the command received
        type = command.split(' ')[0]

        if type == 'put':
            path = command.split(' ')[2]
            name = command.split(' ')[1]

            put(client, path, name)

        if type == 'get':
            com = command.split(' ')[2]
            get(client, command)
            
        if type == 'rm':
            com = command.split(' ')[2]
            rm_dir(com)
        client.close()