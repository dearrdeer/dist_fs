import os
import sys
import socket

BUFFER_SIZE = 4096
NODE_IP = '0.0.0.0'
NODE_PORT = 8042
SEPARATOR = ' '

def make_dir(path):
    # Just create all absent directories specified by namenode
    current_path = "/"
    current_path = os.path.join(current_path, path) 
    os.makedirs(current_path)



def put(client_socket, path):
    make_dir(' '.join([str(elem) for elem in path.split('/')[:-1]]))
    file = path
    with open(file, 'rb') as f:
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

    client_socket.send("File received".encode())
    client_socket.close()

if __name__ == "__main__":
    node = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    node.bind((NODE_IP, NODE_PORT))

    node.listen(1)

    while True:
        client_socket, address = node.accept()
        command = client_socket.recv(BUFFER_SIZE).decode()
        type, arguments = command.split(SEPARATOR)
        if (type=="mkdir"):
            make_dir(client_socket, arguments)
        
