import os
import sys
import socket
import shutil

BUFFER_SIZE = 4096
NODE_IP = '0.0.0.0'
NODE_PORT = 8043
SEPARATOR = ' '

CLIENT_PORT = 9999
ROOT_PATH = "/run/media/ravioo/disk/Download/DS_P2/dist_fs/data"


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

def cp(name,path):
    path = path[:path.rfind('/')]
    make_dir(path)
    full_path=ROOT_PATH+path
    shutil.copy2(name,full_path)

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

def mv(name,path):
    path = path[:path.rfind('/')]
    make_dir(path)
    full_path=ROOT_PATH+path
    shutil.move(name,full_path)
def replicate(command,nodes):
    node = nodes.split('$')
    for n in node:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((n.split(':')[0], int(n.split(':')[1])))
        if ((command.split(" "))[0]== 'put'):           
            command = f"{('0.0.0.0',9999)}@{command}"
        sock.send(command.encode())
        if ((command.split(" "))[0]== 'put'):
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			temp_sock.bind(('0.0.0.0',9999))
			temp_sock.listen(1)
            

			datanode, address = temp_sock.accept()
			with open(filename, "rb") as f:
				while True:
					bytes_read = f.read(BUFFER_SIZE)
					if not bytes_read:
						#file transmitting is done
						break
					datanode.send(bytes_read)

			print("File uploaded")
			datanode.close()
        sock.send(command.encode())
        sock.close()
        
    
def get(client_socket, filename):
    with open(ROOT_PATH+filename, "rb") as f:
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
        args = command.split('@')
        address = ""
        command = ""
        nodes = ""

        if len(args) == 1:
            command = args[0]
        elif len(args) == 2:
            command = args[0]
            nodes = args[1]
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

        if command.split(' ')[0] == 'put' or command.split(' ')[0] == 'get':
            client.settimeout(10)
            client.connect((client_ip, CLIENT_PORT))
            client.settimeout(None)

        # Operate according to the command received
        type = command.split(' ')[0]
        if type == 'cp':
            path = command.split(' ')[2]
            name = command.split(' ')[1]
            cp(name,path)
        if type == 'mv':
            path = command.split(' ')[2]
            name = command.split(' ')[1]
            mv(name,path)
        if type == 'put':
            path = command.split(' ')[2]
            name = command.split(' ')[1]
            put(client, path, name)

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
        if (nodes):
            replicate(command,nodes)
                