import socket
from name_node.FileSystem import *


BUFFER_SIZE = 4096
NODE_IP = '0.0.0.0'
NODE_PORT = 9000
SEPARATOR = ' '

class DataNode:
    def __init__(self, ip, port):
        self.port = port
        self.ip = ip

def process_command(args, client_socket):
    fs_command = args[1]

    if len(filesystems) == 0:
        client_socket.send("No such file system".encode())
        return

    fs = filesystems[0]
    for f in filesystems:
        if f.name == fs_name:
            fs = f
            break

    if not fs.name == fs_name:
        client_socket.send("No such file system".encode())
        return

    if fs_command == "mkdir":
        if not len(args) == 3:
            client_socket.send("Wrong Usage of command, please use mkdir /path/to/new/directory".encode())
            return

        path = args[2]
        result = fs.mkdir(path.split('/')[-1], path[:path.rfind('/')])
        client_socket.send(result[0].encode())

    if fs_command == "ls":
        path = ""
        if len(args) == 3:
            path = args[2]
        elif len(args) > 3:
            client_socket.send("Wrong Usage of command, please use ls /path/to/directory".encode())
            return

        temp = fs.ls(path)
        result = ""
        for t in temp:
            result += t + " "
        client_socket.send(result.encode())

    if fs_command == "put":
        if not len(args) == 4:
            client_socket.send("Wrong Usage of command, please use put /path/to/file /path/to/directoryofDFS".encode())
            return

        filename = args[2]
        path = args[3]
        creation_directory = fs.get_directory_by_path(path)
        new_file = File(filename, path+'/'+filename, )


if __name__ == "__main__":
    datanodes = []
    filesystems = []

    master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master.bind((NODE_IP, NODE_PORT))
    master.listen(1)

    while True:
        client_socket, address = master.accept()
        command = client_socket.recv(BUFFER_SIZE).decode()

        if command == "":
            continue

        args = command.split(" ")
        fs_name = args[0]

        if fs_name == "init":
            new_fs = FileSystem(args[1])
            filesystems.append(new_fs)
            client_socket.send("File System Created".encode())
        else:
            process_command(args, client_socket)

