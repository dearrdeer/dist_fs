import socket
import time

REPLICATION_FACTOR = 1

BUFFER_SIZE = 4096

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('localhost', 9000))

comm = "put testfile.txt /test_fs/user"

client.send(comm.encode())

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(('0.0.0.0',9999))
sock.listen(REPLICATION_FACTOR)

datanode, address = sock.accept()

with open('testfile.txt', "rb") as f:
	while True:
		bytes_read = f.read(BUFFER_SIZE)
		if not bytes_read:
			# file transmitting is done
			break
			client_socket.send(bytes_read)
	
