import socket
import time

REPLICATION_FACTOR = 1

BUFFER_SIZE = 4096

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('localhost', 9000))

comm = "put testfile.txt /"

client.send(comm.encode())
print(client.recv(1024).decode())
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(('0.0.0.0',9999))
sock.listen(REPLICATION_FACTOR)

datanode, address = sock.accept()

with open('/run/media/ravioo/disk/Download/DS_P2/dist_fs/data/alice.txt', "rb") as f:
	while True:
		bytes_read = f.read(BUFFER_SIZE)
		if not bytes_read:
			# file transmitting is done
			break
			sock.send(bytes_read)