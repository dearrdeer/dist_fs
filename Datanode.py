import socket
import time

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('localhost', 8042))
command = "put run/media/ravioo/disk/Download/DS_P2/dist_fs/alice/mf "
client.send(command.encode())
time.sleep(5)
print(client.recv(1024).decode())
client.close()