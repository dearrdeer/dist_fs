import socket
import time
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('localhost', 9000))
comm = 'init test_fs'
client.send(comm.encode())
time.sleep(5)
print(client.recv(1024).decode())
client.close()
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('localhost', 9000))
comm = 'test_fs mkdir /user'
client.send(comm.encode())
time.sleep(5)
print(client.recv(1024).decode())
client.close()
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('localhost', 9000))
comm = 'test_fs mkdir /user/ayaz'
client.send(comm.encode())
time.sleep(5)
print(client.recv(1024).decode())
client.close()
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('localhost', 9000))
comm = 'test_fs mkdir /user/ravida'
client.send(comm.encode())
time.sleep(5)
print(client.recv(1024).decode())
client.close()
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('localhost', 9000))
comm = 'test_fs ls /user'
client.send(comm.encode())
time.sleep(5)
print(client.recv(1024).decode())