import os, socket
import sys

BUFFER_SIZE = 4096
MASTER_IP = 'localhost'
MASTER_PORT = 9000
my_directory = "/"


def send_comm(sock):
	comm = ""
	filename = ""
	path_to_file = ""
	path_to_go = ""
	#check 1
	if len(sys.argv) == 1:
		print("No command provided")
		return

	type = sys.argv[1]

	if type == "ls":
		if len(sys.argv) == 2 or len(sys.argv) == 3:
			directory_to_list = my_directory if len(sys.argv) == 2 else sys.argv[2]
			if directory_to_list[0] != '/':
				directory_to_list = my_directory + directory_to_list

			comm = "ls " + directory_to_list
		else:
			print("Wrong usage of command. Use ls path/to/dir")
			return

	if type == "mkdir":
		if len(sys.argv) == 3:
			path_to_create = sys.argv[2]
			if path_to_create[0] != '/':
				path_to_create = my_directory + path_to_create
			comm = "mkdir " + path_to_create
		else: 
			print("Wrong usage of command. Use mkdir path/to/dir/to/create")
			return

	if type == "put":
		if len(sys.argv) == 4:
			filename = sys.argv[2]
			path_in_dfs = sys.argv[3]

			if path_in_dfs[0] != '/':
				path_in_dfs = my_directory + path_in_dfs

			if not os.path.isfile(filename):
				print(f"File {filename} not found")
				return

			comm = "put " + filename + " " + path_in_dfs
		else:
			print("Wrong usage of command. Use put /path/to/file /path/to/directory/in/dfs")			
			return
	if type == "get":
		if len(sys.argv) == 3:
			path_to_file = sys.argv[2]
			if path_to_file[0] != '/':
				path_to_file = my_directory + path_to_file
			comm = "get " + path_to_file
		else:
			print("Wrong usage of command. Use get /path/to/file")
			return 

	if type == "cd ":
		if len(sys.argv) == 3:
			path_to_go = sys.argv[2]
			if path_to_go[0] != '/':
				path_to_go = my_directory + path_to_go

			comm = "cd " + path_to_go

	if type == "pwd":
		print(my_directory)
		return

	response = ""
	sock.connect((MASTER_IP, MASTER_PORT))
	sock.send(comm.encode())
	while True:
		response = sock.recv(BUFFER_SIZE).decode()
		if response != "":
			break

	print(response)
	if type == "put":
		if response == "Starting":
			
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
					temp_sock.send(bytes_read)

			print("File uploaded")
			temp_sock.close()

	if type == "get":
		if response == "Starting":
			temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			temp_sock.bind(('0.0.0.0',9999))
			temp_sock.listen(1)

			datanode, address = temp_sock.accept()
			filename = path_to_file.split('/')[-1]
			with open(filename, "wb") as f:
				while True:
					bytes_read = temp_sock.recv(BUFFER_SIZE)
					if not bytes_read:
						break
					f.write(bytes_read)

			print("File downloaded")
			temp_sock.close()

	if type == "cd":
		if response == "Success":
			write_current_path(path_to_go)

	sock.close()


def write_current_path(path_to_go):
	with open("directory.txt", "wb") as f:
		f.write(path_to_go.encode())

if __name__ == "__main__":
	sock_to_master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	try:
		with open("directory.txt", "rb") as f:
			bytes_read = f.read(BUFFER_SIZE)
			my_directory = bytes_read.decode()
	except Exception as e:
		pass

	send_comm(sock_to_master)

