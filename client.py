import os, socket
import sys

BUFFER_SIZE = 4096
MASTER_IP = 'localhost'
MASTER_PORT = 9000
my_directory = "/"

# Send command to the namenode socket
def send_comm(sock):
	comm = ""
	filename = ""
	path_to_file = ""
	path_to_go = ""
	# Check if command was provided
	if len(sys.argv) == 1:
		print("No command provided")
		return
	# Identify its type 
	type = sys.argv[1]

	if type == "info":
	# Request to information about scpecific file 
	# Chech the correctness of the inputed command
		if len(sys.argv) == 3:
			file_to_info = sys.argv[2]
			if file_to_info[0] != '/':
				file_to_info = my_directory + file_to_info
			# Prepare command to the namenode
			comm = "info " + file_to_info
		else:
			print("Wrong usage of command. Use info path/to/dir")
			return

	if type=="init":
		# InitializeInitialize the client storage on a new system, 
		# should remove any existing file in the dfs root directory and return available size.
		comm = "init"

	if type=="usage":
		# Return the used size
		comm = "usage"
		
	if type == "ls":
		# Return list of files and directories, which are stored in the directory.
		if len(sys.argv) == 2 or len(sys.argv) == 3:
			directory_to_list = my_directory if len(sys.argv) == 2 else sys.argv[2]
			if directory_to_list[0] != '/':
				directory_to_list = my_directory + directory_to_list

			comm = "ls " + directory_to_list
		else:
			print("Wrong usage of command. Use ls path/to/dir")
			return
	
	if type=="cp":
		# Create a copy of file into specified file 
		if len(sys.argv) == 4:
			directory_to_copy = sys.argv[3]
			if directory_to_copy[0] != '/':
				directory_to_copy = my_directory + directory_to_copy
			comm = "cp" + " " + sys.argv[2] + " " + directory_to_copy
		else:
			print("Wrong usage of command. Use cp file_to_copy path/to/dir")
			return
			
	if type == "mv":
		# Move a file to the specified path.
		if len(sys.argv) == 4:
			directory_to_copy = sys.argv[3]
			if directory_to_copy[0] != '/':
				directory_to_copy = my_directory + directory_to_copy
			comm = "mv" + " " + sys.argv[2] + " " + directory_to_copy
		else:
			print("Wrong usage of command. Use mv file_to_copy path/to/dir")
			return

	if type == "mkdir":
		# Create a new directory incide current
		# Or if path starts with '/' staring from DFS root 
		if len(sys.argv) == 3:
			path_to_create = sys.argv[2]
			if path_to_create[0] != '/':
				path_to_create = my_directory + path_to_create
			comm = "mkdir " + path_to_create
		else: 
			print("Wrong usage of command. Use mkdir path/to/dir/to/create")
			return

	if type == "put":
		# Pput any file to DFS, since pass everything as a bytestream
		if len(sys.argv) == 4:
			filename = sys.argv[2]
			path_in_dfs = sys.argv[3]
			if path_in_dfs[0] != '/':
				path_in_dfs = my_directory + path_in_dfs
			# Check if scpecified file exists in client dir
			if not os.path.isfile(filename):
				print(f"File {filename} not found")
				return
			only_name = filename.split('/')[-1]
			comm = "put " + only_name + " " + path_in_dfs
		else:
			print("Wrong usage of command. Use put /path/to/file /path/to/directory/in/dfs")			
			return

	if type == "get":
		# Read any file from DFS
		if len(sys.argv) == 3:
			path_to_file = sys.argv[2]
			print(path_to_file)
			if path_to_file[0] != '/':
				path_to_file = my_directory + path_to_file
			comm = "get " + path_to_file
		else:
			print("Wrong usage of command. Use get /path/to/file")
			return 

	if type == "cd":
		if len(sys.argv) == 3 or len(sys.argv) == 2:
			path_to_go = sys.argv[2] if len(sys.argv) == 3 else '/'
			if path_to_go[0] != '/':
				path_to_go = my_directory + path_to_go 		
			comm = "cd " + path_to_go
		else:
			print("Wrong usage of command. Use cd /path/to/go")			
			return

	if type == "pwd":
		print(my_directory)
		return

	if type == "rm":
		# Allow to remove file in the DFS or empty directory.
		# If the directory contains files the system will return the message with recommendation 
		# to use rmrf instead 
		if len(sys.argv) == 3:
			file_to_del = sys.argv[2]
			if file_to_del[0] != '/':
				file_to_del = my_directory + file_to_del
			comm = "rm " + file_to_del
		else:
			print("Wrong usage of command. Use rm /path/to/del")			
			return			

	if type == "rmrf":
		# Delete directory with files
		if len(sys.argv) == 3:
			file_to_del = sys.argv[2]
			if file_to_del[0] != '/':
				file_to_del = my_directory + file_to_del
			comm = "rmrf " + file_to_del
		else:
			print("Wrong usage of command. Use rmrf /path/to/del")			
			return
	response = ""
	sock.connect((MASTER_IP, MASTER_PORT))
	sock.send(comm.encode())
	while True:
		response = sock.recv(BUFFER_SIZE).decode()
		if response != "":
			break
	

	
	if type == "put":
		# The responce handler
		# Start the socket allowing the datanode to connect to it
		# Put file directly in connected datanode
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
					datanode.send(bytes_read)
			print("File uploaded")
			datanode.close()

	if type == "get":
		# Get file from connected datanode
		if response == "Starting":
			temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			temp_sock.bind(('0.0.0.0',9999))
			temp_sock.listen(1)
			datanode, address = temp_sock.accept()
			filename = path_to_file.split('/')[-1]
			with open(filename, "wb") as f:
				while True:
					bytes_read = datanode.recv(BUFFER_SIZE)
					if not bytes_read:
						break
					f.write(bytes_read)
			print("File downloaded")
			datanode.close()

	if type == "cd":
		if response == "Success":
			write_current_path(path_to_go)

	if type == "cp":
		if response == "Starting":
			print("File copied")

	sock.close()



def write_current_path(path_to_go):
	# Return current directory of the file
	with open("directory.txt", "wb") as f:
		if path_to_go[-1] != '/':
			path_to_go += '/'
		f.write(path_to_go.encode())

if __name__ == "__main__":
	# Make connection with master
	sock_to_master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
		with open("directory.txt", "rb") as f:
			bytes_read = f.read(BUFFER_SIZE)
			my_directory = bytes_read.decode()
	except Exception as e:
		pass
	# Send command to the master-namenode
	send_comm(sock_to_master)
