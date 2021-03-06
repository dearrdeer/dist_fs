# Distributes sysytems
# Project2. Simple DFS

#### Team members:
* Ayaz Baykov, [@dearrdeer](@dearrdeer), [a.baykov@innopolis.university](a.baykov@innopolis.university), BS18-DS01
* Ravida Saitova, [@RavidaSa](@RavidaSa), [r.saitova@innopolis.university](r.saitova@innopolis.university), BS18-DS01

#### Link to [Github repository](https://github.com/dearrdeer/dist_fs)
#### Link to [Docker Image namenode](https://hub.docker.com/repository/docker/deardeer322/namenode)
#### Link to [Docker Image datanode](https://hub.docker.com/repository/docker/deardeer322/datanode)

#### Contents:
0. [Project description ](#anchors-in-markdown)
1. [Components of created DFS](#components)
2. [Description of created DFS](#dfs)
3. [Implementation detailes](#implementation)
4. [How to run](#how)
5. [Supported commands](#commands)
6. [Challanges we faced](#challange)
7. [Drawbacks of our design](#drawbacks)
8. [Member contribution](#member)

#### 0. __Project description__ <a name="anchors-in-markdown"></a>

A fault-tolerant distributed file system (DFS) is a file system with data stored on a server. The data is accessed and processed as if it was stored on the local client machine. The DFS makes it convenient to share information and files among users on a network.  In this project, we implement a simple Distributed File System (DFS). Files hosted remotely on one or more storage servers. Separately, a single naming server will index the files, indicating which one is stored where. When a client wishes to access a file, it first contacts the naming server to obtain information about the storage server hosting it. 

File system support file reading, writing, creation, deletion, copy, moving, and info queries. It will also support certain directory operations - listing, creation, changing, and deletion. Files are replicated on multiple storage servers. DFS is fault-tolerant and the data will be accessible even if some of the network nodes are offline.

#### 1. __Components of created DFS__ <a name="components"></a>

The name node contains information about files in the storage in the dictionary: (path, an array of nodes containing that file). Moreover, it imitates the structure of the storage(directory) in disk. Client will connect the name server and pass the command it wants to execute. If the server knows the result of the command immediately it will return the answer for the client and connection to storage servers will not be needed. Otherwise, it will address the request to the appropriate data node. The naming server also contacts all data nodes periodically to register their status.

The primary function of storage servers is to provide clients with access to data. Clients access storage servers in order to read and write files. Storage servers respond to certain commands from the naming server.The storage server interacts with the naming server to transparently perform replication of files causing multiple storage servers to maintain copies of the same file.

#### 2. __Description of the created DFS__ <a name="dfs"></a>

![DFS](https://i.ibb.co/F0ymwSw/DFS-1.png)
<div style="page-break-after: always;"></div>

In case of replication, diagram is the following:

![REPL](https://i.ibb.co/GcHzdzD/DFS.png)     
<div style="page-break-after: always;"></div>
                      
#### 3. __Implementation details__ <a name="implementation"></a>

All commands from the client, firstly goes to the namenode. Then the server checks the validity of the command (does files exist, etc.).

If namenode decides that it can handle the command without involving datanodes, it will return the response immediately to the client. For example, if a user wants to make a new directory in the system name server will change only its own structure of the tree. And only when the client will upload a file to that directory storage server where namenode decided to store it will initialize that folder (lazy initialization). Such an approach decreases overall network overheat.

But when we can not handle commands without storage servers, the namenode will tell the client to wait when the datanode will make a connection to him.

Let’s say the client made a command: put alice.txt /user/alice.  Which is basically uploading the file “alice.txt” into directory /user/alice. After receiving and checking the command namenode will tell the client to open a specific port to where the chosen datanode will connect. Then it randomly chooses an alive storage node and sends clients' addresses to it. So, shortly, the client will be considered as a server and storage node will request the file and download to itself. In case of “get” commands it is again datanode who is going to connect and push data to the client. This kind of implementation provides us way to exclude the situations when malicious user 

Now we need to replicate the file we got in order to DFS to be fault tolerant. 

#### 4. __How to run__ <a name="how"></a>

__4.1 Prerequisites:__

You will need virtual private subnet 10.0.0.0/24. Namenode will look for data storages in ips range [10.0.0.11, 10.0.0.20]

__4.2 Docker:__

Run the datanode docker image in you storage nodes:

  - docker run -p 8042:8042 deardeer322/datanode
  
Run the namenode docker image in you master node:

  - docker run -p 9000:9000 -t -i deardeer322/namenode

__4.3 Client:__ 

Make sure the client is also in the VPC. Otherwise NAT will block connections 
from DFS nodes to the client.
Make sure MASTER_IP is pointing to the IP of your namenode.

__4.4 Run the commands:__

`python3 client.py command args*`

#### 5. Supported client commands: <a name="commands"></a>
* Initialize client storage

  Form: `$ python3 client.py init`
 
* File read

  Form: `$ python3 client.py get /path/to/file`

* File write

  Form: `$ python3 client.py put /path/to/file /path/to/directory/in/dfs`

* File or empty directory delete

  Form: `$ python3 client.py rm /path/to/del`

* File copy

  Form: `$ python3 client.py cp file_to_copy path/to/dir`
  
* File move

  Form: `$ python3 client.py mv file_to_copy path/to/dir`
  
* Open directory

  Form: `$ python3 client.py cd /path/to/go`
  
* Read directory

  Form: `$ python3 client.py ls path/to/dir`
  
* Make directory 

  Form: `$ python3 client.py mkdir path/to/dir/to/create`
  
* Delete directory with files

  Form: `$ python3 client.py rmrf /path/to/del`
  
* Show current directory 

  Form: `$ python3 client.py pwd`
  
* Get the info of the file 

  Form: `$ python3 client.py info path/to/dir`
  
* Used space info

  Form: `$ python3 client.py usage`

#### 6. __Challanges we faced__ <a name="challange"></a>

1. How to prohibit malicious users from directly connecting to storages. 

  Solution: prohibit all non-master connections to datanode. Instead it is the client who waits for the datanode to make actions.

2. How to implement replication without significantly losing in simplicity.

Solution: use already defined methods.

3. Deployment. If the client is not in the VPC datanode can not connect because the IP is transformed via NAT. 

Solution: created additional instances in VPS with clients.

#### 7. __Drawbacks of our design__ <a name="drawbacks"></a> 

1. Single point of failure. We did not have time to implement Secondary Namenode.
2. Some set of unlucky events may lead to data inconsistency. Example: the node having a replica of file A goes offline, then the system receives command to delete that file. And when the node will be repaired it will still have that file even if our system does not have it.
3. Fault tolerance works only with the assumption that the majority of servers do not fail simultaneously.
4. We also assumed that servers can not die after the beginning of execution of some command and before the end of it. We had to do that because otherwise code would become too complex to manage.

#### 8. __Member contribution__ <a name="member"></a>
* Ayaz Baykov, namenode
* Ravida Saitova, datanode

Other work(report, client, deployment) was performed in collaboration.

