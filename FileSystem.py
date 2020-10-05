import hashlib

class File:
    """Abstraction of a files going to be uploaded to DFS"""
    def __init__(self, name, fullname, size, owner):
        self.name = name
        self.fullname = fullname
        self.size = size
        self.nodes = []
        self.owner = owner
        self.id = hashlib.md5(fullname.encode()).hexdigest()

    def get_info(self):
        return (self.name, self.path, self.size, self.nodes)

class Directory:
    """Abstraction of directories where files are located"""
    def __init__(self, name, parent_directory):
        self.name = name
        self.parent = parent_directory
        self.subdirs = []
        self.files = []

    def add_file(self, file):
        self.files.append(file)

    def add_directory(self, directory):
        self.subdirs.append(directory)

    def get_name(self):
        return self.name

    def get_subdirs_names(self):
        res = []
        for subdir in self.subdirs:
            res.append(subdir.get_name())
        return res

    def ls_dirs(self):
        return self.subdirs

    def ls_files(self):
        return self.files

    def get_subdir_by_name(self, name):
        for dir in self.subdirs:
            if dir.get_name() == name:
                return dir

    def is_empty(self):
        return len(self.subdirs) == 0 and len(self.files) == 0

    def get_parent(self):
        return self.parent

    def get_file_names(self):
        files = self.ls_files()
        res = []
        for f in files:
            res.append(f.name)
        return res

    def remove_file(self, file_name):
        for i in self.files:
            if i.name == file_name:
                self.files.remove(file_name)
                del i
                break

class FileSystem:
    def __init__(self, name):
        self.name = name
        self.root = Directory("", None)

    def check_path_exists(self, path):
        current_directory = self.root
        dirs = path.split('/')
        for dir in dirs:
            if dir == '':
                continue
            elif not dir in current_directory.get_subdirs_names():
                return False
            else:
                current_directory = current_directory.get_subdir_by_name(dir)

        return True

    def get_directory_by_path(self, path):
        current_directory = self.root
        dirs = path.split("/")
        for dir in dirs:
            if dir == "":
                continue
            else:
                current_directory = current_directory.get_subdir_by_name(dir)

        return current_directory

    def mkdir(self, dir_name, path):
        if not self.check_path_exists(path):
            return (f"{path} does not exists!", False)
        elif self.check_path_exists(path+'/'+dir_name):
            return (f"{path+'/'+dir_name} already exists", False)
        else:
            creation_directory = self.get_directory_by_path(path)
            new_dir = Directory(dir_name, creation_directory)
            creation_directory.add_directory(new_dir)
            return (f"Directory {path + '/' + dir_name} created successfully", True)

    def rm_dir(self, path, recursive=False):
        if not self.check_path_exists(path):
            return (f"{path} does not exists!", False)
        if path == "":
            return ("Can not delete root directory", False)
        dir_to_del = self.get_directory_by_path(path)
        empty = dir_to_del.is_empty()

        if empty:
            parent_dir = dir_to_del.get_parent()
            parent_dir.subdirs.remove(dir_to_del)
            del dir_to_del
        else:
            return (f"Directory {path} is not empty and deletion is not recursive!", False)

    def put_file(self, file, path):
        if not self.check_path_exists(path):
            return (f"Path {path} does not exist", False)
        dir_to_add = self.get_directory_by_path(path)
        if file.name in dir_to_add.get_file_names():
            return (f"File {file.name} already exists", False)

        dir_to_add.add_file(file)
        return ("File added", False)

    def rm_file(self, path):
        file_name = path.split('/')[-1]
        dir_path = path[:path.rfind('/')]

        dir_to_del = self.get_directory_by_path(dir_path)
        dir_to_del.remove_file(file_name)

    def ls(self, path):
        if not self.check_path_exists(path):
            return "Path {path} does not exist"
        else:
            dir = self.get_directory_by_path(path)
            return dir.get_subdirs_names() + dir.get_file_names()

















































