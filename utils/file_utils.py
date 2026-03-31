import os

def check_file_exists(file_path):
    return os.path.exists(file_path)

def check_file_not_empty(file_path):
    return os.path.getsize(file_path) > 0