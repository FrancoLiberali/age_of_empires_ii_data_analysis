import json
import os

def file_open_or_create(dir_path, file_name, is_json):
    os.makedirs(os.path.dirname(dir_path), exist_ok=True)
    full_name = dir_path + file_name
    try:
        file = open(full_name, "r+")
        if is_json:
            content = json.load(file)
        else:
            content = file.readline()
        return file, content
    except FileNotFoundError:
        file = open(full_name, "w+")
        if is_json:
            content = {}
        else:
            content = ''
        return file, content

def write_at_beginning(file, text):
    file.seek(0)
    file.write(text)

def dump_dict_into_json(file, dict):
    file.seek(0)
    json.dump(dict, file)
