import io
import json
import os

from abc import ABC, abstractmethod


class FileAlreadyExistError(Exception):
    pass

class File(ABC):
    def __init__(self, dir_path, file_name, read_content=True, only_create=False):
        os.makedirs(os.path.dirname(dir_path), exist_ok=True)
        self.full_name = dir_path + file_name
        try:
            self.file = open(self.full_name, "r+")
            if only_create:
                raise FileAlreadyExistError
            if read_content:
                self.content = self._load_data()
        except FileNotFoundError:
            self.file = open(self.full_name, "w+")
            if read_content:
                self.content = self._generate_data()

    def close(self):
        self.file.close()

    @abstractmethod
    def _load_data(self):
        pass

    @abstractmethod
    def _generate_data(self):
        pass

    @abstractmethod
    def write(self):
        pass


class OneLineFile(File):
    def _load_data(self):
        return self.file.readline()

    def _generate_data(self):
        return ""

    def write(self, text):
        self.file.seek(0)
        self.file.write(text)
        self.file.truncate()

class BooleanFile(File):
    TRUE_REPRESENTATION = "1"
    FALSE_REPRESENTATION = "0"

    def _load_data(self):
        line = self.file.readline()
        if line == BooleanFile.TRUE_REPRESENTATION:
            return True
        else:
            return False

    def _generate_data(self):
        return False

    def write(self, boolean):
        self.file.seek(0)
        if boolean == True:
            self.file.write(BooleanFile.TRUE_REPRESENTATION)
        else:
            self.file.write(BooleanFile.FALSE_REPRESENTATION)


class JsonFile(File):
    def _load_data(self):
        filesize = os.path.getsize(self.full_name)
        if filesize == 0:
            return {}
        else:
            return json.load(self.file)

    def _generate_data(self):
        return {}

    def write(self, dict):
        self.file.seek(0)
        json.dump(dict, self.file)
        self.file.truncate()

class ListFile(File):
    LIST_SEPARATOR = '\n'

    def _load_data(self):
        return self.file.read().split(ListFile.LIST_SEPARATOR)[0:-1]

    def _generate_data(self):
        return []

    def write(self, list):
        if len(list) > 0:
            list_string = ListFile.LIST_SEPARATOR.join(list)
            self.file.seek(0, io.SEEK_END)
            self.file.write(list_string)
            self.file.write(ListFile.LIST_SEPARATOR)


class ListOfJsonFile(ListFile):
    def _load_data(self):
        list = super()._load_data()
        return [json.loads(player_json) for player_json in list]

    def write(self, list_of_list):
        super().write(
            [json.dumps(list, ensure_ascii=True) for list in list_of_list]
        )

def safe_remove_file(file_path):
    try:
        os.remove(file_path)
    except FileNotFoundError:
        pass
