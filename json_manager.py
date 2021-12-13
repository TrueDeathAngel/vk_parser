import json
import multiprocessing
import os


text_file_name = '1.json'
photo_file_name = '2.json'
link_file_name = '3.json'


def load_dict_from_file(file_name):
    if os.path.isfile(file_name) and os.stat(file_name).st_size != 0:
        with open(file_name, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        return {}


def load_dict_to_file(file_name, data_dict):
    with open(file_name, "w+", encoding="utf-8") as f:
        f.write(json.dumps(data_dict, indent=4, ensure_ascii=False))
