import AddFilesystem
import configparser
import os
import json


def read_file(filesystem: AddFilesystem, file: str) -> list:
    """Read-in text content of a single file."""

    with filesystem.open(file, "r") as f:
        return [line.strip() for line in f]


def read_files(filesystem: AddFilesystem, files: list) -> list:
    """Read-in text content of mutliple files."""
    files_content = []

    for file in files:
        files_content += read_file(filesystem, file)

    return files_content


def append_to_file(filesystem: AddFilesystem, file: str, rows: list) -> None:
    """Appends items in rows to file in a folder_path dir."""

    with filesystem.open(file, "a") as f:
        for row in rows:
            f.write(row + "\n")


def write_file(filesystem: AddFilesystem, file: str, rows: list) -> None:
    """(Over)writes items in rows to file in a folder_path dir."""

    with filesystem.open(file, "w") as f:
        for row in rows:
            f.write(row + "\n")


def dump_json(filesystem: AddFilesystem, file: str, json_object: dict) -> None:
    """(Over)writes items in rows to file in a folder_path dir."""

    with filesystem.open(file, "w") as f:
        json.dump(json_object, f, indent=2)


def merge_dicts_of_lists(dol1: dict, dol2: dict) -> dict:
    """Merges two dists of lists.
    Example:
    DICTS_OF_LISTS_1 = {
        'url_1': ['a', 'b', 'c'],
        'url_2': ['x', 'y']
    }
    DICTS_OF_LISTS_2 = {
        'url_2': ['z'],
        'url_3': ['d', 'e', 'f']
    }
    DICTS_OF_LISTS_MERGED = {
        'url_1': ['a', 'b', 'c'],
        'url_2': ['x', 'y', 'z'],
        'url_3': ['d', 'e', 'f']
    }
    """
    no = []
    keys = set(dol1).union(dol2)

    return dict((k, dol1.get(k, no) + dol2.get(k, no)) for k in keys)


def list_files(
    filesystem: AddFilesystem, base_dir: str, match_name: str = None
) -> list:
    """Recursively grabs files from all children of base_dir.

    Example:

    base_dir/
        my_file_level_1.txt
        sub_dir/
            my_file_level_2.py

    list_files(base_dir)
    >>> ['my_file_level_1.txt', 'my_file_level_2.py']
    """
    file_list = []
    for relative_dir, _, files in filesystem.walk(base_dir):
        for file in files:

            intermediate_dir = relative_dir.split(base_dir.split("/")[-2])[-1][1:]

            if match_name:
                if file == match_name:

                    file_list.append(os.path.join(base_dir, intermediate_dir, file))

            else:
                file_list.append(os.path.join(base_dir, intermediate_dir, file))

    return file_list


def add_prefix_to_elements(l: list, prefix: str):
    """Adds prefix to each element of a list"""

    return [prefix + i for i in l]


def get_all_ini_sections(relative_ini_path, with_keyword=None):
    """Gets all sections (headers) in .ini file."""
    full_ini_path = os.path.dirname(__file__) + relative_ini_path

    config = configparser.ConfigParser()
    config.read(full_ini_path)

    if with_keyword:
        return [section for section in config.sections() if with_keyword in section]
    else:
        return config.sections()


def add_mid_dir(path: str, dir_name: str, position: int) -> str:
    """Turns /some/path/ into /some/<dir_name>/path/.
    Position shown in the example = -2.
    """
    path_list = path.split("/")
    path_list.insert(position, dir_name)

    return "/".join(path_list)


def get_l1_not_in_l2(l1: list, l2: list) -> list:
    """Grabs all items from the 1st list that are not in the 2nd."""
    return list(set(l1).difference(l2))
