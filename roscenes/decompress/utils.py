import multiprocessing
import os
import subprocess
from functools import partial

import yaml
from rich.progress import track


def parse_compressed_file_list(input_path, suffix, archived_record):
    """读取压缩文件列表

    Args:
        input_path (str): 需要读取的根路径
        suffix (str): 符合格式的文件名后缀
        archived_record (str): 记录了已经被解压的文件的记录文件,将跳过这些文件
    """

    def get_files_from_directory(directory, suffix, excluded_paths=[]):
        file_list = []

        # convert ~ to absolute path
        directory = os.path.expanduser(directory)

        # 遍历指定目录
        for dirpath, dirnames, filenames in os.walk(directory):
            # 检查当前目录是否在排除列表中
            if not any(dirpath.startswith(ex_path) for ex_path in excluded_paths):
                for filename in filenames:
                    if filename.endswith(suffix):
                        file_list.append(os.path.join(dirpath, filename))
        return file_list

    files = get_files_from_directory(input_path, suffix)

    # file_list 与 archived_record 进行对比
    # - 移除 file_list 中已经被加入到 archived_record 中的文件，更新 file_list
    # - 将 file_list 中剩余的文件加入到 archived_record 中
    # - 返回更新后的 file_list
    def archived_record_update(file_list, archived_record):
        # judge file exist
        archived_record = os.path.expanduser(archived_record)

        # 如果还没有archived_record文件，创建一个
        if not os.path.exists(archived_record):
            with open(archived_record, "w") as file:
                yaml.dump([], file)

        # 打开 archived_record 文件，读取已经归档的文件列表
        archived_record_list = []
        with open(archived_record, "r") as file:
            archived_record_list = yaml.safe_load(file) or []

        # file_list 与 archived_record_list 进行对比
        # - 移除 file_list 中已经被加入到 archived_record 中的文件
        remaining_files = [f for f in file_list if f not in archived_record_list]

        # - 将 file_list 中剩余的文件加入到 archived_record 中
        archived_record_list.extend(remaining_files)

        # - 将新的 archived_record_list 写入 archived_record
        with open(archived_record, "w") as file:
            yaml.dump(archived_record_list, file)

        # 返回更新后的 file_list
        return remaining_files

    files = archived_record_update(files, archived_record)

    return files


def decompress(compressed_files, output_path, worker_num):
    """解压所有符合条件的压缩文件至目标文件夹"""
    print("compressed files: ")
    for file in compressed_files:
        print(file)
    print("Next Decompressing ...")
    print(f"total {len(compressed_files)} files need to be decompressed")

    # 使用多进程进行解压操作
    wrapped_function = partial(decompress_file_wrapper, output_path=output_path)
    with multiprocessing.Pool(processes=worker_num) as pool:
        list(
            track(
                pool.imap_unordered(wrapped_function, enumerate(compressed_files)),
                total=len(compressed_files),
            )
        )

    print("Decompressing Done!")


def decompress_file_wrapper(args, output_path):
    idx, file = args
    result = decompress_file(file, output_path)
    return idx, result


def decompress_file(file, output_path):
    dir_name = output_path
    base_name = os.path.basename(file).split(".")[0]
    output_dir = os.path.join(dir_name, base_name)
    output_dir = os.path.expanduser(output_dir)

    # 如果输出目录不存在，则创建
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        subprocess.run(
            ["tar", "xzf", file, "-C", output_dir],
            check=True,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as e:
        print(f"Command failed with error {e.returncode}: {e.stderr.decode('utf-8')}")
        raise e
