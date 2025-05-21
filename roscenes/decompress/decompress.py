import os

from .utils import decompress, parse_compressed_file_list


class Decompress:
    """解压数据"""

    def __init__(self, input_path, output_path, suffix=".tgz", worker_num=4):
        self.input_path = os.path.expanduser(input_path)
        self.output_path = os.path.expanduser(output_path)
        self.suffix = suffix
        self.worker_num = worker_num

        # default config
        self.archived_record = os.path.join(self.input_path, "archived.yaml")

    def decompress(self):
        # 1. 首先或许需要解压的文件列表
        compressed_files = parse_compressed_file_list(
            self.input_path, self.suffix, self.archived_record
        )

        # 2. 解压文件至目标文件夹
        decompress(compressed_files, self.output_path, self.worker_num)
