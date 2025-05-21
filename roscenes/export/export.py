import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

from rich.progress import track

from .sus import ExportToSUS


class Export:
    """导出数据集

    Args:
        config (DataConfig): 数据集配置
        ws_raw_path (str): 数据集根目录

    """

    def __init__(self, input_path_list: list, output_path_list: list):
        self.input_path_list = input_path_list
        self.output_path_list = output_path_list
        self.max_workers = 8

        self._export_init()

    def _export_init(self):
        """初始化导出文件夹"""
        # 1. check input_path_list and output_path_list same length
        if len(self.input_path_list) != len(self.output_path_list):
            raise ValueError(
                f"input_path_list: {self.input_path_list} and output_path_list: {self.output_path_list} should be same length"
            )

        # debug
        # check each pair of input_path and output_path have same father path
        for input_path, output_path in zip(self.input_path_list, self.output_path_list):
            if os.path.dirname(input_path) != os.path.dirname(output_path):
                raise ValueError(
                    f"input_path: {input_path} and output_path: {output_path} should have same father path"
                )

        # 2. check input_path valid
        for input_path in self.input_path_list:
            if not os.path.exists(input_path):
                raise FileNotFoundError(f"input_path: {input_path} not found")

        # 3. check output_path valid ,if not exist, create it
        for output_path in self.output_path_list:
            if not os.path.exists(output_path):
                os.makedirs(output_path)

    def export(self):
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.export_to_sus, input_path, output_path): (
                    input_path,
                    output_path,
                )
                for input_path, output_path in zip(
                    self.input_path_list, self.output_path_list
                )
            }
            for future in track(
                as_completed(futures),
                total=len(futures),
                description="export to sus",
            ):
                future.result()  # 等待所有任务完成

        return True

    def export_to_sus(self, input_path, output_path):
        export_to_sus = ExportToSUS(input_path, output_path)
        export_to_sus.export()

        return True
