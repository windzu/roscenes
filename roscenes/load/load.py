"""
Author: wind windzu1@gmail.com
Date: 2023-08-27 18:34:41
LastEditors: wind windzu1@gmail.com
LastEditTime: 2023-08-29 12:01:45
Description:
Copyright (c) 2023 by windzu, All Rights Reserved.
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from rich.progress import track

from .sus import LoadFromSUS


class Load:
    """标注结果导入"""

    def __init__(
        self,
        input_path_list: list,
        output_path_list: list,
        filter_enabled: bool = False,
    ):
        self.input_path_list = input_path_list
        self.output_path_list = output_path_list
        self.filter_enabled = filter_enabled

        self.max_workers = 8

        self._load_init()

    def _load_init(self):
        """初始化导出文件夹"""

        # 1. check input_path_list and output_path_list same length
        if len(self.input_path_list) != len(self.output_path_list):
            raise ValueError(
                f"input_path_list: {self.input_path_list} and output_path_list: {self.output_path_list} should be same length"
            )

        # 2. check input_path valid
        for input_path in self.input_path_list:
            if not os.path.exists(input_path):
                raise FileNotFoundError(f"input_path: {input_path} not found")

        # 3. check output_path valid ,if not exist, create it
        for output_path in self.output_path_list:
            if not os.path.exists(output_path):
                os.makedirs(output_path)

    def load(self):
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.load_from_sus, input_path, output_path): (
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
                description="load sus result",
            ):
                future.result()  # 等待所有任务完成

        return True

    def load_from_sus(self, input_path, output_path):
        load_from_sus = LoadFromSUS(input_path, output_path, self.filter_enabled)
        load_from_sus.load()

        return True


#     @staticmethod
#     def filter_no_result_scene(source_path_list, target_path_list):
#         """滤除没有标注结果的场景"""
#
#         target_source_path_list = []
#         target_target_path_list = []
#         no_result_scene_name_list = []
#         for source_path, target_path in zip(source_path_list, target_path_list):
#             result_path = os.path.join(source_path, "result")
#             if os.path.exists(result_path):
#                 target_source_path_list.append(source_path)
#                 target_target_path_list.append(target_path)
#             else:
#                 no_result_scene_name_list.append(os.path.basename(source_path))
#
#         if len(no_result_scene_name_list) > 0:
#             no_result_scene_name_list.sort()
#             print(f"no result scene: {no_result_scene_name_list}")
#
#         return target_source_path_list, target_target_path_list
