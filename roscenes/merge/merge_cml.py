import json
import os
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed

from pypcd import pypcd
from rich.progress import track

from ..common.nuscenes_check import nuscenes_check
from ..common.scene_check import scene_check

# def scene_check(scene_path):
#     # 1. check scene_path should be valid
#     if not os.path.exists(scene_path):
#         raise FileNotFoundError(f"{scene_path} not found")
#
#     # 2. check all json file valid in scene_path/nuscenes/v1.0-all folder
#     json_file_list = os.listdir(os.path.join(scene_path, "v1.0-all"))
#     if len(json_file_list) == 0:
#         raise FileNotFoundError(f"{scene_path} has no json file")
#     for json_file in json_file_list:
#         if not json_file.endswith(".json"):
#             raise FileNotFoundError(f"{scene_path} has invalid json file")
#         # check json file valid
#         with open(os.path.join(scene_path, "v1.0-all", json_file), "r") as f:
#             try:
#                 json.load(f)
#             except json.JSONDecodeError:
#                 raise ValueError(f"{scene_path} has invalid json file")
#
#     return True


class Merge:
    """数据融合"""

    def __init__(
        self,
        source_scene_path_list: list,
        target_nuscenes_path: str,
        target_type: str,
        main_channel: str,
        max_workers: int = 8,
    ):
        self.source_scene_path_list = source_scene_path_list
        self.target_nuscenes_path = target_nuscenes_path
        self.target_type = target_type
        self.main_channel = main_channel  # used to convert pcd to bin
        self.max_workers = max_workers

        # target type should be v1.0-trainval or v1.0-test
        if self.target_type not in ["v1.0-trainval", "v1.0-test"]:
            raise ValueError("target type should be v1.0-trainval or v1.0-test")

    def merge(self):
        # 1. valid_check
        print("1. valid check")
        self.valid_check()

        # 2. 合并
        # print("2. merge data")
        # for scene_path in track(self.source_scene_path_list):
        #     print("     merging " + str(scene_path))
        #     self.merge_maps_samples_sweeps(scene_path, self.target_nuscenes_path)
        #     self.merge_jsons(scene_path, self.target_nuscenes_path, self.target_type)

        print("2. merge data")
        # 根据worker数量决定使用多进程加速
        # - 如果max_workers=1,则使用单进程,每一次只处理一个scene
        # - 如果max_workers>1,则使用多进程,每一次处理多个scene，但是为了避免json文件写入的冲突，json文件的写入是串行的（单独做）
        if self.max_workers == 1:
            print("     merging scenes with single process:")
            for scene_path in track(self.source_scene_path_list):
                self.merge_scene(scene_path)
        else:
            print(f"     merging scenes with {self.max_workers} processes:")
            # - merge_maps_samples_sweeps 是可以并行的
            print("     merging maps samples sweeps")
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(
                        self.merge_maps_samples_sweeps,
                        scene_path,
                        self.target_nuscenes_path,
                    ): scene_path
                    for scene_path in self.source_scene_path_list
                }
                for future in track(
                    as_completed(futures),
                    total=len(futures),
                    description="Merging scenes",
                ):
                    future.result()  # 等待所有任务完成

            # - merge_jsons
            print("     merging jsons")
            self.merge_all_jsons(
                self.source_scene_path_list,
                self.target_nuscenes_path,
                self.target_type,
            )

        # 3. 改写 .pcd 文件 为 .bin
        # Note : 因为 mmdet3d 等平台仅支持 .bin 文件
        # 所以需要将 .pcd 文件改写为 .bin 文件
        # 同时也同步改写 sample_data.json 中的点云文件名称
        print("3. convert pcd to bin")
        self.pcd2bin()

    def valid_check(self):
        # 1. check all source_scene_path_list should be valid
        for scene_path in self.source_scene_path_list:
            if not os.path.exists(scene_path):
                raise FileNotFoundError(f"{scene_path} not found")

        # 2. check target_nuscenes_path should be valid
        if nuscenes_check(self.target_nuscenes_path) is False:
            raise FileNotFoundError(
                f"{self.target_nuscenes_path} should be nuscenes , but invalid"
            )

        # 3. check each source_scene valid , if not valid , raise error
        invalid_scene_path_list = []
        for scene_path in track(self.source_scene_path_list):
            if scene_check(scene_path) is False:
                invalid_scene_path_list.append(scene_path)
                # raise ValueError(f"{scene_path} is invalid")
        if len(invalid_scene_path_list) > 0:
            # echo invalid scene path
            raise ValueError(f"{invalid_scene_path_list} is invalid")

    def merge_scene(self, scene_path):
        print("     merging " + str(scene_path))
        self.merge_maps_samples_sweeps(scene_path, self.target_nuscenes_path)
        self.merge_jsons(scene_path, self.target_nuscenes_path, self.target_type)

    @staticmethod
    def merge_maps_samples_sweeps(input_path, output_path):
        # copy input_path/map to output_path/map
        # copy input_path/samples to output_path/samples
        # copy input_path/sweeps to output_path/sweeps

        # use rsync to copy
        cmd = f"rsync -r {input_path}/maps/ {output_path}/maps/"
        subprocess.run(cmd, shell=True)
        cmd = f"rsync -r {input_path}/samples/ {output_path}/samples"
        subprocess.run(cmd, shell=True)
        # sweeps folder can be empty , so check it first
        if os.path.exists(os.path.join(input_path, "sweeps")):
            cmd = f"rsync -r {input_path}/sweeps/ {output_path}/sweeps/"
            subprocess.run(cmd, shell=True)

    def merge_all_jsons(
        self,
        input_path_list: list,
        output_path: str,
        merge_type: str,
    ):
        """在一个函数中合并所有scene的json文件(或许能提高效率)

        Args:
            input_path_list (list): a list of input path
            output_path (str): output path
            merge_type (str): v1.0-trainval or v1.0-test
        """
        # input_file_list = []
        # output_file_list = []

        # 1. 获取输入输出文件列表,按照类别进行分组
        input_json_file_dict = {}
        for input_path in input_path_list:
            # list all files in input_path/v1.0-all
            input_filename_list = os.listdir(os.path.join(input_path, "v1.0-all"))
            for filename in input_filename_list:
                if filename not in input_json_file_dict:
                    input_json_file_dict[filename] = []
                input_json_file_dict[filename].append(
                    os.path.join(input_path, "v1.0-all", filename)
                )

        # 2. remove map.json from input_json_file_dict
        map_json_file_list = []
        if "map.json" in input_json_file_dict:
            map_json_file_list = input_json_file_dict.pop("map.json")
        else:
            raise FileNotFoundError("map.json not found in input_json_file_dict")

        # 3. merge all json files except map.json
        print("     merging json files")
        for filename in track(input_json_file_dict):
            input_file_list = input_json_file_dict[filename]
            output_file = os.path.join(output_path, merge_type, filename)
            self.merge_nuscenes_jsons(input_file_list, output_file)

        # 4. merge map.json
        map_json_output_file = os.path.join(output_path, merge_type, "map.json")
        self.merge_map_jsons(map_json_file_list, map_json_output_file)

        return True

    def merge_jsons(self, input_path, output_path, merge_type):
        input_file_list = []
        output_file_list = []

        # 1. 获取输入输出文件列表
        # list all files in input_path/v1.0-all
        input_filename_list = os.listdir(os.path.join(input_path, "v1.0-all"))
        # Note : 因为map.json 比较特殊 暂时先从list中移除 map.json
        input_filename_list.remove("map.json")
        for filename in input_filename_list:
            input_file_list.append(os.path.join(input_path, "v1.0-all", filename))
        # generate output_file_list
        for filename in input_filename_list:
            output_file_list.append(os.path.join(output_path, merge_type, filename))

        # 2. 合并
        input_file_list = sorted(input_file_list)
        output_file_list = sorted(output_file_list)

        for input_file, output_file in zip(input_file_list, output_file_list):
            self.merge_nuscenes_json(input_file, output_file)

        # 3. 合并map.json
        # merge map.json 与 其他json文件不同,
        # 因为map.json中有一个log_tokens的key,这个key的值是一个list
        # 所以需要单独处理
        input_file = os.path.join(input_path, "v1.0-all", "map.json")
        output_file = os.path.join(output_path, merge_type, "map.json")
        self.merge_map_json(input_file, output_file)

    def pcd2bin(self):
        """将pcd文件转换为bin文件"""
        # 1. 获取输入文件列表
        samples_pcd_filepath_list = []
        sweeps_pcd_filepath_list = []

        samples_path = os.path.join(
            self.target_nuscenes_path, "samples", self.main_channel
        )
        if os.path.exists(samples_path):
            samples_filename_list = os.listdir(samples_path)
            samples_filepath_list = [
                os.path.join(samples_path, filename)
                for filename in samples_filename_list
            ]
            samples_pcd_filepath_list = [
                filepath
                for filepath in samples_filepath_list
                if filepath.endswith(".pcd")
            ]
        sweeps_path = os.path.join(
            self.target_nuscenes_path, "sweeps", self.main_channel
        )
        if os.path.exists(sweeps_path):
            sweeps_filename_list = os.listdir(sweeps_path)
            sweeps_filepath_list = [
                os.path.join(sweeps_path, filename) for filename in sweeps_filename_list
            ]
            sweeps_pcd_filepath_list = [
                filepath
                for filepath in sweeps_filepath_list
                if filepath.endswith(".pcd")
            ]

        pcd_filepath_list = samples_pcd_filepath_list + sweeps_pcd_filepath_list

        def replace_pcd_to_bin(pcd_filepath):
            pc = pypcd.PointCloud.from_path(pcd_filepath)
            # no need remove .pcd file
            # os.remove(pcd_filepath)
            bin_filepath = pcd_filepath + ".bin"

            pc.save_bin(bin_filepath, "xyzi")

        # 2. 转换
        for pcd_filepath in track(
            pcd_filepath_list, description="convert pcd to bin file"
        ):
            replace_pcd_to_bin(pcd_filepath)

        # 3. 修改 sample_data.json 中的点云文件名称
        def update_sample_data(sample_data_filepath):
            # 3.1 读取 sample_data.json
            with open(sample_data_filepath, "r") as f:
                sample_data = json.load(f)

            # 3.2 修改 sample_data.json 中的点云文件名称 为其加上后缀.bin
            for item in sample_data:
                if item["filename"].endswith(".pcd"):
                    item["filename"] += ".bin"

            # 3.3 写入 sample_data.json
            with open(sample_data_filepath, "w") as f:
                json.dump(sample_data, f, indent=4)

        trainval_sample_data_filepath = os.path.join(
            self.target_nuscenes_path, "v1.0-trainval", "sample_data.json"
        )
        if os.path.exists(trainval_sample_data_filepath):
            update_sample_data(trainval_sample_data_filepath)

        test_sample_data_filepath = os.path.join(
            self.target_nuscenes_path, "v1.0-test", "sample_data.json"
        )
        if os.path.exists(test_sample_data_filepath):
            update_sample_data(test_sample_data_filepath)

    @staticmethod
    def merge_nuscenes_json(input_file, output_file):
        """合并nuscenes的json文件

        所有的json文件中的内容都是list,每一个list中的元素都是一个dict,
        每一个dict都含有一个key为"token"的元素，这个元素的值是一个唯一的字符串

        Args:
            input_file (str): 输入文件的路径
            output_file (str): 输出文件的路径
        """

        # 1. 文件路径检查
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"{input_file} not found")
        if not os.path.exists(output_file):
            # create empty output_file

            with open(output_file, "w") as f:
                f.write("[]")

        # 2. 读取输入和输出文件
        with open(input_file, "r") as f:
            input_data = json.load(f)
        with open(output_file, "r") as f:
            output_data = json.load(f)

        # 3. 创建一个基于token的字典来跟踪输出数据
        output_data_dict = {item["token"]: item for item in output_data}

        # 4. 合并数据
        for item in input_data:
            output_data_dict[item["token"]] = item  # 如果token存在则覆盖，不存在则添加

        # 5. 将字典转换回列表
        output_data = list(output_data_dict.values())

        # 6. 格式化写入输出文件
        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=4)

    @staticmethod
    def merge_nuscenes_jsons(input_files: list, output_file: str):
        """多个json文件合并为一个nuscenes的json文件

        所有的json文件中的内容都是list,每一个list中的元素都是一个dict,
        每一个dict都含有一个key为"token"的元素，这个元素的值是一个唯一的字符串

        Args:
            input_files (list): 多个输入文件的路径
            output_file (str): 输出文件的路径
        """

        # 1. 文件路径检查
        for input_file in input_files:
            if not os.path.exists(input_file):
                raise FileNotFoundError(f"{input_file} not found")
        if not os.path.exists(output_file):
            # create empty output_file

            with open(output_file, "w") as f:
                f.write("[]")

        # 2. 创建一个空字典，目的是基于token的字典来跟踪输出数据
        with open(output_file, "r") as f:
            output_data = json.load(f)
        output_data_dict = {}

        # 3. 合并数据
        for input_file in input_files:
            with open(input_file, "r") as f:
                input_data = json.load(f)
            for item in input_data:
                output_data_dict[item["token"]] = item

        # 4. 将字典转换回列表
        output_data = list(output_data_dict.values())

        # 5. 格式化写入输出文件
        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=4)

    @staticmethod
    def merge_map_json(input_file, output_file):
        """合并nuscenes的map.json文件

        所有的json文件中的内容都是list,每一个list中的元素都是一个dict,
        每一个dict都含有一个key为"token"的元素，这个元素的值是一个唯一的字符串

        但是map.json有一个特殊的地方,就是它有一个 log_tokens 的key,这个key的值是一个list,
        如果遇到两个token相同的map.json,那么就将这两个map.json中的log_tokens合并

        Args:
            input_file (str): 输入文件的路径
            output_file (str): 输出文件的路径
        """

        # 1. 文件路径检查
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"{input_file} not found")
        if not os.path.exists(output_file):
            # create empty output_file

            with open(output_file, "w") as f:
                f.write("[]")

        # 2. 读取输入文件
        with open(input_file, "r") as f:
            input_data = json.load(f)
        with open(output_file, "r") as f:
            output_data = json.load(f)

        # 3. 合并
        # 3.1 构建以token为key的字典
        output_token_dict = {}
        for item in output_data:
            output_token_dict[item["token"]] = item

        input_token_dict = {}
        for item in input_data:
            input_token_dict[item["token"]] = item

        # 3.2 遍历 input_token_dict, 如果token在output_token_dict中,那么就合并log_tokens
        for token in input_token_dict:
            if token in output_token_dict:
                output_token_dict[token]["log_tokens"] += input_token_dict[token][
                    "log_tokens"
                ]
            else:
                output_token_dict[token] = input_token_dict[token]

        # 3.3 将output_token_dict转换为list
        output_data = []
        for token in output_token_dict:
            output_data.append(output_token_dict[token])

        # 3.4 对 log_tokens list 去重
        for item in output_data:
            item["log_tokens"] = list(set(item["log_tokens"]))

        # 4. 格式化写入输出文件
        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=4)

    @staticmethod
    def merge_map_jsons(input_file_list: list, output_file: str):
        """合并nuscenes的map.json文件

        所有的json文件中的内容都是list,每一个list中的元素都是一个dict,
        每一个dict都含有一个key为"token"的元素，这个元素的值是一个唯一的字符串

        但是map.json有一个特殊的地方,就是它有一个 log_tokens 的key,这个key的值是一个list,
        如果遇到两个token相同的map.json,那么就将这两个map.json中的log_tokens合并

        Args:
            input_file (str): 输入文件的路径
            output_file (str): 输出文件的路径
        """

        # 1. 文件路径检查
        for input_file in input_file_list:
            if not os.path.exists(input_file):
                raise FileNotFoundError(f"{input_file} not found")
        if not os.path.exists(output_file):
            # create empty output_file

            with open(output_file, "w") as f:
                f.write("[]")

        # 2. create empty dict , key is token
        output_token_dict = {}

        # 3. merge
        for input_file in input_file_list:
            with open(input_file, "r") as f:
                input_data = json.load(f)
            for item in input_data:
                token = item["token"]
                if token in output_token_dict:
                    output_token_dict[token]["log_tokens"] += item["log_tokens"]
                else:
                    output_token_dict[token] = item

        # 4. convert dict to list
        output_data = list(output_token_dict.values())

        # 5. remove duplicate log_tokens
        for item in output_data:
            item["log_tokens"] = list(set(item["log_tokens"]))

        # 6. write to output file
        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=4)
