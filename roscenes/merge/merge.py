import json
import os
import subprocess

from pypcd import pypcd
from rich.progress import track
import random


def check_and_add_prefix_to_scenes(scenes_list, prefix_path):
    # 初始一个空的结果列表
    valid_scenes = []

    # 遍历给定的 scenes_list，检查和添加前缀路径
    for scene in scenes_list:
        full_path = os.path.join(prefix_path, scene)
        if os.path.exists(full_path):
            valid_scenes.append(full_path)
        else:
            print(f"Scene {scene} not found in the scene library.")

    return valid_scenes


class Merge:
    """数据融合"""

    def __init__(
        self,
        input_path,
        output_path,
        main_channel,
        worker_num=4,
    ):
        self.input_path = input_path
        self.output_path = output_path
        self.main_channel = main_channel
        self.worker_num = worker_num

    def merge(self):
        # 输出路径检查
        self.output_path_valid_check(self.output_path)

        # 1. 加载已标注的数据
        # 执行指令 roscenes load first
        print("1. Load roscenes data first")
        os.system("roscenes load")

        # 2. 获取有效的 scene name list
        scene_path_list = self.get_scene_path_list(self.input_path)
        print("2. All Need Merge Scene:")
        for scene_path in scene_path_list:
            print("Need Merge Scene:" + str(scene_path.split("/")[-1]))

        # 3. split trainval and test
        print("3. Split trainval and test")
        # if exists trainval and test scene file then split
        if os.path.exists(
            os.path.join(
                os.path.dirname(os.path.dirname(self.input_path)),
                "/.roscenes/trainval_list.lst",
            )
        ) and os.path.exists(
            os.path.join(
                os.path.dirname(os.path.dirname(self.input_path)),
                "/.roscenes/test_list.lst",
            )
        ):
            print("use self lst split scenes")
            with open(
                os.path.join(self.input_path, "/.roscenes/trainval_list.lst")
            ) as f:
                trainval_scene_path_list = f.read().splitlines()
            f.close()
            trainval_scene_path_list = [
                scene for scene in trainval_scene_path_list if scene in scene_path_list
            ]
            trainval_scene_path_list = check_and_add_prefix_to_scenes(
                trainval_scene_path_list, self.input_path
            )
            with open(os.path.join(self.input_path, "/.roscenes/test_list.lst")) as f:
                test_scene_path_list = f.read().splitlines()
            f.close()
            test_scene_path_list = [
                scene for scene in test_scene_path_list if scene in scene_path_list
            ]
            test_scene_path_list = check_and_add_prefix_to_scenes(
                test_scene_path_list, self.input_path
            )

        else:
            test_scene_path_list = random.sample(
                scene_path_list, int(len(scene_path_list) * 0.2)
            )
            trainval_scene_path_list = list(
                set(scene_path_list) - set(test_scene_path_list)
            )
        print("trainval scene list:")
        for scene_path in trainval_scene_path_list:
            print("trainval scene list:" + str(scene_path.split("/")[-1]))
        print("test scene list:")
        for scene_path in test_scene_path_list:
            print("test scene list:" + str(scene_path.split("/")[-1]))

        # 4. 合并
        print("4. Merge")
        for scene_path in track(test_scene_path_list):
            print(" " + str(scene_path.split("/")[-1]))
            self.merge_maps_samples_sweeps(scene_path, self.output_path)
            self.merge_jsons(scene_path, self.output_path, "v1.0-test")
            scene_path_list.remove(scene_path)
        for scene_path in track(trainval_scene_path_list):
            self.merge_maps_samples_sweeps(scene_path, self.output_path)
            self.merge_jsons(scene_path, self.output_path, "v1.0-trainval")
        # 5. 改写 .pcd 文件 为 .bin
        # Note : 因为 mmdet3d 等平台仅支持 .bin 文件
        # 所以需要将 .pcd 文件改写为 .bin 文件
        # 同时也同步改写 sample_data.json 中的点云文件名称
        self.pcd2bin()

    @staticmethod
    def merge_maps_samples_sweeps(input_path, output_path, force=False):
        # copy input_path/map to output_path/map
        # copy input_path/samples to output_path/samples
        # copy input_path/sweeps to output_path/sweeps

        # if set force=True, need remove maps samples sweeps folder first
        if force:
            # remove output_path/maps output_path/samples output_path/sweeps
            cmd = f"rm -rf {output_path}/maps"
            subprocess.run(cmd, shell=True)
            cmd = f"rm -rf {output_path}/samples"
            subprocess.run(cmd, shell=True)
            cmd = f"rm -rf {output_path}/sweeps"
            subprocess.run(cmd, shell=True)

        # check output_path/maps output_path/samples output_path/sweeps before copy
        if not os.path.exists(os.path.join(output_path, "maps")):
            os.makedirs(os.path.join(output_path, "maps"))
        if not os.path.exists(os.path.join(output_path, "samples")):
            os.makedirs(os.path.join(output_path, "samples"))
        if not os.path.exists(os.path.join(output_path, "sweeps")):
            os.makedirs(os.path.join(output_path, "sweeps"))

        # use rsync to copy
        cmd = f"rsync -r {input_path}/maps/ {output_path}/maps/"
        subprocess.run(cmd, shell=True)
        cmd = f"rsync -r {input_path}/samples/ {output_path}/samples"
        subprocess.run(cmd, shell=True)
        # sweeps folder can be empty , so check it first
        if os.path.exists(os.path.join(input_path, "sweeps")):
            cmd = f"rsync -r {input_path}/sweeps/ {output_path}/sweeps/"
            subprocess.run(cmd, shell=True)

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
            self.merge_nuscens_json(input_file, output_file)

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

        samples_path = os.path.join(self.output_path, "samples", self.main_channel)
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
        sweeps_path = os.path.join(self.output_path, "sweeps", self.main_channel)
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
            self.output_path, "v1.0-trainval", "sample_data.json"
        )
        if os.path.exists(trainval_sample_data_filepath):
            update_sample_data(trainval_sample_data_filepath)

        test_sample_data_filepath = os.path.join(
            self.output_path, "v1.0-test", "sample_data.json"
        )
        if os.path.exists(test_sample_data_filepath):
            update_sample_data(test_sample_data_filepath)

    @staticmethod
    def merge_nuscens_json(input_file, output_file):
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
    def get_scene_path_list(root_path):
        """获取所有需要导出的 scene 的路径列表

        Args:
            root_path (str): 数据集根目录
            该目录下结构如下：
            root_path
            ├── scene_name_xx
            ├── scene_name_xx
            └── ...

        Returns:
                scene_list (list): 需要导出的 scene 列表
        """
        if not os.path.exists(root_path):
            return []

        # get all scene name
        scene_name_list = os.listdir(root_path)

        # remove not folder file
        scene_name_list = [
            scene_name
            for scene_name in scene_name_list
            if os.path.isdir(os.path.join(root_path, scene_name))
        ]

        scene_path_list = []
        for scene_name in scene_name_list:
            scene_path = os.path.join(root_path, scene_name)
            scene_path_list.append(scene_path)

        def input_scene_path_valid_check(path):
            if not os.path.isdir(path):
                print(f"{path} is not a directory")
                return False

            if not os.path.exists(os.path.join(path, "maps")):
                print(f"{path} should have maps folder")
                return False
            if not os.path.exists(os.path.join(path, "samples")):
                print(f"{path} should have samples folder")
                return False
            if not os.path.exists(os.path.join(path, "sweeps")):
                # print with yellow color
                print("\033[33m" + f"{path} should have sweeps folder" + "\033[0m")
                # but sweeps folder can be empty

            if not os.path.exists(os.path.join(path, "v1.0-all")):
                print(f"{path} should have v1.0-all folder")
                return False

            if not os.path.exists(
                os.path.join(path, "v1.0-all", "sample_annotation.json")
            ):
                return False

            if not os.path.exists(os.path.join(path, "v1.0-all", "instance.json")):
                return False
            else:
                with open(os.path.join(path, "v1.0-all", "instance.json"), "r") as f:
                    instance_data = json.load(f)
                if len(instance_data) == 0:
                    return False

            if not os.path.exists(
                os.path.join(path, "v1.0-all", "sample_annotation.json")
            ):
                return False
            else:
                with open(
                    os.path.join(path, "v1.0-all", "sample_annotation.json"), "r"
                ) as f:
                    sample_annotation_data = json.load(f)
                if len(sample_annotation_data) == 0:
                    return False

            return True

        scene_path_list = [
            scene_path
            for scene_path in scene_path_list
            if input_scene_path_valid_check(scene_path)
        ]

        return scene_path_list

    @staticmethod
    def output_path_valid_check(path):
        if not os.path.isdir(path):
            os.makedirs(path)

        # should have map samples sweeps v1.0-trainval 文件夹
        if not os.path.exists(os.path.join(path, "maps")):
            os.makedirs(os.path.join(path, "maps"))
        if not os.path.exists(os.path.join(path, "samples")):
            os.makedirs(os.path.join(path, "samples"))
        if not os.path.exists(os.path.join(path, "sweeps")):
            os.makedirs(os.path.join(path, "sweeps"))
        if not os.path.exists(os.path.join(path, "v1.0-trainval")):
            os.makedirs(os.path.join(path, "v1.0-trainval"))
        if not os.path.exists(os.path.join(path, "v1.0-test")):
            os.makedirs(os.path.join(path, "v1.0-test"))
