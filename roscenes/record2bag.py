import os
from argparse import ArgumentParser
from rich.progress import track
from rosbag import Bag, Compression
from datetime import date
from .common.data_config import DataConfig
from .common.utils import add_bag_info
import subprocess
import re


def extract_numbers(folder_name):
    """匹配并提取文本字符串中的尾部数字。"""
    match = re.search(r"-(\d+)_YC", folder_name)
    if match:
        return int(match.group(1))  # 返回匹配到的整数部分
    return None


def is_container_running(container_name):
    process = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )
    running_containers = process.stdout.splitlines()
    return container_name in running_containers


def get_sort_key(file_path):
    # 使用正则表达式提取文件名中的数字部分
    match = re.search(r"\.record\.(\d+)\.bag", file_path)
    if match:
        return int(match.group(1))
    else:
        return float("inf")  # 未匹配到时返回一个较大的值


class MergeBag:
    def __init__(
        self,
        input_path_list,
        output="./output.bag",
        compression="lz4",
    ):
        self.input_path_list = input_path_list
        self.output = output
        self.compression = self.parse_compression(compression)

    def run(self):
        if len(self.input_path_list) == 0:
            return

        with Bag(self.output, "w", compression=self.compression) as o:
            for file_path in track(self.input_path_list):
                # print(f"merge {file_lidar_path,file_camera_path}")
                print("file_path", file_path)
                with Bag(file_path, "r") as ib:
                    for topic, msg, t in ib:
                        o.write(topic, msg, t)

    @staticmethod
    def parse_compression(compression):
        if compression == "none" or compression == "NONE":
            compression = Compression.NONE
        elif compression == "bz2":
            compression = Compression.BZ2
        elif compression == "lz4":
            compression = Compression.LZ4
        return compression


class SplitScene:
    """防止单个场景过大，将场景拆分成多个场景,并且为了保障测试,如果是训练集的数据,还可以选择将部分训练场景拆分成测试场景,具体规则如下

    检查bag的时间长度,用于scene的划分(这里默认每个bag的时长都是一样的)

    1 如果单个包时长大于 min_bag_duration,则一个包将独立成为一个scene
    1.1 如果只有一个包,则不需要拆分
    1.2 如果有多个包,则每个包都将独立成为一个scene,此时scene的命名规则见下面示例
    - `0001_YC200-2021-007` 有1个包 -> `0001_YC200-2021-007`
    - `0001_YC200-2021-007` 有3个包 -> `0001-0_YC200-2021-007`, `0001-1_YC200-2021-007`, `0001-2_YC200-2021-007`

    2 如果单个包时长小于 min_bag_duration,则尝试通过合并多个包来达到最小时长
    2.1 如果进行多组合并后时长仍然小于最小时长,则将多个包合并成一个scene
    2.2 如果进行多组合并后,剩下了一个包,则该包将独立成为一个scene

    3 数据拆分

    4 如果拆分后的数据集中有多个scene,则将其中20%的scene移动到test目录下

    """

    def __init__(self, scene_path, min_bag_duration=20, test_scene_path=None):
        self.scene_path = scene_path
        self.min_bag_duration = min_bag_duration
        self.test_scene_path = test_scene_path

        self.new_bug_folders = []

        # 1. get all bag files (scene_path/bags/*.bag)
        self.bag_files = []
        bags_path = os.path.join(scene_path, "bags")
        for root, dirs, files in os.walk(bags_path):
            for file in files:
                if file.endswith(".bag"):
                    self.bag_files.append(os.path.join(root, file))

    def split(self):
        # 1. check nums of bag files
        if len(self.bag_files) == 0:
            # echo no bag files in scene_path
            print(f"No bag files in {self.scene_path}")
            return
        elif len(self.bag_files) == 1:
            # echo only one bag file in scene_path
            print(f"Only one bag file in {self.scene_path} , no need to split")
            return

        # sort bag files by name
        self.bag_files.sort()

        # 2. split group by duration
        group_id_files_dict = {}
        group_id_duration_sum_dict = {}

        group_count = 0
        for bag_file in self.bag_files:
            # 1. check if group_id_files_dict[group_count] exist
            if group_count not in group_id_files_dict:
                group_id_files_dict[group_count] = []
                group_id_duration_sum_dict[group_count] = 0

            # 2. check current group duration_sum
            try:
                current_bag_duration = self.get_bag_duration(bag_file)
            except:
                os.system(f"rosbag reindex {bag_file}")
                current_bag_duration = self.get_bag_duration(bag_file)
            if group_id_duration_sum_dict[group_count] > self.min_bag_duration:
                group_count += 1
                # add bag file to next group but need to check first
                if group_count not in group_id_files_dict:
                    group_id_files_dict[group_count] = []
                    group_id_duration_sum_dict[group_count] = 0

                group_id_files_dict[group_count].append(bag_file)
                group_id_duration_sum_dict[group_count] += current_bag_duration
                continue
            else:
                # 3. directly add bag file to current group
                group_id_files_dict[group_count].append(bag_file)
                group_id_duration_sum_dict[group_count] += current_bag_duration

        # 3. create new scene folder and copy bag files
        # check group_id_files_dict size first
        if len(group_id_files_dict) == 1:
            # only one group, no need to split
            return
        current_scene_name = os.path.basename(self.scene_path)
        current_scene_id = current_scene_name.split("_")[0]
        current_scene_car_id = current_scene_name.split("_")[1]
        current_scene_farther_path = os.path.dirname(self.scene_path)
        for group_id, bag_files in group_id_files_dict.items():
            # create new scene folder
            new_scene_name = f"{current_scene_id}-{group_id}_{current_scene_car_id}"
            new_scene_path = os.path.join(current_scene_farther_path, new_scene_name)
            # check and create new scene folder and bags folder
            if not os.path.exists(new_scene_path):
                os.mkdir(new_scene_path)
                os.mkdir(os.path.join(new_scene_path, "bags"))
            else:
                # remove all files in new scene folder
                os.system(f"rm -rf {new_scene_path}/*")

            # copy bag files to new scene folder
            for bag_file in bag_files:
                os.system(f"cp {bag_file} {os.path.join(new_scene_path, 'bags')}")

            self.new_bug_folders.append(new_scene_path)

        # rm raw scene folder
        os.system(f"rm -rf {self.scene_path}")

    def get_bag_duration(self, bag_file):
        duration = 0
        with Bag(bag_file, "r") as bag:
            for topic, msg, t in bag:
                if t.to_sec() > duration:
                    duration = t.to_sec()
        return duration


def record2bag(args, unknown):
    # parse unknown args
    # --cml : check if exist
    # -i/--input : input path
    # -o/--output : output path

    parser = ArgumentParser(add_help=False)
    parser.add_argument("--cml", action="store_true", default=False)
    parser.add_argument("-i", "--input", type=str, required=False)
    parser.add_argument("-o", "--output", type=str, required=False)
    parser.add_argument(
        "--container_name", type=str, required=False, default="yczx_dev"
    )

    args, unknown = parser.parse_known_args(unknown)
    cml_mode = args.cml
    input_path = args.input
    output_path = args.output
    container_name = args.container_name

    # use cmd line mode , no need to check workspace
    # but need to check input and output path is valid
    config = None
    if cml_mode:
        if not input_path or not output_path:
            raise Exception("Please specify input and output path")

        config = DataConfig(cml_mode=True)
        config.set_container_name(container_name)
    else:
        ws_path = os.getcwd()
        config_path = os.path.join(ws_path, ".roscenes", "config.yaml")
        config = DataConfig(config_path=config_path)

    print("----------------------")
    print("----  record2bag  ----")
    print("----------------------")

    if cml_mode:
        record2bag_for_cml(
            config=config, input_path=input_path, output_path=output_path
        )
    else:
        record2bag_for_ws(ws_path=ws_path, config=config)


def record2bag_for_ws(ws_path, config: DataConfig):
    config = config

    # 1. 获取所有还没有生成bag文件的bug文件夹
    print(f"1. Get all bug folders which need to convert to bag files")
    print("  raw_bug_folder_root: ", config.raw_bug_folder_root)
    all_bug_folders, valid_bug_folders, invalid_bug_folders = get_bug_folders(
        config.raw_bug_folder_root
    )
    need_convert_folders = invalid_bug_folders
    # echo need_convert_folders
    print("Need convert folders:")
    for bug_folder in need_convert_folders:
        print(" ", bug_folder)

    # 2 遍历每一个`bug`文件夹中的`record`文件夹中的record文件,该文件包含`record`且size大于100MB
    # 拷贝 `~/repo_ws_dev/optimus/recorder2bag/conf/recorder2ros_config.pb.txt`文件
    # 至每一个`bug`文件夹中的`record2bag_conf`文件夹,
    # 假设该bug的文件夹路径是`$bug`,那么`record2bag_conf`文件夹路径是`$bug/record2bag_conf`,
    # 如果没有该文件夹则创建该文件夹,并修改该文件的名称为对应record文件的名称,
    # 假设该record文件名称为`$record_name.record`,则配置文件路径为`$bug/record2bag_conf/$record_name.pb.txt`,
    # 然后修改该conf文件的内容,具体修改规则如下
    # - `recorder_file_name` 对应的字段修改该record文件的路径,即 `$bug/record/$record_name.record`
    # - `bag_file_name` 对应的字段修改该record文件对应的rosbag文件的路径,这个rosbag的路径的文件夹是`bags`,文件名是`record`文件的名称,但是后缀是`.bag`,即 `$bug/bags/$record_name.bag`
    # - `sensor_param_path` 是相对该bug的文件,文件名为sensor_param.pb.txt , 需要进行搜索
    print(f"2. Generate recorder2ros config")
    for bug_folder in need_convert_folders:
        generate_recorder2ros_config(bug_folder)

    # 3 遍历所有路径,对每一个路径均执行如下操作
    # - 进入容器 `yczx_dev` 中,执行`source ~/repo_ws_dev/optimus-modules/setup.bash`命令
    # - 接下来根据该路径中的`record2bag_conf`文件夹中的配置文件的数量,
    # 针对每一个配置文件都依次调用 `~/repo_ws_dev/optimus-modules/bin/recorder2rosbag` 可执行文件,参数为配置文件
    print(f"3. Convert record , please wait...")
    print("Need convert folders:")
    for bug_folder in need_convert_folders:
        print(" ", bug_folder)
    for bug_folder in track(need_convert_folders):
        bug_name = os.path.basename(bug_folder)
        record2bag_conf_path = os.path.join(bug_folder, "record2bag_conf")
        config_files = []
        for root, dirs, files in os.walk(record2bag_conf_path):
            for file in files:
                if file.endswith(".pb.txt"):
                    config_files.append(os.path.join(root, file))

        convert_shell_path = os.path.join(bug_folder, "convert.sh")
        user = bug_folder.split("/")[2]
        generate_convert_shell(convert_shell_path, config_files, user)

        # enter container `yczx_dev` and execute `convert_shell_path`
        # check if the container `yczx_dev` is running
        cyber_container_name = config.cyber_container_name
        if is_container_running(cyber_container_name):
            cmd = f"docker exec -u {user} {cyber_container_name} bash {convert_shell_path}"
            subprocess.run(
                cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        else:
            raise Exception(
                cyber_container_name
                + " container is not running , please use commond `orun_dev` to start the container first"
            )

    # 4. 检查train中的bag的时间长度,用于scene的划分(防止一个场景中的bag文件过大)
    new_bug_folders = []
    print(f"4. Split scene , please wait...")
    min_bag_duration = config.min_bag_duration
    for bug_folder in need_convert_folders:
        split_train_scene = SplitScene(
            scene_path=bug_folder,
            min_bag_duration=min_bag_duration,
        )
        split_train_scene.split()

        # update new_bug_folders
        if split_train_scene.new_bug_folders:
            need_convert_folders.remove(bug_folder)
            new_bug_folders.extend(split_train_scene.new_bug_folders)

    need_convert_folders.extend(new_bug_folders)
    # filter repeat bug folders
    need_convert_folders = list(set(need_convert_folders))

    # 5 合并所有的bag文件,将其保存到`$bug`文件夹中,文件名为`$bug.bag`,同时检查该bag是否有效,如果无效则删除
    print("need_convert_folders: ", need_convert_folders)
    print(f"5. Merge bag files , please wait...")

    sorted_folders = sorted(need_convert_folders, key=extract_numbers)
    # print("sorted_folders: ", sorted_folders)
    # print("len(sorted_folders): ", len(sorted_folders))
    if config.car_brand == "yc200":
        for index, bug_folder in enumerate(sorted_folders[: len(sorted_folders) // 2]):
            input_path_list = []
            bug_name = os.path.basename(bug_folder)
            bag_path = os.path.join(bug_folder, "bags")
            for root, dirs, files in os.walk(bag_path):
                for file in files:
                    if file.endswith(".bag"):
                        input_path_list.append(os.path.join(root, file))
            output_path = os.path.join(bug_folder, f"{bug_name}.bag")
            # camera_name = os.path.basename(sorted_folders[index+len(sorted_folders)/2])
            if config.car_brand == "yc200":
                camera_path = os.path.join(
                    sorted_folders[index + len(sorted_folders) // 2], "bags"
                )
                for root, dirs, files in os.walk(camera_path):
                    for file in files:
                        if file.endswith(".bag"):
                            input_path_list.append(os.path.join(root, file))
            if len(input_path_list) == 0:
                continue
            print("bug_folder: ", bug_folder)
            print("output_path: ", output_path)
            print("input_path_list: ", input_path_list)
            merge_bag = MergeBag(
                input_path_list=input_path_list,
                compression="lz4",
                output=output_path,
            )
            merge_bag.run()
            # check if the bag file is valid by compare bag file size(MB)
            # compare with 1MB
            if os.path.getsize(output_path) < 1000000:
                os.system(f"rm -f {output_path}")
                # echo invalid info with red color
                print(f"\033[31mRemove invalid bag file {output_path}\033[0m")
        # if yc200 需要删除后//2的文件夹
        if config.car_brand == "yc200":
            for index, bug_folder in enumerate(
                sorted_folders[len(sorted_folders) // 2 :]
            ):
                os.system(f"rm -rf {bug_folder}")

    else:
        for index, bug_folder in enumerate(need_convert_folders):
            input_path_list = []
            bug_name = os.path.basename(bug_folder)
            bag_path = os.path.join(bug_folder, "bags")
            for root, dirs, files in os.walk(bag_path):
                for file in files:
                    if file.endswith(".bag"):
                        input_path_list.append(os.path.join(root, file))
            output_path = os.path.join(bug_folder, f"{bug_name}.bag")
            # camera_name = os.path.basename(sorted_folders[index+len(sorted_folders)/2])
            if config.car_brand == "yc200":
                camera_path = os.path.join(
                    sorted_folders[index + len(sorted_folders) // 2], "bags"
                )
                for root, dirs, files in os.walk(camera_path):
                    for file in files:
                        if file.endswith(".bag"):
                            input_path_list.append(os.path.join(root, file))
            if len(input_path_list) == 0:
                continue
            print("bug_folder: ", bug_folder)
            print("output_path: ", output_path)
            print("input_path_list: ", input_path_list)
            merge_bag = MergeBag(
                input_path_list=input_path_list,
                compression="lz4",
                output=output_path,
            )
            merge_bag.run()
            # check if the bag file is valid by compare bag file size(MB)
            # compare with 1MB
            if os.path.getsize(output_path) < 1000000:
                os.system(f"rm -f {output_path}")
                # echo invalid info with red color
                print(f"\033[31mRemove invalid bag file {output_path}\033[0m")

    # 6. 筛选所有与当前workspace设置中匹配的车型数据
    # 并复制`$bug.bag`文件到`$ws_path/train/bags`文件夹中
    print("6. Copy bag files to workspace, please wait...")
    # filter bags by car_brand
    car_brand = config.car_brand
    need_copy_bug_folder_list = []
    _, all_valid_bug_folders, _ = get_bug_folders(config.raw_bug_folder_root)
    for bug_folder in all_valid_bug_folders:
        bug_name = os.path.basename(bug_folder)
        # use lower case to compare
        if car_brand.lower() in bug_name.lower():
            need_copy_bug_folder_list.append(bug_folder)
    # echo need copy bug folders bug name
    print("Need copy bug folders:")
    for bug_folder in need_copy_bug_folder_list:
        print(os.path.basename(bug_folder))
    for bug_folder in track(need_copy_bug_folder_list):
        bug_name = os.path.basename(bug_folder)
        bag_path = os.path.join(bug_folder, f"{bug_name}.bag")
        copy_bag(ws_path=ws_path, bag_path=bag_path)


def record2bag_for_cml(config: DataConfig, input_path, output_path):
    config = config
    # 1. check input path and output path
    print(f"1. Check input path and output path")
    if not os.path.exists(input_path):
        raise Exception(f"Input path {input_path} not exist!")
    # output_path should end with .bag
    if not output_path.endswith(".bag"):
        raise Exception(f"Output path {output_path} should end with .bag")
    input_folder_name = os.path.basename(input_path)
    output_file_name = os.path.basename(output_path).split(".")[0]
    if input_folder_name != output_file_name:
        raise Exception(
            f"Input folder name {input_folder_name} should be same with output file name {output_file_name}"
        )
    # 2. check input path should be a folder and include `record` folder
    print(f"2. Check input path should be a folder and include record folder")
    record_folder_path = os.path.join(input_path, "record")
    if not os.path.exists(record_folder_path):
        raise Exception(f"Input path {input_path} should include `record` folder")

    # 3. generate recorder2ros config
    print(f"3. Generate recorder2ros config")
    generate_recorder2ros_config(input_path)

    # 4. convert record to bag
    print(f"4. Convert record , please wait...")
    record2bag_conf_path = os.path.join(input_path, "record2bag_conf")
    config_files = []
    for root, dirs, files in os.walk(record2bag_conf_path):
        for file in files:
            if file.endswith(".pb.txt"):
                config_files.append(os.path.join(root, file))

    convert_shell_path = os.path.join(input_path, "convert.sh")
    user = input_path.split("/")[2]
    generate_convert_shell(convert_shell_path, config_files, user)

    if in_docker():
        cmd = f"bash {convert_shell_path}"
        subprocess.run(
            cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    else:
        cyber_container_name = config.cyber_container_name

        if is_container_running(cyber_container_name):
            cmd = f"docker exec -u {user} {cyber_container_name} bash {convert_shell_path}"
            subprocess.run(
                cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        else:
            raise Exception(
                cyber_container_name
                + " container is not running , please use commond `orun_dev` to start the container first"
            )

    # 5 合并所有的bag文件,将其保存到`$bug`文件夹中,文件名为`$bug.bag`,同时检查该bag是否有效,如果无效则删除
    print(f"5. Merge bag files , please wait...")
    bug_name = os.path.basename(input_path)
    bag_path = os.path.join(input_path, "bags")
    input_path_list = []
    for root, dirs, files in os.walk(bag_path):
        for file in files:
            if file.endswith(".bag"):
                input_path_list.append(os.path.join(root, file))
    merged_bag_path = os.path.join(input_path, f"{bug_name}.bag")
    if len(input_path_list) == 0:
        return
    input_path_list = sorted(input_path_list, key=get_sort_key)
    print("input_path_list", input_path_list)
    merge_bag = MergeBag(
        input_path_list=input_path_list,
        compression="lz4",
        output=merged_bag_path,
    )
    merge_bag.run()
    # check if the bag file is valid by compare bag file size(MB)
    # compare with 1MB
    if os.path.getsize(merged_bag_path) < 1000000:
        os.system(f"rm -f {merged_bag_path}")
        # echo invalid info with red color
        print(f"\033[31mRemove invalid bag file {merged_bag_path}\033[0m")

    # 6. cp to target path
    # 并复制`$bug.bag`文件到`$ws_path/train/bags`文件夹中
    print("6. Copy bag files to target path, please wait...")
    os.system(f"cp {merged_bag_path} {output_path}")


def generate_recorder2ros_config(bug_path):
    # 1. check if `record2bag_conf` folder exist
    record2bag_conf_path = os.path.join(bug_path, "record2bag_conf")
    if not os.path.exists(record2bag_conf_path):
        os.mkdir(record2bag_conf_path)
    # clear all files in `record2bag_conf` folder
    os.system(f"rm -rf {record2bag_conf_path}/*")

    # get config raw file path
    config_raw_file_path = (
        "~/repo_ws_dev/optimus/recorder2bag/conf/recorder2ros_config.pb.txt"
    )
    config_raw_file_path = os.path.expanduser(config_raw_file_path)

    # get all record files
    record_files = []
    record_path = os.path.join(bug_path, "record")
    for root, dirs, files in os.walk(record_path):
        for file in files:
            # judge file if vaiid by file size and filename
            # - file size > 100MB
            # - file name include `record`
            if (
                os.path.getsize(os.path.join(root, file)) > 100000000
                and "record" in file
            ):
                record_files.append(os.path.join(root, file))

    # copy config file to record2bag_conf folder
    for record_file in record_files:
        record_name = os.path.basename(record_file)
        # record_name = record_name.split(".")[0]
        # record_name = record_name[:-7]
        config_file_path = os.path.join(record2bag_conf_path, f"{record_name}.pb.txt")
        os.system(f"cp {config_raw_file_path} {config_file_path}")

        # modify config file
        with open(config_file_path, "r") as f:
            lines = f.readlines()
        with open(config_file_path, "w") as f:
            for line in lines:
                if line.startswith("recorder_file_name"):
                    f.write(f'recorder_file_name: "{record_file}"\n')
                elif line.startswith("bag_file_name"):
                    bag_file_name = os.path.join(bug_path, "bags", f"{record_name}.bag")
                    # check if the folder `bags` exist
                    if not os.path.exists(os.path.join(bug_path, "bags")):
                        os.mkdir(os.path.join(bug_path, "bags"))
                    f.write(f'bag_file_name: "{bag_file_name}"\n')
                elif line.startswith("sensor_param_path"):
                    # find deep sensor_param.pb.txt file in conf folder
                    sensor_param_path = ""
                    for root, dirs, files in os.walk(os.path.join(bug_path, "conf")):
                        for file in files:
                            if file == "sensor_param.pb.txt":
                                sensor_param_path = os.path.join(root, file)
                                break
                        if sensor_param_path:
                            break

                    if not sensor_param_path:
                        raise Exception("Find sensor_param.pb.txt failed!")

                    f.write(f'sensor_param_path: "{sensor_param_path}"\n')
                else:
                    f.write(line)


def generate_convert_shell(convert_shell_path, config_files, user="root"):
    with open(convert_shell_path, "w") as f:
        f.write("#!/bin/bash\n")
        f.write(
            "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/ros/noetic/lib:/opt/third_party_libs/lib:/opt/third_party_libs/lib64\n"
        )
        f.write(
            "export PATH=$PATH:/opt/ros/noetic/bin:/opt/third_party_libs/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n"
        )
        f.write(f"source /opt/ros/noetic/setup.bash\n")
        f.write(f"source /home/{user}/repo_ws_dev/optimus-modules/setup.bash\n")
        for config_file in config_files:
            f.write(
                f"/home/{user}/repo_ws_dev/optimus-modules/bin/recorder2rosbag {config_file}\n"
            )
    os.system(f"chmod +x {convert_shell_path}")


def copy_bag(ws_path, bag_path):
    """将指定路径的bag文件拷贝到指定的workspace的raw/bags文件夹中,并且将bag文件的信息添加到`INFO.json`文件中

    如果bag文件已经存在,则不再拷贝

    Args:
        ws_path (_type_): 目标workspace的路径
        bag_path (_type_): 源bag文件的路径
    """
    # judge if the bag file exist
    if not os.path.exists(bag_path):
        return

    bug_name = os.path.basename(bag_path).split(".")[0]
    target_bag_path = os.path.join(ws_path, "raw", "bags", f"{bug_name}.bag")

    # check if the folder `raw/bags` exist
    if not os.path.exists(os.path.join(ws_path, "raw", "bags")):
        os.mkdir(os.path.join(ws_path, "raw", "bags"))

    # check if the bag file exist
    if os.path.exists(target_bag_path):
        # print(f"{target_bag_path} already exist!")
        # no need to print the info
        pass
    else:
        # copy bag file with show progress
        # os.system(f"rsync -ah --progress {bag_path} {target_bag_path}")
        # os.system(f"cp {bag_path} {target_bag_path}")
        cmd = f"cp {bag_path} {target_bag_path}"
        subprocess.run(
            cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

    # check if the bag file is copied successfully
    if not os.path.exists(target_bag_path):
        print(f"Copy {bag_path} to {target_bag_path} failed!")
        return

    # add bag info to INFO.json file
    add_bag_info(target_bag_path)

    return


def get_bug_folders(raw_bug_folder_root):
    """获取所有的bug文件夹

    Returns:
        list: 所有的bug文件夹,包括有效和无效的,按照名称排序,其中是否有效是通过判断是否已经生成了对应的bag文件
    """
    all_bug_folders = []
    # test_path = "~/repo_ws_dev/bug/lidar/test"
    # replace `~` with the real path
    raw_bug_folder_root = os.path.expanduser(raw_bug_folder_root)
    # test_path = os.path.expanduser(test_path)
    for root, dirs, files in os.walk(raw_bug_folder_root):
        for dir in dirs:
            all_bug_folders.append(os.path.join(root, dir))
        break
    valid_bug_folders = [
        bug_folder
        for bug_folder in all_bug_folders
        if os.path.exists(
            os.path.join(bug_folder, os.path.basename(bug_folder) + ".bag")
        )
    ]
    invalid_bug_folders = list(set(all_bug_folders) - set(valid_bug_folders))
    # sort bug_folders by name
    valid_bug_folders.sort()
    invalid_bug_folders.sort()

    return all_bug_folders, valid_bug_folders, invalid_bug_folders


def in_docker():
    try:
        with open("/proc/1/cgroup", "rt") as ifh:
            return "docker" in ifh.read() or "kubepods" in ifh.read()
    except Exception:
        return False
