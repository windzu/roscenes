import os
import json
from nuscenes.nuscenes import NuScenes


def get_available_scenes(nuscene_path, data_version):
    """Get available scenes from the input nuScenes dataset.

    Args:
        nuscene_path (str): Path to the root folder of the nuScenes dataset.
        data_version (str): Version of the nuScenes dataset. Should be
            "v1.0-trainval" or "v1.0-test".

    Returns:
        available_scenes (list[dict]): List of basic information for the
            available scenes.
    """
    # data_version should be "v1.0-trainval" or "v1.0-test"
    if data_version not in ["v1.0-trainval", "v1.0-test"]:
        raise Exception("data_version should be 'v1.0-trainval' or 'v1.0-test'")

    # nuscene_path should be valid path
    target_path = os.path.join(nuscene_path, data_version)
    if os.path.exists(target_path):
        # check if target_path folder have files
        if len(os.listdir(target_path)) == 0:
            print(
                "get_available_scenes success: "
                + target_path
                + " is a valid path, but have no files."
            )
            return []
    else:
        print(
            "get_available_scenes success: "
            + target_path
            + " is not a valid path,will create it."
        )
        os.makedirs(target_path)
        return []

    # count dataset info
    # 统计数据集信息
    # different_category_size_dict = {"car": 0, "truck": 0, "bus": 0, "trailer": 0}
    nusc = NuScenes(version=data_version, dataroot=nuscene_path, verbose=True)
    available_scenes = []
    # Dictionary to store counts of each category

    for scene in nusc.scene:
        scene_info = {
            "name": None,
            "num_samples": None,
            "category_counts": {},
        }
        category_counts = {}

        scene_token = scene["token"]
        scene_rec = nusc.get("scene", scene_token)
        sample_rec = nusc.get("sample", scene_rec["first_sample_token"])

        # Count the number of samples in the scene
        sample_token = scene_rec["first_sample_token"]
        num_samples = 0
        while sample_token:
            sample_rec = nusc.get("sample", sample_token)
            num_samples += 1

            # Count the number of annotations of each category in the sample
            for ann_token in sample_rec["anns"]:
                ann_rec = nusc.get("sample_annotation", ann_token)
                category = ann_rec["category_name"]
                if category in category_counts:
                    category_counts[category] += 1
                else:
                    category_counts[category] = 1

            sample_token = sample_rec["next"]

        scene_info["name"] = scene["name"]
        scene_info["num_samples"] = num_samples
        scene_info["category_counts"] = category_counts

        available_scenes.append(scene_info)

    # sort scenes by name
    available_scenes = sorted(available_scenes, key=lambda x: x["name"])
    return available_scenes


def get_all_scene_name(nuscenes_path):
    # 1. check nuscenes_path
    if not os.path.exists(nuscenes_path):
        raise Exception("nuscenes_path should be valid path")

    # 2. echo nuscenes info
    root_path = nuscenes_path

    # echo v1.0-trainval info
    trainval_available_scenes = get_available_scenes(root_path, "v1.0-trainval")
    print("v1.0-trainval info :")
    print(" available scene num: {}".format(len(trainval_available_scenes)))
    print(" available scene:")
    for scene in trainval_available_scenes:
        # echo scene info
        print("     " + scene["name"] + " " + str(scene["num_samples"]))

    # echo v1.0-test info
    test_available_scenes = get_available_scenes(root_path, "v1.0-test")
    print("v1.0-test info :")
    print(" available scene num: {}".format(len(test_available_scenes)))
    print(" available scene:")
    for scene in test_available_scenes:
        print("     " + scene["name"] + " " + str(scene["num_samples"]))

    # get trainval and test available scene name list
    trainval_scene_name_list = []
    test_scene_name_list = []
    trainval_scene_name_list = [scene["name"] for scene in trainval_available_scenes]
    test_scene_name_list = [scene["name"] for scene in test_available_scenes]

    return trainval_scene_name_list, test_scene_name_list


def get_scene_name_list_by_car_brand(scene_name_list, car_brand):
    car_brand_lower = car_brand.lower()
    car_brand_upper = car_brand.upper()
    scene_name_list_by_car_brand = []
    for scene_name in scene_name_list:
        raw_car_name = scene_name.split("_")[1]
        if car_brand_lower in raw_car_name or car_brand_upper in raw_car_name:
            scene_name_list_by_car_brand.append(scene_name)
    return scene_name_list_by_car_brand


def get_nuscenes_api_path(conda_env_name):
    # 3.2. make sure target conda env if exist nuscenes api
    # example :
    # - user : wind
    # - conda : miniconda
    # - conda env name : mmdet3d-centerpoint
    # - python version : 3.8
    # - nuscenes install path : /home/wind/miniconda3/envs/mmdet3d-centerpoint/lib/python3.8/site-packages/nuscenes
    # 3.2.1 get user name
    user_name = os.popen("whoami").read().strip()
    # 3.2.2 get conda_env_path
    conda_env_path = ""
    miniconda_env_path = os.path.join(
        "/home", user_name, "miniconda3", "envs", conda_env_name
    )
    anaconda_env_path = os.path.join(
        "/home", user_name, "anaconda3", "envs", conda_env_name
    )
    # check which conda env path is valid
    if os.path.exists(miniconda_env_path):
        conda_env_path = miniconda_env_path
    elif os.path.exists(anaconda_env_path):
        conda_env_path = anaconda_env_path
    else:
        raise Exception(
            "conda env path not exist, please check if conda env name is correct"
        )
    # 3.2.3 get python version
    # ls conda_env_path/lib and get python version which dir name start with python3
    python_version = ""
    for root, dirs, files in os.walk(os.path.join(conda_env_path, "lib")):
        for dir in dirs:
            if dir.startswith("python3"):
                python_version = dir
    if python_version == "":
        raise Exception("python version not exist, please check if python is installed")
    # 3.2.4 get nuscenes api path
    nuscenes_api_path = os.path.join(
        conda_env_path, "lib", python_version, "site-packages", "nuscenes"
    )

    # 3.2.5 check which conda env path is valid
    if not os.path.exists(nuscenes_api_path):
        # raise error and echo nuscenes api path
        print("nuscenes_api_path: {}".format(nuscenes_api_path))
        raise Exception(
            "nuscenes api path not exist, please check if nuscenes api path is correct"
        )

    return nuscenes_api_path


def get_real_available_scenes(available_scenes):
    # combine scene info by scene name
    available_scenes_dict = {}
    for scene in available_scenes:
        scene_name = scene["name"]
        # split scene name by "_"
        scene_id = scene_name.split("_")[0]
        car_id = scene_name.split("_")[1]
        real_scene_id = scene_id.split("-")[0]
        real_scene_name = real_scene_id + "_" + car_id
        if real_scene_name not in available_scenes_dict:
            available_scenes_dict[real_scene_name] = scene
            # change scene name to real scene name
            scene["name"] = real_scene_name
        else:
            available_scenes_dict[real_scene_name]["num_samples"] += scene[
                "num_samples"
            ]
            category_counts = scene["category_counts"]
            for category in category_counts:
                if (
                    category
                    in available_scenes_dict[real_scene_name]["category_counts"]
                ):
                    available_scenes_dict[real_scene_name]["category_counts"][
                        category
                    ] += category_counts[category]
                else:
                    available_scenes_dict[real_scene_name]["category_counts"][
                        category
                    ] = category_counts[category]

    # regen trainval_available_scenes
    available_scenes = list(available_scenes_dict.values())
    return available_scenes


class LabelSummary:
    def __init__(self, label_format):
        self.label_format = label_format

        self.ws_path = os.getcwd()

        self.label_folder_name = None
        if self.label_format == "sus":
            self.label_folder_name = "label"
        else:
            raise Exception("label_format not support")

        self.lidar_folder_name = None
        if self.label_format == "sus":
            self.lidar_folder_name = "lidar"
        else:
            raise Exception("label_format not support")

        self.scene_anno_dict = {}

    def summary(self):
        # 1. get all get_all_valid_scene_folder_path_list
        self.get_all_valid_scene_folder_path_list = (
            self.get_all_valid_scene_folder_path_list()
        )
        # 2. iter all scene folder and get all annos
        self.get_all_annos()

        return self.scene_anno_dict

    def get_all_valid_scene_folder_path_list(self):
        valid_scene_folder_path_list = []

        # 1. get all need label scene file folder path
        train_scene_folder_path = os.path.join(self.ws_path, "train", self.label_format)
        test_scene_folder_path = os.path.join(self.ws_path, "test", self.label_format)

        # 2. check folder path valid and get all scene folder path
        if os.path.exists(train_scene_folder_path):
            # try to find all scene folder
            for scene_folder in os.listdir(train_scene_folder_path):
                scene_folder_path = os.path.join(train_scene_folder_path, scene_folder)
                if os.path.isdir(scene_folder_path):
                    valid_scene_folder_path_list.append(scene_folder_path)
        if os.path.exists(test_scene_folder_path):
            # try to find all scene folder
            for scene_folder in os.listdir(test_scene_folder_path):
                scene_folder_path = os.path.join(test_scene_folder_path, scene_folder)
                if os.path.isdir(scene_folder_path):
                    valid_scene_folder_path_list.append(scene_folder_path)

        # 3. get valid scene folder path list by check if have not empty label file
        for scene_folder_path in valid_scene_folder_path_list:
            label_file_path = os.path.join(scene_folder_path, self.label_folder_name)
            if os.path.exists(label_file_path):
                if len(os.listdir(label_file_path)) > 0:
                    continue
            valid_scene_folder_path_list.remove(scene_folder_path)

        return valid_scene_folder_path_list

    def get_all_annos(self):
        for scene_folder_path in self.get_all_valid_scene_folder_path_list:

            lidar_folder_path = os.path.join(scene_folder_path, self.lidar_folder_name)
            label_folder_path = os.path.join(scene_folder_path, self.label_folder_name)

            # get scene name
            scene_full_name = os.path.basename(scene_folder_path)
            scene_id = scene_full_name.split("_")[0].split("-")[0]
            if scene_id not in self.scene_anno_dict:
                self.scene_anno_dict[scene_id] = {
                    "file_num": 0,
                    "annos_num": 0,
                }

            # get lidar file num
            lidar_file_num = len(os.listdir(lidar_folder_path))
            if lidar_file_num == 0:
                continue
            self.scene_anno_dict[scene_id]["file_num"] += lidar_file_num

            # get all annos
            # label format json file which content is list of dict
            # the annos is list of dict
            for label_file_name in os.listdir(label_folder_path):
                label_file_path = os.path.join(label_folder_path, label_file_name)
                with open(label_file_path, "r") as f:
                    annos = json.load(f)
                    annos_num = len(annos)
                    self.scene_anno_dict[scene_id]["annos_num"] += annos_num


def echo_nuscenes_info(args, unknown):
    ws_path = os.getcwd()

    print("----------------------")
    print("----     info     ----")
    print("----------------------")

    nuscenes_path = os.path.join(ws_path, "nuscenes")
    trainval_available_scenes = get_available_scenes(nuscenes_path, "v1.0-trainval")
    trainval_available_scenes = get_real_available_scenes(trainval_available_scenes)

    print("\n\n")
    print("===========================================================================")
    print("v1.0-trainval info")
    print("===========================================================================")
    title_list = ["scene_name", "num_samples", "num_annos"]
    space_size = 30
    title_str = "".join(f"{title:<{space_size}}" for title in title_list)
    print(title_str)
    print("-" * (len(title_str)))
    trainval_available_scenes_info_dict = {}
    for scene in trainval_available_scenes:
        scene_name = str(scene["name"])
        num_samples = str(scene["num_samples"])
        category_counts = scene["category_counts"]
        num_annos = sum(category_counts.values())
        if scene_name not in trainval_available_scenes_info_dict:
            trainval_available_scenes_info_dict[scene_name] = {}
            trainval_available_scenes_info_dict[scene_name]["num_samples"] = num_samples
            trainval_available_scenes_info_dict[scene_name]["num_annos"] = num_annos
        else:
            raise Exception("scene_name should be unique")
    # sort scene by scene name
    trainval_available_scenes_info_dict = dict(
        sorted(trainval_available_scenes_info_dict.items())
    )
    for scene_name in trainval_available_scenes_info_dict:
        num_samples = trainval_available_scenes_info_dict[scene_name]["num_samples"]
        num_annos = trainval_available_scenes_info_dict[scene_name]["num_annos"]
        print(
            f"{scene_name:<{space_size}}{num_samples:<{space_size}}{num_annos:<{space_size}}"
        )

    # echo v1.0-test info
    test_available_scenes = get_available_scenes(nuscenes_path, "v1.0-test")
    test_available_scenes = get_real_available_scenes(test_available_scenes)
    print("\n\n")
    print("===========================================================================")
    print("v1.0-test info")
    print("===========================================================================")
    title_list = ["scene_name", "num_samples", "num_annos"]
    space_size = 30
    title_str = "".join(f"{title:<{space_size}}" for title in title_list)
    print(title_str)
    print("-" * (len(title_str)))
    test_available_scenes_info_dict = {}
    for scene in test_available_scenes:
        scene_name = str(scene["name"])
        num_samples = str(scene["num_samples"])
        category_counts = scene["category_counts"]
        num_annos = sum(category_counts.values())
        if scene_name not in test_available_scenes_info_dict:
            test_available_scenes_info_dict[scene_name] = {}
            test_available_scenes_info_dict[scene_name]["num_samples"] = num_samples
            test_available_scenes_info_dict[scene_name]["num_annos"] = num_annos
        else:
            raise Exception("scene_name should be unique")
    # sort scene by scene name
    test_available_scenes_info_dict = dict(
        sorted(test_available_scenes_info_dict.items())
    )
    for scene_name in test_available_scenes_info_dict:
        num_samples = test_available_scenes_info_dict[scene_name]["num_samples"]
        num_annos = test_available_scenes_info_dict[scene_name]["num_annos"]
        print(
            f"{scene_name:<{space_size}}{num_samples:<{space_size}}{num_annos:<{space_size}}"
        )

    # count total samples and annos
    total_samples = 0
    total_annos = 0
    total_available_scenes = {}
    for scene in trainval_available_scenes:
        total_samples += scene["num_samples"]
        total_annos += sum(scene["category_counts"].values())
        scene_name = scene["name"]
        if scene_name not in total_available_scenes:
            total_available_scenes[scene_name] = {}
            total_available_scenes[scene_name]["num_samples"] = scene["num_samples"]
            total_available_scenes[scene_name]["num_annos"] = sum(
                scene["category_counts"].values()
            )
        else:
            total_available_scenes[scene_name]["num_samples"] += scene["num_samples"]
            total_available_scenes[scene_name]["num_annos"] += sum(
                scene["category_counts"].values()
            )
    for scene in test_available_scenes:
        total_samples += scene["num_samples"]
        total_annos += sum(scene["category_counts"].values())
        scene_name = scene["name"]
        if scene_name not in total_available_scenes:
            total_available_scenes[scene_name] = {}
            total_available_scenes[scene_name]["num_samples"] = scene["num_samples"]
            total_available_scenes[scene_name]["num_annos"] = sum(
                scene["category_counts"].values()
            )
        else:
            total_available_scenes[scene_name]["num_samples"] += scene["num_samples"]
            total_available_scenes[scene_name]["num_annos"] += sum(
                scene["category_counts"].values()
            )
    # sort scene by scene name
    total_available_scenes = dict(sorted(total_available_scenes.items()))
    print("\n\n")
    print("===========================================================================")
    print("total info")
    print("===========================================================================")
    title_list = ["scene_name", "num_samples", "num_annos"]
    space_size = 30
    title_str = "".join(f"{title:<{space_size}}" for title in title_list)
    print(title_str)
    print("-" * (len(title_str)))
    for scene_name in total_available_scenes:
        num_samples = total_available_scenes[scene_name]["num_samples"]
        num_annos = total_available_scenes[scene_name]["num_annos"]
        print(
            f"{scene_name:<{space_size}}{num_samples:<{space_size}}{num_annos:<{space_size}}"
        )
    print("-" * (len(title_str)))
    print("total samples: {}".format(total_samples))
    print("total annos: {}".format(total_annos))

    # count all annos with category
    category_counts = {}
    for scene in trainval_available_scenes:
        for category in scene["category_counts"]:
            if category in category_counts:
                category_counts[category] += scene["category_counts"][category]
            else:
                category_counts[category] = scene["category_counts"][category]
    for scene in test_available_scenes:
        for category in scene["category_counts"]:
            if category in category_counts:
                category_counts[category] += scene["category_counts"][category]
            else:
                category_counts[category] = scene["category_counts"][category]
    print("\n\n")
    print("===========================================================================")
    print("category info")
    print("===========================================================================")
    title_list = ["category", "num_annos"]
    space_size = 25
    title_str = "".join(f"{title:<{space_size}}" for title in title_list)
    print(title_str)
    print("-" * (len(title_str)))
    for category in category_counts:
        num_annos = category_counts[category]
        print(f"{category:<{space_size}}{num_annos:<{space_size}}")

    print("\n\n")
    print("===========================================================================")
    print("lable summary info")
    print("===========================================================================")
    label_anno_dict = LabelSummary("sus").summary()
    title_list = ["scene_id", "file_num", "annos_num"]
    space_size = 25
    title_str = "".join(f"{title:<{space_size}}" for title in title_list)
    print(title_str)
    print("-" * (len(title_str)))
    for scene_id in label_anno_dict:
        file_num = label_anno_dict[scene_id]["file_num"]
        annos_num = label_anno_dict[scene_id]["annos_num"]
        print(
            f"{scene_id:<{space_size}}{file_num:<{space_size}}{annos_num:<{space_size}}"
        )
