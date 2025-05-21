import json
import os

from ..common.data_config import DataConfig
from ..common.utils import add_bag_info
from ..nuscenes.nuscenes_info import NuscenesInfo


class DataInfo:
    """数据集的信息
    car_info_list : 所有车辆的信息列表
    """

    def __init__(self, data_config: DataConfig, data_path):
        self.data_config = data_config
        self.data_path = data_path

        # get train scene bag file list
        self.scene_bag_file_list = self.get_scene_bag_file_list()
        self.scene_bag_info_dict = self.get_scene_bag_info_dict()

        # check scene_bag_file_list and scene_bag_info_dict
        self.check_scene_bag_file_list_and_info_dict()

        self.scene_info_list = self.scene_info_list_init()

    def get_scene_bag_file_list(self):
        bags_path = os.path.join(self.data_path, "bags")
        bag_suffix = self.data_config.data_suffix
        scene_bag_file_list = []
        for dirpath, _, filenames in os.walk(bags_path):
            for file in filenames:
                if file.endswith(bag_suffix):
                    scene_bag_file_list.append(os.path.join(dirpath, file))
        return scene_bag_file_list

    def get_scene_bag_info_dict(self):
        scene_bag_info_dict = {}
        info_path = os.path.join(self.data_path, "bags", "INFO.json")

        if len(self.scene_bag_file_list) > 0:
            # check if the bag file exists
            for bag_file in self.scene_bag_file_list:
                add_bag_info(bag_file)
        else:
            return {}

        with open(info_path, "r") as f:
            scene_bag_info_list = json.load(f)
        for info in scene_bag_info_list:
            scene_bag_info_dict[info["scene_name"]] = info
        return scene_bag_info_dict

    def check_scene_bag_file_list_and_info_dict(self):
        """bag文件必须有info描述信息"""
        scene_bag_file_list = self.get_scene_bag_file_list()
        scene_bag_info_dict = self.get_scene_bag_info_dict()
        # scene_bag_file_list and scene_bag_info_dict should have same "keys"
        for bag_file_path in scene_bag_file_list:
            bag_filename = os.path.basename(bag_file_path)
            scene_name = bag_filename.split(".")[0]
            assert scene_name in scene_bag_info_dict.keys()

    def scene_info_list_init(self):
        """初始化场景信息列表"""
        # iter scene_bag_file_list
        scene_info_list = []
        for scene_bag_file in self.scene_bag_file_list:
            bag_filename = os.path.basename(scene_bag_file)
            car_id, scene_id = bag_filename.split(".")[0].split("_")
            # calib_info_dict = self.data_config.cars_calib_info_dict[car_id]
            scene_name = str(car_id) + "_" + str(scene_id)
            save_root = os.path.join(self.data_path, "frames")

            info = self.scene_bag_info_dict[scene_name]
            map_name = info["map_name"]
            description = info["description"]
            date_captured = info["date_captured"]

            scene_info = NuscenesInfo(
                data_config=self.data_config,
                scene_name=scene_name,
                map_name=map_name,
                date_captured=date_captured,
                description=description,
                scene_bag_file=scene_bag_file,
                save_root=save_root,
            )
            scene_info_list.append(scene_info)

        return scene_info_list


class SceneInfo:
    """scene信息"""

    def __init__(
        self,
        bag_path: str,
        bag_info_dict: dict,
        config: DataConfig,
    ):
        # self.data_config = data_config
        self.bag_path = bag_path
        self.bag_info_dict = bag_info_dict
        self.config = config

    def get_nuscene_info(self):
        """获取nuscenes场景信息"""
        bag_filename = os.path.basename(self.bag_path)
        scene_id, car_id = bag_filename.split(".")[0].split("_")
        scene_name = str(scene_id) + "_" + str(car_id)

        # get frames_folder_path
        frames_folder_path = None
        if not self.config.cml_mode:
            bags_folder_path = os.path.dirname(self.bag_path)
            root_path = os.path.dirname(bags_folder_path)
            frames_folder_path = os.path.join(root_path, "frames")
        else:
            frames_folder_path = self.config.output_path

        # get scene_bag_info_dict
        if not self.bag_info_dict:
            map_name = "suzhou"
            description = "lidar data"
            date_captured = "1970-01-01"
        else:
            # check if bag_info_dict has the key : map_name, description, date_captured
            map_name = self.bag_info_dict["map_name"]
            description = self.scene_bag_info["description"]
            date_captured = self.scene_bag_info["date_captured"]

        nuscene_info = NuscenesInfo(
            data_config=self.config,
            scene_name=scene_name,
            map_name=map_name,
            date_captured=date_captured,
            description=description,
            scene_bag_file=self.bag_path,
            save_root=frames_folder_path,
        )
        return nuscene_info

    @staticmethod
    def get_scene_bag_info_dict(bag_path):
        scene_bag_info_dict = {}
        bags_folder = os.path.dirname(bag_path)
        info_path = os.path.join(bags_folder, "INFO.json")

        # check if the info file exists
        if not os.path.exists(info_path):
            return {}

        with open(info_path, "r") as f:
            scene_bag_info_list = json.load(f)
        for info in scene_bag_info_list:
            scene_bag_info_dict[info["scene_name"]] = info
        return scene_bag_info_dict
