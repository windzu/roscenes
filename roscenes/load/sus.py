import json
import os

import quaternion

from ..common.constant import FILTER_RULES, SUSToNuscenesMap
from ..nuscenes.annotation import InstanceTable, SampleAnnotationTable
from ..nuscenes.nuscenes_objects import NuscenesObject
from ..nuscenes.rule import parse_filename
from ..nuscenes.utils import (
    generate_instance_info_list,
    generate_sample_annotation_info_list,
)
from .utils import (
    get_nuscenes_attribute_name_list,
    get_nuscenes_category_name_list,
    get_nuscenes_visibility_list,
)


class LoadFromSUS:
    """将 sus 格式的标注结果中导入到 nuscenes 数据库中
    共导入如下几个文件：
    - instance.json
    - sample_annotation.json

    """

    def __init__(
        self,
        sus_path: str,
        nuscenes_path: str,
        filter_enabled: bool = False,
    ):
        self.sus_path = sus_path
        self.nuscenes_path = nuscenes_path
        self.filter_enabled = filter_enabled

        self.nuscenes_category_name_list = get_nuscenes_category_name_list()
        self.nuscenes_attribute_name_list = get_nuscenes_attribute_name_list()
        self.nuscenes_visibility_list = get_nuscenes_visibility_list()

    def load(self):
        # 1. sus_path valid check
        if not os.path.exists(self.sus_path):
            raise FileNotFoundError(f"{self.sus_path} not exists")
        # check sus_path if have label folder and have json file
        label_path = os.path.join(self.sus_path, "label")
        if not os.path.exists(label_path):
            raise FileNotFoundError(f"{label_path} not exists")
        json_file_list = [
            json_file
            for json_file in os.listdir(label_path)
            if json_file.endswith(".json")
        ]
        if len(json_file_list) == 0:
            raise FileNotFoundError(f"{label_path} not have json file")

        # 2. nuscenes_path valid check
        if not os.path.exists(self.nuscenes_path):
            raise FileNotFoundError(f"{self.nuscenes_path} not exists")

        # 3. load from sus
        self.load_from_sus(self.sus_path, self.nuscenes_path)

    def load_from_sus(self, source_path, target_path):
        """load from sus to nuscenes

        Args:
            source_path (str): sus path
            target_path (str): nuscenes path

        Raises:
            ValueError: _description_
            ValueError: _description_
            ValueError: _description_
        """

        # 1. 获取 ${source_path}/label 下的标注结果 后缀为 .json
        result_path = os.path.join(source_path, "label")
        label_file_list = [
            os.path.join(result_path, json_file)
            for json_file in os.listdir(result_path)
            if json_file.endswith(".json")
        ]

        # 2. 解析所有标注文件并创建该 scenes 下的所有帧的 nuscenes_object_list
        nuscenes_object_list = []
        for label_file in label_file_list:
            nuscenes_object_list.extend(self.parse_label_file(label_file))

        # 3. double check
        # Note : 为了防止 category_name 和 attribute 是错误的
        # 从而导致生成的 token 也是错的 , 所以这里再做一次检查
        invalid_object_index = []
        for i, object in enumerate(nuscenes_object_list):
            # check category
            if object.category not in self.nuscenes_category_name_list:
                raise ValueError(
                    f"Error : {object.category} not in nuscenes_category_name_list, scene_name : {object.scene_name}, timestamp : {object.timestamp}"
                )

            # check attribute
            for attribute_name in object.attribute_name_list:
                if attribute_name not in self.nuscenes_attribute_name_list:
                    raise ValueError(
                        f"Error : {attribute_name} not in nuscenes_attribute_name_list, scene_name : {object.scene_name}, timestamp : {object.timestamp}"
                    )

            # check visibility
            if object.visibility not in self.nuscenes_visibility_list:
                raise ValueError(
                    f"Error : {object.visibility} not in nuscenes_visibility_list, scene_name : {object.scene_name}, timestamp : {object.timestamp}"
                )

        # 4. 导出为 nuscenes database
        # 4.1 transform nuscenes_object_list to global coordinate
        # 将 base_link 坐标系下的坐标目标转换到 map 坐标系下
        # load ego_pose.json to ego_pose_dict
        ego_pose_json_file = os.path.join(
            target_path,
            "v1.0-all",
            "ego_pose.json",
        )
        ego_pose_dict = self.load_ego_pose_dict(ego_pose_json_file)

        for nuscenes_object in nuscenes_object_list:
            ego_pose = ego_pose_dict[str(nuscenes_object.timestamp)]
            global_rotation = ego_pose["rotation"]
            global_translation = ego_pose["translation"]
            nuscenes_object.transform_to_global(global_rotation, global_translation)

        # 4.2 save nuscenes_object_list to nuscenes database
        # 将结果保存到 nuscenes database
        save_path = os.path.join(target_path, "v1.0-all")

        # - save instance.json
        instance_info_list, _ = generate_instance_info_list(nuscenes_object_list)
        instance_table = InstanceTable(instance_info_list)
        instance_table.sequence_to_json(save_path, "instance.json")

        # - save sample_annotation.json
        sample_annotation_info_list = generate_sample_annotation_info_list(
            nuscenes_object_list
        )
        sample_annotation_table = SampleAnnotationTable(sample_annotation_info_list)
        sample_annotation_table.sequence_to_json(save_path, "sample_annotation.json")

    @staticmethod
    def load_ego_pose_dict(ego_pose_json_file):
        with open(ego_pose_json_file, "r") as f:
            ego_pose_json = json.load(f)

        ego_pose_dict = {}

        for ego_pose in ego_pose_json:
            ego_pose_dict[str(ego_pose["timestamp"])] = ego_pose
        return ego_pose_dict

    def parse_label_file(self, file_path):
        """parse sus label file

        Args:
            file_path (str): sus label file path

        Raises:
            Exception: _description_
            ValueError: _description_

        Returns:
            list: _description_
        """

        filename = file_path.split("/")[-1]

        object_list = []
        try:
            scene_name, channel, timestamp, fileformat = parse_filename(filename)
        except:
            raise Exception("filename : {} is not valid.".format(file_path))

        # read annotation
        with open(file_path, "r") as f:
            label_file_json = json.load(f)

        # check if label_file_json is empty
        if len(label_file_json) == 0:
            return object_list

        sus_objects = label_file_json

        nuscenes_object_info_list = []
        for i, sus_object in enumerate(sus_objects):
            object_id = i  # sus_object 没有id 所以用索引代替
            track_id = sus_object["obj_id"]
            obj_type = None
            if "obj_type" in sus_object:
                obj_type = sus_object["obj_type"]
            else:
                raise ValueError("obj_type not in sus_object")

            category_name = SUSToNuscenesMap.get_category_name_by_obj_type(obj_type)

            # 如果没有匹配的 category 则跳过
            if not category_name:
                # print(f"Error : {obj_type} not in SUSToNuscenesMap")
                continue

            # 3d bbox position in local coordinate
            translation = [
                sus_object["psr"]["position"]["x"],
                sus_object["psr"]["position"]["y"],
                sus_object["psr"]["position"]["z"],
            ]

            # 3d bbox size (l,w,h)
            size = [
                sus_object["psr"]["scale"]["x"],
                sus_object["psr"]["scale"]["y"],
                sus_object["psr"]["scale"]["z"],
            ]

            # rotation
            rotation_x = sus_object["psr"]["rotation"]["x"]
            rotation_y = sus_object["psr"]["rotation"]["y"]
            rotation_z = sus_object["psr"]["rotation"]["z"]
            # convert to quaternion
            rotation = quaternion.from_euler_angles(rotation_x, rotation_y, rotation_z)
            rotation = [rotation.w, rotation.x, rotation.y, rotation.z]

            # num_lidar_pts
            num_lidar_pts = sus_object["num_lidar_pts"]

            if self.if_filter(obj_type, size, num_lidar_pts):
                continue  # 跳过此物体，不添加到结果中

            attribute_name_list = (
                SUSToNuscenesMap.get_attribute_name_list_by_category_name(category_name)
            )
            visibility = "v80-100"

            nuscenes_object_info = {
                "scene_name": scene_name,
                "timestamp": timestamp,
                "object_id": object_id,
                "track_id": track_id,
                "category": category_name,
                "translation": translation,
                "rotation": rotation,
                "size": [size[1], size[0], size[2]],  # Note : nuscenes use  [w,l,h]
                "num_lidar_pts": num_lidar_pts,
                "visibility": visibility,
                "attribute_name_list": attribute_name_list,
            }
            nuscenes_object_info_list.append(nuscenes_object_info)

        # create nuscenes_object_list
        for nuscenes_object_info in nuscenes_object_info_list:
            nuscenes_object = NuscenesObject(
                scene_name=nuscenes_object_info["scene_name"],
                timestamp=nuscenes_object_info["timestamp"],
                object_id=nuscenes_object_info["object_id"],
                track_id=nuscenes_object_info["track_id"],
                category=nuscenes_object_info["category"],
                translation=nuscenes_object_info["translation"],
                rotation=nuscenes_object_info["rotation"],
                size=nuscenes_object_info["size"],
                visibility=nuscenes_object_info["visibility"],
                attribute_name_list=nuscenes_object_info["attribute_name_list"],
                num_lidar_pts=nuscenes_object_info["num_lidar_pts"],
            )
            object_list.append(nuscenes_object)

        return object_list

    def if_filter(self, obj_type, size, num_lidar_pts):
        """
        根据物体类型、尺寸和激光雷达点数进行过滤

        Args:
            obj_type (str): 物体类型
            size (list): 物体尺寸 [w, l, h]
            num_lidar_pts (int): 物体上的激光雷达点数

        Returns:
            bool: True 表示应该过滤掉该物体，False 表示保留该物体
        """
        # 如果过滤功能未启用，直接返回 False（不过滤）
        if not self.filter_enabled:
            return False

        # 如果物体类型不在过滤规则中，不过滤
        if obj_type not in FILTER_RULES:
            return False

        # 获取该类型物体的过滤规则
        rule = FILTER_RULES[obj_type]

        # 检查物体的尺寸是否小于最小尺寸
        if "min_size" in rule:
            min_size = rule["min_size"]
            # 检查物体的三个维度是否都大于等于最小尺寸
            if size[0] < min_size[0] or size[1] < min_size[1] or size[2] < min_size[2]:
                print(
                    f"Filtering out {obj_type} with size {size}, "
                    f"min size {min_size}"
                )
                return True  # 过滤掉

        # 检查物体上的激光雷达点数是否小于最小点数
        if "min_points" in rule and num_lidar_pts < rule["min_points"]:
            print(
                f"Filtering out {obj_type} with num_lidar_pts {num_lidar_pts}, "
                f"min points {rule['min_points']}"
            )
            return True  # 过滤掉

        # 通过所有过滤条件，不过滤
        return False
