import json
import os

import quaternion
from rich.progress import track

from ..common.utils import get_points_num


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

from ..common.constant import SUSToNuscenesMap


class LoadFromSUS:
    """将 sus 格式的标注结果中导入到 nuscenes 数据库中
    共导入如下几个文件：
    - instance.json
    - sample_annotation.json

    """

    def __init__(
        self,
        source_path_list,
        target_path_list,
        worker_num=4,
    ):
        self.source_path_list = source_path_list
        self.target_path_list = target_path_list
        self.worker_num = worker_num

        self.nuscenes_category_name_list = get_nuscenes_category_name_list()
        self.nuscenes_attribute_name_list = get_nuscenes_attribute_name_list()
        self.nuscenes_visibility_list = get_nuscenes_visibility_list()

        self.source_path_list, self.target_path_list = self.remove_invalid_path_list(
            self.source_path_list, self.target_path_list
        )

    def load(self):
        print(f"total {len(self.source_path_list)} scenes need to be load to nuscenes")

        args_list = list(zip(self.source_path_list, self.target_path_list))
        for source_path, target_path in track(args_list):
            self.load_from_sus(source_path, target_path)

        # tmp not use multiprocessing
        # wrapped_function = partial(self.load_from_sus_wrapper)
        # with multiprocessing.Pool(processes=self.worker_num) as pool:
        #     list(
        #         track(
        #             pool.imap_unordered(wrapped_function, enumerate(args_list)),
        #             total=len(args_list),
        #         )
        #     )

    def load_from_sus_wrapper(self, args):
        idx, (source_path, target_path) = args
        result = self.load_from_sus(source_path, target_path)
        return idx, result

    def load_from_sus(self, source_path, target_path):
        # 2. 获取 ${source_path}/label 下的标注结果 后缀为 .json
        result_path = os.path.join(source_path, "label")
        label_file_list = [
            os.path.join(result_path, json_file)
            for json_file in os.listdir(result_path)
            if json_file.endswith(".json")
        ]

        # 3. 解析所有标注文件并创建该 scenes 下的所有帧的 nuscenes_object_list
        nuscenes_object_list = []
        for label_file in label_file_list:
            # nuscenes_objects = NuscenesObjects(result_file)

            nuscenes_object_list.extend(self.parse_label_file(label_file))

        # Note : 为了防止 category_name 和 attribute 是错误的
        # 从而导致生成的 token 也是错的 , 所以这里再做一次检查
        invalid_object_index = []
        for i, object in enumerate(nuscenes_object_list):
            # check category
            if object.category not in self.nuscenes_category_name_list:
                print(
                    f"Error : {object.category} not in nuscenes_category_name_list, scene_name : {object.scene_name}, timestamp : {object.timestamp}"
                )
                invalid_object_index.append(i)
                continue

            # check attribute
            for attribute_name in object.attribute_name_list:
                if attribute_name not in self.nuscenes_attribute_name_list:
                    print(
                        f"Error : {attribute_name} not in nuscenes_attribute_name_list, scene_name : {object.scene_name}, timestamp : {object.timestamp}"
                    )
                    attribute_name = None
                    continue

            # check visibility
            if object.visibility not in self.nuscenes_visibility_list:
                print(
                    f"Error : {object.visibility} not in nuscenes_visibility_list, scene_name : {object.scene_name}, timestamp : {object.timestamp}"
                )
                object.visibility = "v0-40"
                continue

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

        for i, sus_object in enumerate(sus_objects):
            object_id = i  # sus_object 没有id 所以用索引代替
            track_id = sus_object["obj_id"]
            obj_type = None
            if "obj_type" in sus_object:
                obj_type = sus_object["obj_type"]
            else:
                raise ValueError("obj_type not in sus_object")

            try:
                category_name = SUSToNuscenesMap.get_category_name_by_obj_type(obj_type)
            except:
                print("obj type error:", obj_type, file_path)

            # 如果没有匹配的 category 则跳过
            if not category_name:
                print(f"Error : {obj_type} not in SUSToNuscenesMap")
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

            attribute_name_list = (
                SUSToNuscenesMap.get_attribute_name_list_by_category_name(category_name)
            )
            visibility = "v80-100"

            # get num_lidar_pts
            # sus 没有这个字段，所以需要计算
            pointcloud_file_path = file_path.replace("label", "lidar")
            pointcloud_file_path = pointcloud_file_path.replace(".json", ".pcd")

            num_lidar_pts = get_points_num(
                filepath=pointcloud_file_path,
                size=size,
                position=translation,
                rotation=rotation,
            )

            # check if num_lidar_pts is valid
            if num_lidar_pts == 0:
                print(
                    f"Error : num_lidar_pts is 0, scene_name : {scene_name}, timestamp : {timestamp}"
                )
                continue

            nuscenes_object = NuscenesObject(
                scene_name=scene_name,
                timestamp=timestamp,
                object_id=object_id,
                track_id=track_id,
                category=category_name,
                translation=translation,
                rotation=rotation,
                size=[size[1], size[0], size[2]],  # Note : nuscenes use  [w,l,h]
                visibility=visibility,
                attribute_name_list=attribute_name_list,
                num_lidar_pts=num_lidar_pts,
            )

            object_list.append(nuscenes_object)

        return object_list

    @staticmethod
    def remove_invalid_path_list(source_path_list, target_path_list):
        """滤除没有标注结果的场景"""

        target_source_path_list = []
        target_target_path_list = []
        no_result_scene_name_list = []
        for source_path, target_path in zip(source_path_list, target_path_list):
            # check if target_path exists
            if not os.path.exists(target_path):
                continue
            # check if source_path exists
            if not os.path.exists(source_path):
                continue

            # check if source_path/label exists and has json file
            source_label_path = os.path.join(source_path, "label")
            if os.path.exists(source_label_path):
                json_file_list = [
                    json_file
                    for json_file in os.listdir(source_label_path)
                    if json_file.endswith(".json")
                ]
                if len(json_file_list) > 0:
                    target_source_path_list.append(source_path)
                    target_target_path_list.append(target_path)
                else:
                    no_result_scene_name_list.append(os.path.basename(source_path))
            else:
                no_result_scene_name_list.append(os.path.basename(source_path))

        if len(no_result_scene_name_list) > 0:
            no_result_scene_name_list.sort()
            print(f"no result scene: {no_result_scene_name_list}")

        return target_source_path_list, target_target_path_list
