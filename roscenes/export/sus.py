import json
import os
import shutil

import cv2
import numpy as np
import quaternion

from ..common.calib import NuscenesCalibratedSensor
from ..common.data_config import DataConfig
from ..nuscenes.rule import parse_filename


class CameraConfig:
    def __init__(self, nuscenes_camera_config):
        self.nuscenes_camera_config = nuscenes_camera_config
        self.height = self.nuscenes_camera_config["height"]
        self.width = self.nuscenes_camera_config["width"]
        self.translation_vector = self.nuscenes_camera_config["translation"]
        self.rotation_vector = self.nuscenes_camera_config["rotation"]
        self.camera_intrinsic_matrix = self.nuscenes_camera_config["camera_intrinsic"]

    def to_xtreme1_camera_config(self):
        """将 camera_config 转换为 xtreme1 camera_config 格式"""
        xtreme1_camera_config = {}
        xtreme1_camera_config["camera_internal"] = self.get_camera_internal()
        xtreme1_camera_config["width"] = self.width
        xtreme1_camera_config["height"] = self.height
        xtreme1_camera_config["camera_external"] = self.get_camera_external()
        xtreme1_camera_config["rowMajor"] = False

        return xtreme1_camera_config

    def get_camera_internal(self):
        """获取相机内参"""
        camera_internal = {}
        camera_internal["fx"] = self.camera_intrinsic_matrix[0][0]
        camera_internal["fy"] = self.camera_intrinsic_matrix[1][1]
        camera_internal["cx"] = self.camera_intrinsic_matrix[0][2]
        camera_internal["cy"] = self.camera_intrinsic_matrix[1][2]
        return camera_internal

    def get_camera_external(self):
        """获取相机外参矩阵"""

        # Given quaternion(w,x,y,z)
        rotation = quaternion.from_float_array(self.rotation_vector)
        rotation_matrix = quaternion.as_rotation_matrix(rotation)
        translation = np.array(self.translation_vector)

        # get transform matrix (camera to lidar)
        transform_matrix = np.eye(4)
        transform_matrix[:3, :3] = rotation_matrix
        transform_matrix[:3, 3] = translation

        # get transform matrix (lidar to camera)
        transform_matrix = np.linalg.inv(transform_matrix)

        # convert to row major
        transform_matrix = transform_matrix.T  # transpose to row major
        camera_external = transform_matrix.reshape(-1).tolist()

        return camera_external


class ExportToSUS:
    def __init__(
        self,
        nuscenes_path,
        sus_path,
    ):
        self.nuscenes_path = nuscenes_path
        self.sus_path = sus_path

        self.config = DataConfig()
        self.main_channel = self.config.main_channel

        # create camera_channel_list
        camera_topic_channel_dict = self.config.camera_topic_channel_dict
        self.camera_channel_list = [
            camera_topic_channel_dict[channel] for channel in camera_topic_channel_dict
        ]

        # check camera_channel_list
        # list all channel in source_path/sample
        source_sample_path = os.path.join(nuscenes_path, "samples")
        source_sample_channel_list = os.listdir(source_sample_path)
        # remove camera_channel which not in source_sample_channel_list
        self.camera_channel_list = [
            channel
            for channel in self.camera_channel_list
            if channel in source_sample_channel_list
        ]

        self.lidar_target_name = "lidar"

    def export(self):
        # 1. check source valid
        self.source_check()

        # 2. copy sensor data
        self.export_lidar_data()
        self.export_camera_data()

        self.export_label_data()
        self.export_calib_data()
        self.export_ego_pose()

    def source_check(self):

        # 1. 确保 source_path 路径下存在特定的文件
        # - samples/${main_channel} 文件夹
        # - v1.0-all/calibrated_sensor.json 文件
        # - v1.0-all/ego_pose.json 文件
        main_channel_path = os.path.join(
            self.nuscenes_path, "samples", self.main_channel
        )
        calibrated_sensor_path = os.path.join(
            self.nuscenes_path, "v1.0-all", "calibrated_sensor.json"
        )
        ego_pose_path = os.path.join(self.nuscenes_path, "v1.0-all", "ego_pose.json")

        assert os.path.exists(main_channel_path)
        assert os.path.exists(calibrated_sensor_path)
        assert os.path.exists(ego_pose_path)

        # 2. 将特定文件拷贝至${target_path} 目录下 并按照格式需求进行重命名
        # 2.1 ${source_path}/samples 下所有 camera 数据拷贝至 ${target_path}/camera 目录下
        # 2.2 ${source_path}/samples 下所有 lidar 数据拷贝至 ${target_path}/lidar 目录下
        # 2.3 ${source_path}/v1.0-all/ego_pose.json 文件拷贝至 ${target_path} 目录下
        # 2.4 camera 数据重命名
        if not os.path.exists(self.sus_path):
            os.makedirs(self.sus_path)

    def export_label_data(self):
        """导出 label 数据

        但其实没有实际的导出动作,只是检查是否存在label文件夹,
        如果有了且有文件,则不再导出,如果没有则创建一个label文件夹
        """
        label_folder_path = os.path.join(self.sus_path, "label")
        if not os.path.exists(label_folder_path):
            os.makedirs(label_folder_path)

    def export_camera_data(self):
        target_camera_folder_path = os.path.join(self.sus_path, "camera")
        if not os.path.exists(target_camera_folder_path):
            os.makedirs(target_camera_folder_path)

        lidar_filename_list = os.listdir(
            os.path.join(self.nuscenes_path, "samples", self.main_channel)
        )
        lidar_filename_list = [
            filename.split(".")[0] for filename in lidar_filename_list
        ]

        # 1. copy camera data
        for camera_channel in self.camera_channel_list:
            raw_folder = os.path.join(self.nuscenes_path, "samples", camera_channel)
            target_folder = os.path.join(target_camera_folder_path, camera_channel)
            # check raw folder exist
            if not os.path.exists(raw_folder):
                continue
            # check target folder exist
            if not os.path.exists(target_folder):
                os.makedirs(target_folder)
            # copy file if same file just skip
            camera_filename_list = os.listdir(raw_folder)
            img_suffix = camera_filename_list[0].split(".")[-1]
            # print("img suffix : ", img_suffix)
            # check len of camera_filename_list and lidar_filename_list
            if len(camera_filename_list) != len(lidar_filename_list):
                raise ValueError(
                    f"camera folder and lidar folder have different number of files"
                )

            for i, filename in enumerate(camera_filename_list):
                source_file_path = os.path.join(raw_folder, filename)
                new_camera_filename = filename.replace(camera_channel, "lidar-fusion")
                # new_camera_filename = lidar_filename_list[i] + "." + img_suffix
                target_file_path = os.path.join(target_folder, new_camera_filename)
                assert new_camera_filename.split(".")[0] in lidar_filename_list
                if os.path.exists(target_file_path):
                    continue
                shutil.copy(source_file_path, target_file_path)

    def export_lidar_data(self):
        target_lidar_folder_path = os.path.join(self.sus_path, "lidar")
        if not os.path.exists(target_lidar_folder_path):
            os.makedirs(target_lidar_folder_path)
        lidar_folder_path = os.path.join(
            self.nuscenes_path, "samples", self.main_channel
        )
        for filename in os.listdir(lidar_folder_path):
            source_file_path = os.path.join(lidar_folder_path, filename)
            target_file_path = os.path.join(target_lidar_folder_path, filename)
            if os.path.exists(target_file_path):
                continue
            shutil.copy(source_file_path, target_file_path)

    def export_calib_data(self):
        """导出标定数据"""
        target_calib_folder_path = os.path.join(self.sus_path, "calib", "camera")
        calibrated_sensor_path = os.path.join(
            self.nuscenes_path, "v1.0-all", "calibrated_sensor.json"
        )
        if not os.path.exists(target_calib_folder_path):
            os.makedirs(target_calib_folder_path)
        nuscenes_calibrated_sensor = NuscenesCalibratedSensor(calibrated_sensor_path)
        for camera_channel in self.camera_channel_list:
            camera_calib_info = nuscenes_calibrated_sensor.get_calib_info(
                camera_channel
            )

            extrinsic = camera_calib_info.get_extrinsic().flatten().tolist()
            intrinsic = camera_calib_info.get_intrinsic().flatten().tolist()

            json_dict = {
                "extrinsic": extrinsic,
                "intrinsic": intrinsic,
            }

            camera_calib_info_path = os.path.join(
                self.sus_path, "calib", "camera", camera_channel + ".json"
            )

            with open(camera_calib_info_path, "w") as f:
                json.dump(json_dict, f, indent=4)

    def export_ego_pose(self):
        """通过 nuscenes 的 ego pose 文件生成 sus 的 ego pose 文件

        Args:
            target_folder_path (_type_): 最终生成的 ego pose 文件夹路径
        """

        target_ego_pose_folder_path = os.path.join(self.sus_path, "ego_pose")
        if not os.path.exists(target_ego_pose_folder_path):
            os.makedirs(target_ego_pose_folder_path)

        # step1. parse nuscenes ego pose file
        nuscenes_ego_pose_file_path = os.path.join(
            self.nuscenes_path, "v1.0-all", "ego_pose.json"
        )
        # check file exist
        if not os.path.exists(nuscenes_ego_pose_file_path):
            raise FileNotFoundError(f"{nuscenes_ego_pose_file_path} not found")
        # get all lidar file name
        lidar_folder_path = os.path.join(
            self.nuscenes_path, "samples", self.main_channel
        )
        lidar_files = os.listdir(lidar_folder_path)
        lidar_filename_list = [filename.split(".")[0] for filename in lidar_files]
        # create a dict to store ego pose and lidar_filename by timestamp
        # 1. create a dict to store lidar_filename by timestamp
        lidar_filename_dict = {}
        for filename in lidar_filename_list:
            timestamp = filename.split("_")[-1]
            lidar_filename_dict[timestamp] = filename
        # 2. parse nuscenes ego pose file to create a dict to store ego pose by timestamp
        with open(nuscenes_ego_pose_file_path, "r") as f:
            ego_pose = json.load(f)
        all_ego_pose_dict = {}
        for pose in ego_pose:
            all_ego_pose_dict[str(pose["timestamp"])] = pose
        # 3. filter ego pose by lidar timestamp(just save the ego pose which timestamp in lidar_filename_list)
        ego_pose_dict = {}
        for timestamp in lidar_filename_dict:
            if timestamp in all_ego_pose_dict:
                ego_pose_dict[timestamp] = {
                    "filename": lidar_filename_dict[timestamp],
                    "ego_pose": all_ego_pose_dict[timestamp],
                }

        # step2. generate sus ego pose file
        # - ecah lidar frame has a ego pose file , which named by lidar frame name,suffix is .json
        # - target ego pose file format is like this:
        # {
        #     "translation": [
        #         -34.3687109791769,
        #         -2302.800518195987,
        #         12.732722526196365,
        #     ],
        #     "rotation": [
        #         0.07968682128809775,
        #         0.000485533588441593,
        #         0.011274789554449945,
        #         0.9967560653894393,
        #     ],
        # }

        # - nuscenes ego pose file format is like this:
        # {
        #     "token": "37091c75b9704e0daa829ba56dfa0906",
        #     "timestamp": 1532402927664178,
        #     "rotation": [
        #         0.5721129977125774,
        #         -0.0014962022442161157,
        #         0.011922678049447764,
        #         -0.8200867813684729
        #     ],
        #     "translation": [
        #         411.25243634487725,
        #         1180.7511754315697,
        #         0.0
        #     ]
        # }
        for timestamp in ego_pose_dict:
            ego_pose = ego_pose_dict[timestamp]["ego_pose"]
            filename = ego_pose_dict[timestamp]["filename"]

            # convert quaternion to euler angle
            rotation_in_quaternion = ego_pose["rotation"]
            rotation_in_quaternion = quaternion.from_float_array(rotation_in_quaternion)
            rotation_in_euler = quaternion.as_euler_angles(
                rotation_in_quaternion
            ).tolist()
            ego_pose_file_path = os.path.join(
                target_ego_pose_folder_path, filename + ".json"
            )
            with open(ego_pose_file_path, "w") as f:
                json.dump(
                    {
                        "translation": ego_pose["translation"],
                        "rotation": ego_pose["rotation"],
                    },
                    f,
                    indent=4,
                )


def rename_image_files(image_folder_path_list, point_cloud_folder_path):
    """将 image_folder 下图片文件重命名,并且与 point_cloud_folder 中的文件名对应起来

    例如：
        - image0 文件夹下的文件名为 YC200B-M1-0004_17885471_cam-front_1695888879276.jpg
        - point_cloud 文件夹下的文件名为 YC200B-M1-0004_17885471_lidar-fusion_1695888879276.pcd
    则将 image0 文件夹下的文件名重命名为 YC200B-M1-0004_17885471_lidar-fusion_1695888879276.jpg
    """
    # check folder exist
    for image_folder_path in image_folder_path_list:
        image_folder_path = os.path.expanduser(image_folder_path)
        if not os.path.exists(image_folder_path):
            raise FileNotFoundError(f"{image_folder_path} not found")

    point_cloud_folder_path = os.path.expanduser(point_cloud_folder_path)
    if not os.path.exists(point_cloud_folder_path):
        raise FileNotFoundError(f"{point_cloud_folder_path} not found")

    # generate point_cloud timestamp dict
    point_cloud_files = os.listdir(point_cloud_folder_path)
    point_cloud_timestamp_dict = {}
    for filename in point_cloud_files:
        scene_name, channel, timestamp, fileformat = parse_filename(filename)
        point_cloud_file_name = filename.split(".")[0]
        point_cloud_timestamp_dict[timestamp] = point_cloud_file_name

    # get image suffix
    temp_image_folder_path = os.path.expanduser(image_folder_path_list[0])
    temp_image_files = os.listdir(temp_image_folder_path)
    image_suffix = temp_image_files[0].split(".")[-1]
    image_suffix = "." + image_suffix

    # rename image files
    point_cloud_files = os.listdir(point_cloud_folder_path)
    for image_folder_path in image_folder_path_list:
        image_folder_path = os.path.expanduser(image_folder_path)
        image_files = os.listdir(image_folder_path)
        # first check files number
        if len(image_files) != len(point_cloud_files):
            raise ValueError(
                f"image folder and point cloud folder have different number of files"
            )

        # rename image files
        for image_filename in image_files:
            # compare file name
            scene_name, channel, timestamp, fileformat = parse_filename(image_filename)
            # image_file_timestamp = image_filename.split(".")[0].split("-")[-1]
            # check timestamp exist in point_cloud_timestamp_dict
            if timestamp not in point_cloud_timestamp_dict:
                raise ValueError(f"{timestamp} not found in point cloud folder")
            point_cloud_file_name = point_cloud_timestamp_dict[timestamp]

            image_file_path = os.path.join(image_folder_path, image_filename)
            new_image_file_path = (
                os.path.join(image_folder_path, point_cloud_file_name) + image_suffix
            )
            # rename
            os.rename(image_file_path, new_image_file_path)


def generate_camera_config(
    calibrated_sensor_path,
    image_folder_rename_dict,
    rename_image_folder_path_list,
):
    # read calibrated_sensor.json
    with open(calibrated_sensor_path, "r") as f:
        calibrated_sensor = json.load(f)

    # filter camera config
    camera_config_dict = {}
    for sensor in calibrated_sensor:
        if sensor["channel"] in image_folder_rename_dict:
            camera_config_dict[image_folder_rename_dict[sensor["channel"]]] = sensor

    # sort camera config by key
    camera_config_dict = dict(sorted(camera_config_dict.items(), key=lambda x: x[0]))

    # add image height and width to camera config
    rename_image_folder_path_list.sort()

    for i, camera in enumerate(camera_config_dict.values()):
        image_folder_path = rename_image_folder_path_list[i]
        image_file_path_list = os.listdir(image_folder_path)
        first_image_file_path = os.path.join(image_folder_path, image_file_path_list[0])
        (image_height, image_width, _) = cv2.imread(first_image_file_path).shape

        camera["height"] = image_height
        camera["width"] = image_width

    # reconstruct camera config dict with class CameraConfig
    camera_config_dict = {
        key: CameraConfig(camera_config_dict[key]) for key in camera_config_dict
    }

    # convert camera config to xtreme1 camera config in json format
    camera_config_list = [
        camera_config_dict[key].to_xtreme1_camera_config() for key in camera_config_dict
    ]

    return camera_config_list
