import json
import multiprocessing
import os
import shutil
from functools import partial

import cv2
import numpy as np
import yaml

# from minio import Minio
# from minio.error import S3Error
from rich.progress import track
from scipy.spatial.transform import Rotation as R

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
        """获取相机外参"""

        def compose_transform(R, t):
            T = np.eye(4)
            T[:3, :3] = R
            T[:3, 3] = t
            return T

        # Given quaternion (注意：scipy要求四元数以 (w, x, y, z) 的形式给出)
        q = self.rotation_vector
        t = self.translation_vector
        rotation_matrix = R.from_quat(q).as_matrix()
        T = compose_transform(rotation_matrix, t)

        # convert T to one dimension list
        camera_external = T.reshape(-1).tolist()

        return camera_external


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
        _, _, _, timestamp, _ = parse_filename(filename)
        # point_cloud_timestamp = point_cloud_file.split(".")[0].split("-")[-1]
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
            _, _, _, timestamp, _ = parse_filename(image_filename)
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


def export_to_x(
    format,
    source_path_list,
    target_path_list,
    suffix_list,
    main_channel_list,
    worker_num=4,
):
    print(f"total {len(source_path_list)} scenes need to be export to xtreme1")
    print("target format: ", format)

    format_list = [format for _ in range(len(source_path_list))]

    # 使用zip将参数打包成元组
    args_list = list(
        zip(
            format_list,
            source_path_list,
            target_path_list,
            suffix_list,
            main_channel_list,
        )
    )

    wrapped_function = partial(export_scene_to_x_wrapper)
    with multiprocessing.Pool(processes=worker_num) as pool:
        list(
            track(
                pool.imap_unordered(wrapped_function, enumerate(args_list)),
                total=len(args_list),
            )
        )

    print("Decompressing Done!")


def export_scene_to_x_wrapper(args):
    idx, (format, source_path, target_path, suffix, main_channel) = args
    result = export_scene_to_x(format, source_path, target_path, suffix, main_channel)
    return idx, result


def export_scene_to_x(
    format,
    source_path,
    target_path,
    suffix=".zip",
    main_channel="lidar-fusion",
):
    scene_id = os.path.basename(source_path)
    car_id = os.path.basename(os.path.dirname(source_path))

    lidar_target_name = ""
    camera_target_name_list = []

    if format == "xtreme1":
        lidar_target_name = "point_cloud"
        camera_target_name_list = [
            "image0",
            "image1",
            "image2",
            "image3",
            "image4",
            "image5",
            "image6",
            "image7",
            "image8",
        ]
    elif format == "basicai":
        lidar_target_name = "lidar_point_cloud_0"
        camera_target_name_list = [
            "camera_image_0",
            "camera_image_1",
            "camera_image_2",
            "camera_image_3",
            "camera_image_4",
            "camera_image_5",
            "camera_image_6",
            "camera_image_7",
            "camera_image_8",
        ]

    # 1. 确保 source_path 路径下存在特定的文件
    # - samples/${main_channel} 文件夹
    # - v1.0-trainval/calibrated_sensor.json 文件
    main_channel_path = os.path.join(source_path, "samples", main_channel)
    calibrated_sensor_path = os.path.join(
        source_path,
        "v1.0-all",
        "calibrated_sensor.json",
    )

    assert os.path.exists(main_channel_path)
    assert os.path.exists(calibrated_sensor_path)

    # 2. 将 ${source_path}/samples 下所有的文件夹拷贝至 ${target_path} 目录下
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    else:
        os.system("rm -rf {}".format(target_path))
        os.makedirs(target_path)
    # copy
    os.system("cp -r {}/* {}".format(os.path.join(source_path, "samples"), target_path))

    # 3. 文件夹重命名
    # 3.1 将雷达数据文件夹重命名 ${target_path}/${main_channel}
    os.system(
        "mv {} {}".format(
            os.path.join(target_path, main_channel),
            os.path.join(target_path, lidar_target_name),
        )
    )
    # 3.2 将相机数据文件夹重命名
    # - 首先对这些文件夹进行排序
    # - 按照顺序文件夹名称进行重命名
    image_folder_list = [
        folder for folder in os.listdir(target_path) if folder != lidar_target_name
    ]

    image_folder_list.sort()
    image_folder_rename_dict = {
        folder: camera_target_name_list[index]
        for index, folder in enumerate(image_folder_list)
    }
    for raw_folder_name in image_folder_list:
        os.system(
            "mv {} {}".format(
                os.path.join(target_path, raw_folder_name),
                os.path.join(target_path, image_folder_rename_dict[raw_folder_name]),
            )
        )

    # 4. 将所有图像文件进行重命名
    # - 首先对这些文件进行排序
    # - 将每个文件名与 点云 文件夹下的文件名进行对应并重命名
    #     例如：
    #           - 图像 文件夹下的文件名为 YC200B-M1-0004_17885471_cam-front_1695888879276.jpg
    #           - 点云 文件夹下的文件名为 YC200B-M1-0004_17885471_lidar-fusion_1695888879276.pcd
    #       则将 图像 文件夹下的文件名重命名为 YC200B-M1-0004_17885471_lidar-fusion_1695888879276.jpg
    rename_image_folder_list = [
        rename_folder for _, rename_folder in image_folder_rename_dict.items()
    ]
    rename_image_folder_path_list = [
        os.path.join(target_path, folder) for folder in rename_image_folder_list
    ]
    point_cloud_folder_path = os.path.join(target_path, lidar_target_name)

    rename_image_files(rename_image_folder_path_list, point_cloud_folder_path)

    # 5. 生成 ${target_path}/camera_config 文件夹
    # 5.1 读取 ${source_path}/v1.0-all/calibrated_sensor.json 文件中的内容
    # 5.2 筛选出存在于 image_folder_rename_dict value 中的相机参数 并使其与 image_folder_rename_dict key 对应
    # 5.3 将其格式转换为 xtreme1 camera_config 格式
    # 5.4 将其写入 ${target_path}/camera_config 文件夹下
    # - 写入的为一系列 json 文件，所有 json 文件内容相同 文件数量和名称与 ${lidar_target_name} 文件夹下的文件名对应
    # get image width and height

    first_image_folder_path = rename_image_folder_path_list[0]
    first_image_file_path_list = os.listdir(first_image_folder_path)
    first_image_file_path = os.path.join(
        first_image_folder_path, first_image_file_path_list[0]
    )

    (image_height, image_width, _) = cv2.imread(first_image_file_path).shape
    # generate camera config template
    camera_config_list = generate_camera_config(
        calibrated_sensor_path,
        image_folder_rename_dict,
        image_height,
        image_width,
    )
    # write camera config to file with json format
    camera_config_folder_path = os.path.join(target_path, "camera_config")
    if not os.path.exists(camera_config_folder_path):
        os.makedirs(camera_config_folder_path)
    point_cloud_filename_list = os.listdir(point_cloud_folder_path)
    for filename in point_cloud_filename_list:
        camera_config_filename = filename.split(".")[0] + ".json"
        camera_config_path = os.path.join(
            camera_config_folder_path, camera_config_filename
        )
        with open(camera_config_path, "w") as f:
            json.dump(camera_config_list, f, indent=4)

    # 6. 将 ${target_path} 文件夹进行压缩
    # - 压缩后的文件名为 ${target_path}.zip
    # - 压缩后的文件路径为 ${car_id}-${target_path}.zip
    target_folder_path = os.path.dirname(target_path)
    compressed_file_name = car_id + "-" + scene_id + suffix
    target_compressed_file_path = os.path.join(target_folder_path, compressed_file_name)

    os.system(
        "cd {} && zip -r {} {} > /dev/null 2>&1".format(
            os.path.dirname(target_path),
            target_compressed_file_path,
            os.path.basename(target_path),
        )
    )

    # 7. 删除 ${target_path} 文件夹
    shutil.rmtree(target_path)


def generate_camera_config(
    calibrated_sensor_path,
    other_folder_dict,
    image_height,
    image_width,
):
    # read calibrated_sensor.json
    with open(calibrated_sensor_path, "r") as f:
        calibrated_sensor = yaml.load(f, Loader=yaml.FullLoader)

    # filter camera config
    camera_config_dict = {}
    for camera in calibrated_sensor:
        if camera["channel"] in other_folder_dict:
            camera_config_dict[other_folder_dict[camera["channel"]]] = camera

    # sort camera config by key
    camera_config_dict = dict(sorted(camera_config_dict.items(), key=lambda x: x[0]))

    # add image height and width to camera config
    for camera in camera_config_dict.values():
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
