import json
import os
from datetime import date

import numpy as np
import quaternion
from pypcd import pypcd


def get_points_num(filepath, size, position, rotation):
    """获取当前box中点的数量

    Args:
        filepath (str): pointcloud file path (.pcd, .bin)
        size (list): box size [x, y, z]
        position (list): box position [x, y, z]
        rotation (list): box rotation [w, x, y, z]
    Returns:
        int: box points number
    """

    # make sure the point cloud filepath is valid
    if not os.path.exists(filepath):
        return 0

    # load point cloud
    pc = pypcd.PointCloud.from_path(filepath)
    point_cloud = pc.to_array()
    point_cloud = point_cloud.reshape(-1, 4)

    # check box parameters
    if not isinstance(size, list) or len(size) != 3:
        return 0
    if not isinstance(position, list) or len(position) != 3:
        return 0
    if not isinstance(rotation, list) or len(rotation) != 4:
        return 0

    size = np.array(size)
    position = np.array(position)
    rotation = quaternion.from_float_array(rotation)  # (w, x, y, z)
    rotation_matrix = quaternion.as_rotation_matrix(rotation)

    # 2. translate point cloud to the origin
    point_cloud[:, :3] = point_cloud[:, :3] - position

    # 3. rotate point cloud
    point_cloud[:, :3] = np.dot(point_cloud[:, :3], rotation_matrix)

    # 4. get points in box
    min_x = -size[0] / 2
    max_x = size[0] / 2
    min_y = -size[1] / 2
    max_y = size[1] / 2
    min_z = -size[2] / 2
    max_z = size[2] / 2

    mask_x = np.logical_and(point_cloud[:, 0] >= min_x, point_cloud[:, 0] <= max_x)
    mask_y = np.logical_and(point_cloud[:, 1] >= min_y, point_cloud[:, 1] <= max_y)
    mask_z = np.logical_and(point_cloud[:, 2] >= min_z, point_cloud[:, 2] <= max_z)

    mask = np.logical_and(mask_x, np.logical_and(mask_y, mask_z))
    selected_points = point_cloud[mask]

    point_num = selected_points.shape[0]

    return point_num


def get_points_num_dict(
    filepath,
    id_list: list,
    size_list: list,
    position_list: list,
    rotation_list: list,
    fake_mode: bool = False,
):
    """get each box's points number

    Args:
        filepath: str, point cloud file path
        id_list: list, box id list
        size_list: list, box size list
        position_list: list, box position list
        rotation_list: list, box rotation list

    Returns:
        dict: box id and points number dict
    """

    # make sure the point cloud filepath is valid
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    # if fake mode, return random points number
    if fake_mode:
        points_num_dict = {}
        for i in range(len(id_list)):
            points_num_dict[id_list[i]] = np.random.randint(100, 1000)
        return points_num_dict

    # load point cloud
    pc = pypcd.PointCloud.from_path(filepath)
    raw_point_cloud = pc.to_array()
    raw_point_cloud = raw_point_cloud.reshape(-1, 4)

    # check box parameters
    if (
        len(id_list) != len(size_list)
        or len(id_list) != len(position_list)
        or len(id_list) != len(rotation_list)
    ):
        return 0

    points_num_dict = {}
    for i in range(len(id_list)):
        point_cloud = raw_point_cloud.copy()

        size = size_list[i]
        position = position_list[i]
        rotation = rotation_list[i]

        size = np.array(size)
        position = np.array(position)
        rotation = quaternion.from_float_array(rotation)  # (w, x, y, z)
        rotation_matrix = quaternion.as_rotation_matrix(rotation)

        # 2. translate point cloud to the origin
        point_cloud[:, :3] = point_cloud[:, :3] - position

        # 3. rotate point cloud
        point_cloud[:, :3] = np.dot(point_cloud[:, :3], rotation_matrix)

        # 4. get points in box
        min_x = -size[0] / 2
        max_x = size[0] / 2
        min_y = -size[1] / 2
        max_y = size[1] / 2
        min_z = -size[2] / 2
        max_z = size[2] / 2

        mask_x = np.logical_and(point_cloud[:, 0] >= min_x, point_cloud[:, 0] <= max_x)
        mask_y = np.logical_and(point_cloud[:, 1] >= min_y, point_cloud[:, 1] <= max_y)
        mask_z = np.logical_and(point_cloud[:, 2] >= min_z, point_cloud[:, 2] <= max_z)

        mask = np.logical_and(mask_x, np.logical_and(mask_y, mask_z))
        selected_points = point_cloud[mask]

        point_num = selected_points.shape[0]

        points_num_dict[id_list[i]] = point_num

    return points_num_dict


def add_bag_info(bag_path):
    """Add bag info to INFO.json file"""

    # get bag name
    bag_name = os.path.basename(bag_path)
    scene_name = bag_name.split(".")[0]
    info_file_path = os.path.join(os.path.dirname(bag_path), "INFO.json")

    # check if the bag file exists
    if not os.path.exists(info_file_path):
        with open(info_file_path, "w") as f:
            f.write("[]")
    # read INFO.json file with json and need support chinese
    with open(info_file_path, "r") as f:
        info = json.load(f)

        # # example of info
        # [
        #     {
        #         "scene_name": "0001_YC200-2021-007",
        #         "description": "lidar data",
        #         "map_name": "suzhou",
        #         "date_captured": "2023-10-30",
        #     },
        #     {
        #         "scene_name": "0029_YC800B01-N1-0002",
        #         "description": "厂房,晚上,大量静态机动车",
        #         "map_name": "suzhou",
        #         "date_captured": "2023-11-24",
        #     },
        # ]
        # check if the bag file is already in INFO.json file
        for item in info:
            if item["scene_name"] == scene_name:
                return
        # if not, add the bag file to INFO.json file
        # get current date with format `2023-11-24`
        today = date.today()
        current_date = today.strftime("%Y-%m-%d")

        current_bug_info = {
            "scene_name": scene_name,
            "description": "lidar data",
            "map_name": "suzhou",
            "date_captured": current_date,
        }

        info.append(current_bug_info)
        # write back to INFO.json file
        with open(info_file_path, "w") as f:
            json.dump(info, f, ensure_ascii=False, indent=4)
