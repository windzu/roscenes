import bisect
import datetime
import json
import os
import uuid

import cv2
import numpy as np
import rosbag
from pypcd import pypcd


NAMESPACE_URL = uuid.NAMESPACE_URL


def save_camera(
    msg,
    path,
    filename,
    default_img_width,
    default_img_height,
):
    file_path = os.path.join(path, filename)

    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    # save CompressedImage to png
    # 如果 msg 不为空 则按照正常流程保存图片
    # 如果 msg 为空 则创建一张绿色图片,并保存,使用默认的宽高
    if msg:
        np_arr = np.fromstring(msg.data, np.uint8)
        image_np = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        cv2.imwrite(file_path, image_np)

        img_width = image_np.shape[1]
        img_height = image_np.shape[0]
        return (img_width, img_height, file_path)
    else:
        # 创建一张绿色图片
        img = np.zeros((default_img_height, default_img_width, 3), np.uint8)
        img[:, :, 1] = 255
        cv2.imwrite(file_path, img)

        return (default_img_width, default_img_height, file_path)


def save_lidar(msg, path, filename):
    file_path = os.path.join(path, filename)

    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    # save PointCloud2 to bin
    pc = pypcd.PointCloud.from_msg(msg)
    pc.save_pcd(file_path, compression="binary_compressed")
    # pc.save_bin(file_path, "xyzi")
    return (0, 0, file_path)


def fusion_lidar_points(
    lidar_msg_dict,
    calib_info_dict,
    lidar_fusion_flag,
    channel_name=None,
    transform_lidar_flag=True,
):
    def transform_pc2_msg(msg, transform_matrix):
        """变换pcd

        Args:
            pcd (PointCloud2): pcd
            transform (numpy.ndarray): 变换矩阵

        Returns:
            numpy.ndarray: 变换后的pcd
        """
        pc = pypcd.PointCloud.from_msg(msg)
        x = pc.pc_data["x"].flatten()
        y = pc.pc_data["y"].flatten()
        z = pc.pc_data["z"].flatten()
        intensity = pc.pc_data["intensity"].flatten()
        nan_index = (
            np.isnan(x) | np.isnan(y) | np.isnan(z) | np.isnan(intensity)
        )  # filter nan data
        pc_array_4d = np.zeros((x[~nan_index].shape[0], 4), dtype=np.float32)
        pc_array_4d[:, 0] = x[~nan_index]
        pc_array_4d[:, 1] = y[~nan_index]
        pc_array_4d[:, 2] = z[~nan_index]
        pc_array_4d[:, 3] = 1  # 待会儿要乘以变换矩阵，所以最后一列要是1

        if transform_matrix is None:
            return pc_array_4d
        pc_array_4d = np.dot(transform_matrix, pc_array_4d.T).T
        pc_array_4d[:, 3] = intensity[~nan_index]  # 变换后的点云的intensity要重新赋值
        return pc_array_4d

    fusion_lidar_msg = None
    fusion_lidar_array = None
    lidar_calib_dict = {}
    for tmp_channel_name in lidar_msg_dict.keys():
        lidar_calib_dict[tmp_channel_name] = calib_info_dict[
            tmp_channel_name
        ].get_transform_matrix()

    for tmp_channel_name in lidar_msg_dict.keys():
        if not lidar_fusion_flag:
            if tmp_channel_name != channel_name:
                continue

        # transform pcd
        pc_array_4d = None
        if transform_lidar_flag:
            pc_array_4d = transform_pc2_msg(
                lidar_msg_dict[tmp_channel_name], lidar_calib_dict[tmp_channel_name]
            )
        else:
            pc_array_4d = transform_pc2_msg(lidar_msg_dict[tmp_channel_name], None)

        if fusion_lidar_array is None:
            fusion_lidar_array = pc_array_4d
        else:
            fusion_lidar_array = np.vstack((fusion_lidar_array, pc_array_4d))

    fusion_lidar_pc = pypcd.PointCloud.from_array_without_dtype(
        fusion_lidar_array, format="xyzi"
    )
    fusion_lidar_msg = fusion_lidar_pc.to_msg()

    return fusion_lidar_msg


def parse_ego_pose(msg):
    # get rotation and translation from PoseStamped
    rotation = [
        msg.pose.orientation.w,
        msg.pose.orientation.x,
        msg.pose.orientation.y,
        msg.pose.orientation.z,
    ]
    translation = [msg.pose.position.x, msg.pose.position.y, msg.pose.position.z]
    return rotation, translation


def preprocess_bag(bag_path, topics):
    bag = rosbag.Bag(bag_path)
    data_by_topic = {}
    for topic, msg, t in bag.read_messages(topics=topics):
        if topic not in data_by_topic:
            data_by_topic[topic] = {}

        # use us
        timestamp_us = ros_timestamp_to_us(t)
        data_by_topic[topic][timestamp_us] = msg
    return data_by_topic


def ros_timestamp_to_ms(ros_timestamp):
    timestamp_ns = int(ros_timestamp.secs) * 1e9 + int(ros_timestamp.nsecs)
    timestamp_ms = int(timestamp_ns // 1e6)
    return timestamp_ms


def ros_timestamp_to_us(ros_timestamp):
    timestamp_ns = int(ros_timestamp.secs) * 1e9 + int(ros_timestamp.nsecs)
    timestamp_us = int(timestamp_ns // 1000)
    return timestamp_us


def closest_timestamp(target_time, timestamps):
    # return min(timestamps, key=lambda t: abs(t - target_time))

    idx = bisect.bisect_left(timestamps, target_time)
    if idx == 0:
        return timestamps[0]
    if idx == len(timestamps):
        return timestamps[-1]
    before = timestamps[idx - 1]
    after = timestamps[idx]
    if after - target_time < target_time - before:
        return after
    else:
        return before


def save_msg(
    msg,
    path,
    filename,
    default_img_width,
    default_img_height,
):
    width = 0
    height = 0
    file_path = ""
    msg_type = str(msg._type)

    if msg_type == "sensor_msgs/CompressedImage" or msg_type == "sensor_msgs/Image":
        (width, height, file_path) = save_camera(msg, path, filename)
    elif msg_type == "sensor_msgs/PointCloud2":
        (width, height, file_path) = save_lidar(msg, path, filename)
    else:
        print("msg type not supported")
        print(type(msg))

    filename = "/".join(file_path.split("/")[-3:])

    return (width, height, filename)


def save_to_json(data, path, filename):
    if not os.path.exists(path):
        os.makedirs(path)

    with open(os.path.join(path, filename), "w") as f:
        # 使用json.dump方法来格式化并保存数据，使其具有良好的可读性
        # 支持中文，需要指定ensure_ascii=False
        json.dump(data, f, indent=4, ensure_ascii=False)


def generate_uuid_from_input(input_string):
    return str(uuid.uuid5(NAMESPACE_URL, input_string).hex)


def convert_uuid_to_five_segment(uuid_str):
    uuid_str = uuid_str.replace("-", "")
    uuid_str = (
        uuid_str[0:8]
        + "-"
        + uuid_str[8:12]
        + "-"
        + uuid_str[12:16]
        + "-"
        + uuid_str[16:20]
        + "-"
        + uuid_str[20:32]
    )
    # upper
    uuid_str = uuid_str.upper()
    return uuid_str


def check_uuid_valid(uuid_str):
    # remove '-' from uuid_str
    uuid_str = uuid_str.replace("-", "")

    if len(uuid_str) != 32:
        return False
    try:
        uuid.UUID(uuid_str)
    except ValueError:
        return False
    return True


def generate_sample_data_info_list_dict(data_path):
    """生成sample_data_list_dict

    Args:
        data_path (str): nuscens数据集路径

    Returns:
        dict: sample_data_list_dict
        {
            "channel_name":[
                {
                    filename : filename,
                    scene_name : scene_name,
                    channel : channel,
                    timestamp : timestamp,
                    fileformat : fileformat,
                    width : width,
                    height : height,
                    is_key_frame : is_key_frame
                },
                ...
            ]
        }
    """
    sample_data_list_dict = {}

    # 获取 samples 文件夹下数据信息
    # 1. 确保存在 data_path/samples 文件夹
    samples_path = os.path.join(data_path, "samples")
    if not os.path.exists(samples_path):
        return sample_data_list_dict

    # 2. 遍历获取 data_path/samples 文件夹下的所有文件夹名称 其中每个文件夹的名字是 channel
    samples_channel_list = os.listdir(samples_path)

    # 3. 遍历每个 channel 文件夹 获取 sample_data_list
    for channel in samples_channel_list:
        # judge dict if exist channel key
        if channel not in sample_data_list_dict:
            sample_data_list_dict[channel] = []
        channel_path = os.path.join(samples_path, channel)
        sample_data_list = generate_sample_data_info_list(channel_path, True)
        sample_data_list_dict[channel].extend(sample_data_list)

    # 获取 sweeps 文件夹下数据信息
    # 1.. 判断是否存在 data_path/sweeps 文件夹
    sweeps_path = os.path.join(data_path, "sweeps")
    if not os.path.exists(sweeps_path):
        return sample_data_list_dict

    # 2. 遍历获取 data_path/sweeps 文件夹下的所有文件夹名称 其中每个文件夹的名字是 channel
    sweeps_channel_list = os.listdir(sweeps_path)
    for channel in sweeps_channel_list:
        # judge dict if exist channel key
        if channel not in sample_data_list_dict:
            sample_data_list_dict[channel] = []
        channel_path = os.path.join(sweeps_path, channel)
        sample_data_list = generate_sample_data_info_list(channel_path, False)
        sample_data_list_dict[channel].extend(sample_data_list)

    # 对 sample_data_list_dict 每个channel中的数据进行排序 以 timestamp 为key
    for channel in sample_data_list_dict.keys():
        sample_data_list_dict[channel].sort(key=lambda x: x["timestamp"])

    return sample_data_list_dict


def generate_sample_data_info_list(data_path, is_key_frame=False):
    from .rule import parse_filename

    # 1. 遍历该文件夹下的所有文件名称 构成 filename_list
    filename_list = os.listdir(data_path)

    # 1.1 解析第一个 filename 获取 width height
    sample_filename = os.path.join(data_path, filename_list[0])
    width, height = get_width_and_height(sample_filename)

    # 2. 遍历 filename_list 并对每个 filename 进行处理
    sample_data_list = []
    for filename in filename_list:
        sample_data = {}
        # 3. 解析 filename 获取 timestamp,channel_name,is_key_frame
        scene_name, channel, timestamp, fileformat = parse_filename(filename)

        # 4. 构造 sample_data
        sample_data = {
            "filename": filename,
            "scene_name": scene_name,
            "channel": channel,
            "timestamp": timestamp,
            "fileformat": fileformat,
            "width": width,
            "height": height,
            "is_key_frame": is_key_frame,
        }

        # 5. 将 sample_data 添加到 sample_data_list
        sample_data_list.append(sample_data)
    return sample_data_list


def get_width_and_height(filename):
    width = 0
    height = 0

    # 确定filename的类型 并获取宽高
    suffix = filename.split(".")[-1]
    if suffix == "png" or suffix == "jpg":
        img = cv2.imread(filename)
        width = img.shape[1]
        height = img.shape[0]

    return width, height


def generate_sensor_info_list(data_path):
    """生成 sensor_info_list
    Note: sensor info 中的 modality 信息是根据 channel 中是否包含 cam 或者 lidar 来判断的

    Args:
        data_path (str): nuscens数据集路径

    Returns:
        list: sensor_info_list
        [
            {
                "modality": "camera",
                "channel": "cam_front",
            },
            ...
        ]

    """
    # 1. 获取 data_path/samples 文件夹下的所有文件夹名称 其中每个文件夹的名字是 channel
    samples_path = os.path.join(data_path, "samples")
    samples_channel_list = os.listdir(samples_path)

    # 2. 遍历 samples_channel_list 获取 sensor_info_list
    sensor_info_list = []
    for channel in samples_channel_list:
        sensor_info = {}
        # 3. 判断 channel 是否包含 cam 或者 lidar
        if channel.find("cam") != -1:
            sensor_info["modality"] = "camera"
        elif channel.find("lidar") != -1:
            sensor_info["modality"] = "lidar"
        sensor_info["channel"] = channel
        sensor_info_list.append(sensor_info)

    return sensor_info_list


def generate_calibrated_sensor_info_list(calib_info_dict):
    calibrated_sensor_info_list = []

    for channel, calib_info in calib_info_dict.items():
        calibrated_sensor_info = {}
        calibrated_sensor_info["channel"] = channel
        calibrated_sensor_info["sensor_token"] = None
        calibrated_sensor_info["translation"] = calib_info.translation
        calibrated_sensor_info["rotation"] = calib_info.rotation
        calibrated_sensor_info["camera_intrinsic"] = calib_info.camera_intrinsic
        calibrated_sensor_info_list.append(calibrated_sensor_info)
    return calibrated_sensor_info_list


def generate_instance_info_list(nuscenes_object_list):
    # if len(nuscenes_object_list) == 0:
    #     raise ValueError("nuscenes_object_list is empty")

    instance_info_list = []

    # instance_info = {
    #     scene_name,
    #     track_id,
    #     category,
    #     nbr_annotations,
    #     first_annotation_timestamp,
    #     first_annotation_object_id,
    #     last_annotation_timestamp,
    #     last_annotation_object_id,
    # }

    track_id_dict = {}

    for object in nuscenes_object_list:
        track_id = object.track_id
        if track_id not in track_id_dict:
            track_id_dict[track_id] = {}
            track_id_dict[track_id]["scene_name"] = object.scene_name
            track_id_dict[track_id]["track_id"] = track_id
            track_id_dict[track_id]["category"] = object.category
            track_id_dict[track_id]["nbr_annotations"] = 1
            track_id_dict[track_id]["timestamp_object_id_dict"] = {}
            track_id_dict[track_id]["timestamp_object_id_dict"][
                object.timestamp
            ] = object.object_id

            track_id_dict[track_id]["first_annotation_timestamp"] = object.timestamp
            track_id_dict[track_id]["first_annotation_object_id"] = object.object_id
            track_id_dict[track_id]["last_annotation_timestamp"] = object.timestamp
            track_id_dict[track_id]["last_annotation_object_id"] = object.object_id
        else:
            track_id_dict[track_id]["nbr_annotations"] += 1
            track_id_dict[track_id]["timestamp_object_id_dict"][
                object.timestamp
            ] = object.object_id

            # compare timestamp
            if object.timestamp < track_id_dict[track_id]["first_annotation_timestamp"]:
                track_id_dict[track_id]["first_annotation_timestamp"] = object.timestamp
                track_id_dict[track_id]["first_annotation_object_id"] = object.object_id
            if object.timestamp > track_id_dict[track_id]["last_annotation_timestamp"]:
                track_id_dict[track_id]["last_annotation_timestamp"] = object.timestamp
                track_id_dict[track_id]["last_annotation_object_id"] = object.object_id

    for track_id in track_id_dict.keys():
        instance_info_list.append(track_id_dict[track_id])

    return instance_info_list, track_id_dict


def generate_sample_annotation_info_list(nuscenes_object_list):
    # if len(nuscenes_object_list) == 0:
    #     raise ValueError("nuscenes_object_list is empty")

    sample_annotation_info_list = []

    # sample_annotation_info = {
    #     scene_name,
    #     timestamp,
    #     object_id,
    #     track_id,
    #     attribute_name_list,
    #     visibility,
    #     translation,
    #     size,
    #     rotation,
    #     num_lidar_pts,
    #     pre_timestamp,
    #     pre_object_id,
    #     next_timestamp,
    #     next_object_id,
    # }

    instance_info_list, track_id_dict = generate_instance_info_list(
        nuscenes_object_list
    )

    for object in nuscenes_object_list:
        sample_annotation_info = {}
        sample_annotation_info["scene_name"] = object.scene_name
        sample_annotation_info["timestamp"] = object.timestamp
        sample_annotation_info["object_id"] = object.object_id
        sample_annotation_info["track_id"] = object.track_id
        sample_annotation_info["attribute_name_list"] = object.attribute_name_list
        sample_annotation_info["visibility"] = object.visibility
        sample_annotation_info["translation"] = object.translation
        sample_annotation_info["size"] = object.size
        sample_annotation_info["rotation"] = object.rotation
        sample_annotation_info["num_lidar_pts"] = object.num_lidar_pts

        # get pre and next object
        timestamp_object_id_dict = track_id_dict[object.track_id][
            "timestamp_object_id_dict"
        ]
        pre_timestamp = None
        pre_object_id = None
        next_timestamp = None
        next_object_id = None
        for timestamp in timestamp_object_id_dict.keys():
            if timestamp < object.timestamp:
                pre_timestamp = timestamp
                pre_object_id = timestamp_object_id_dict[timestamp]
            if timestamp > object.timestamp:
                next_timestamp = timestamp
                next_object_id = timestamp_object_id_dict[timestamp]

        sample_annotation_info["pre_timestamp"] = pre_timestamp
        sample_annotation_info["pre_object_id"] = pre_object_id
        sample_annotation_info["next_timestamp"] = next_timestamp
        sample_annotation_info["next_object_id"] = next_object_id

        sample_annotation_info_list.append(sample_annotation_info)

    return sample_annotation_info_list
