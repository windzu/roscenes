import os
import time

import cv2
import numpy as np
import rosbag
from sensor_msgs.msg import CompressedImage

from ..common.calib import CalibInfo
from ..common.data_config import DataConfig
from . import rule
from .annotation import InstanceTable, LidarsegTable, SampleAnnotationTable
from .extraction import EgoPoseTable, SampleDataTable, SampleTable, SceneTable
from .taxonomy import AttributeTable, CategoryTable, VisibilityTable

# utils
from .utils import (
    closest_timestamp,
    fusion_lidar_points,
    generate_calibrated_sensor_info_list,
    generate_sample_data_info_list_dict,
    generate_sensor_info_list,
    parse_ego_pose,
    preprocess_bag,
    ros_timestamp_to_us,
    save_camera,
    save_lidar,
    save_msg,
)
from .vehicle import CalibratedSensorTable, LogTable, MapTable, SensorTable


class NuscenesInfo:
    def __init__(
        self,
        data_config: DataConfig,
        scene_name: str,
        scene_bag_file: str,
        nuscenes_folder_path: str,
        map_name: str,
        date_captured: str,
        description: str,
        start_time=None,
        end_time=None,
    ):
        self.data_config = data_config
        self.scene_name = scene_name
        self.scene_bag_file = scene_bag_file
        self.nuscenes_folder_path = nuscenes_folder_path
        self.map_name = map_name
        self.date_captured = date_captured
        self.description = description
        self.start_time = start_time
        self.end_time = end_time

        # load param from data_config
        self.lidar_fusion_flag = self.data_config.lidar_fusion_flag
        self.main_topic = self.data_config.main_topic
        self.camera_topic_channel_dict = self.data_config.camera_topic_channel_dict
        self.lidar_topic_channel_dict = self.data_config.lidar_topic_channel_dict
        self.pose_topic_channel_dict = self.data_config.pose_topic_channel_dict
        self.time_diff_threshold_us = int(self.data_config.time_diff_threshold) * 1000
        self.sample_interval = self.data_config.sample_interval
        self.save_sweep_data_flag = self.data_config.save_sweep_data_flag

        # parse bag get some info
        # - self.lidar_topic_channel_dict
        # - self.calib_info_dict
        # - self.data_by_topic
        self.parse_bag()

        self.nuscenes_databse_dict = {}

        # wait scene info sync from outside
        self.scene_description = ""
        self.scene_label_list = []
        self.scene_file_list = []

        # global info
        self.ego_pose_info_list = []

        # global variable
        self.sweeps_count = 0

    def slice(self):
        # 1. 存储初始化
        print("1. Store init")
        if not self.store_init():
            return

        print("2. Slice bag to file")
        self.slice_bag_to_file()
        self.generate_database(self.nuscenes_folder_path)

    def store_init(self):
        """init nuscenes folder path

        if nuscenes folder path is not exist, create it and sub folder
        else, remove it and create it again

        """

        if not os.path.exists(self.nuscenes_folder_path):
            os.makedirs(self.nuscenes_folder_path)
        else:
            os.system(f"rm -rf {self.nuscenes_folder_path}")
            os.makedirs(self.nuscenes_folder_path)

        os.makedirs(os.path.join(self.nuscenes_folder_path, "samples"))
        os.makedirs(os.path.join(self.nuscenes_folder_path, "sweeps"))
        os.makedirs(os.path.join(self.nuscenes_folder_path, "v1.0-all"))
        os.makedirs(os.path.join(self.nuscenes_folder_path, "maps"))

        return True

    def slice_bag_to_file(self):
        """从bag包中提取数据"""

        print("start slice bag to file")
        topic_list = []
        topic_list.extend(self.camera_topic_channel_dict.keys())
        topic_list.extend(self.lidar_topic_channel_dict.keys())
        topic_list.extend(self.pose_topic_channel_dict.keys())

        lidar_channel_list = []
        lidar_channel_list.extend(self.lidar_topic_channel_dict.values())
        lidar_channel_topic_dict = {
            channel: topic for topic, channel in self.lidar_topic_channel_dict.items()
        }

        camera_channel_list = []
        camera_topic_list = []
        camera_topic_list.extend(self.camera_topic_channel_dict.keys())
        camera_channel_list.extend(self.camera_topic_channel_dict.values())
        camera_channel_topic_dict = {
            channel: topic for topic, channel in self.camera_topic_channel_dict.items()
        }
        camera_topic_resolution_dict = {
            topic: (1280, 720) for topic in camera_topic_list
        }

        pose_channel_topic_dict = {
            channel: topic for topic, channel in self.pose_topic_channel_dict.items()
        }

        samples_path = os.path.join(self.nuscenes_folder_path, "samples")
        sweeps_path = os.path.join(self.nuscenes_folder_path, "sweeps")

        # 根据目标topic,从bag包中提取数据 , 如果没有对应的topic数据,则其值为空字典
        # data_by_topic = preprocess_bag(self.scene_bag_file, topic_list)
        data_by_topic = self.data_by_topic

        # get camera real resolution
        # 获取真实的camera分辨率,如果没有camera数据,则使用默认的分辨率
        for camera_topic in camera_topic_list:
            # 如果camera数据为空,为其填充一张默认分辨率的ros image
            if camera_topic not in data_by_topic:
                # create fake ros image msg with default resolution
                main_lidar_first_timestamp = list(
                    data_by_topic[self.main_topic].values()
                )[0].header.stamp
                fake_camera_msg = msg = CompressedImage()
                fake_camera_msg.header.stamp = main_lidar_first_timestamp
                fake_camera_msg.format = "jpeg"
                fake_image = np.zeros((720, 1280, 3), np.uint8)
                _, fake_jpeg_image = cv2.imencode(".jpg", fake_image)
                fake_jpeg_bytes = fake_jpeg_image.tobytes()
                fake_camera_msg.data = fake_jpeg_bytes
                data_by_topic[camera_topic] = {0: fake_camera_msg}

            # # 如果camera数据为空,则跳过,使用默认的分辨率即可
            # if not data_by_topic[camera_topic]:
            #     continue

            # 获取camera数据的分辨率
            camera_msg_sample = list(data_by_topic[camera_topic].values())[0]
            np_arr = np.fromstring(camera_msg_sample.data, np.uint8)
            image_np = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
            img_width = image_np.shape[1]
            img_height = image_np.shape[0]
            camera_topic_resolution_dict[camera_topic] = (img_width, img_height)
        print("parse camera data finished")
        main_timestamps = []
        if not self.start_time and not self.end_time:
            main_timestamps = list(data_by_topic[self.main_topic].keys())
        else:
            start_time, end_time = self.start_time, self.end_time
            for timestamp in list(data_by_topic[self.main_topic].keys()):
                if start_time <= timestamp <= end_time:
                    main_timestamps.append(timestamp)
        # 遍历main topic的时间戳,进行帧同步
        for timestamp in main_timestamps:
            all_topics_found = True
            closest_time_dict = {}

            # 第一次遍历，找到所有topic的最近时间戳,并判断是否所有的 topic msg 都在 time_diff_threshold_us 内
            # - 如果不在，就跳过这个时间戳
            # - 如果在，就保存这个时间戳所有 topic 的数据
            for topic in topic_list:
                # 如果这个topic没有数据，继续检查该topic是否是camera数据,
                # 原则上允许camera数据缺失,但是其他数据不允许
                if not data_by_topic[topic].keys():
                    if topic in camera_topic_list:
                        continue
                    else:
                        all_topics_found = False
                        break

                # 如果这个topic有数据但是时间同步不满足,需要进一步判断
                # 如果是camera数据,则可以认为是时间同步满足的
                # 如果是其他数据,则认为此帧所有数据都不满足时间同步,跳过
                closest_time = closest_timestamp(
                    timestamp, list(data_by_topic[topic].keys())
                )
                if abs(closest_time - timestamp) > self.time_diff_threshold_us:
                    if topic in camera_topic_list:
                        closest_time_dict[topic] = closest_time
                        continue
                    else:
                        all_topics_found = False
                        break
                else:
                    closest_time_dict[topic] = closest_time

            # 当此帧所有数据都满足时间同步时,保存此帧数据
            if all_topics_found:
                sample_data_dict = {}
                sample_data_dict["timestamp"] = timestamp
                sample_data_dict["msg_info_list"] = []

                # fusion lidar points prepare
                lidar_msg_dict = {}

                # - 保存 ego_pose 数据
                ego_pose_topic = pose_channel_topic_dict["ego-pose"]
                ego_pose_msg = data_by_topic[ego_pose_topic][
                    closest_time_dict[ego_pose_topic]
                ]
                (rotation, translation) = parse_ego_pose(ego_pose_msg)
                ego_pose_info = {
                    "timestamp": timestamp,
                    "rotation": rotation,
                    "translation": translation,
                }
                self.ego_pose_info_list.append(ego_pose_info)

                # - 保存 lidar 数据 (但是只有 fusion lidar 才保存)
                lidar_data_list = []

                fusion_lidar_filename = rule.generate_filename(
                    self.scene_name, "lidar-fusion", timestamp, ".pcd"
                )

                lidar_msg_dict = {}
                for lidar_channel in lidar_channel_list:
                    lidar_topic = lidar_channel_topic_dict[lidar_channel]
                    lidar_msg = data_by_topic[lidar_topic][
                        closest_time_dict[lidar_topic]
                    ]
                    lidar_msg_dict[lidar_channel] = lidar_msg
                fusion_lidar_msg = fusion_lidar_points(
                    lidar_msg_dict=lidar_msg_dict,
                    calib_info_dict=self.calib_info_dict,
                    lidar_fusion_flag=self.lidar_fusion_flag,
                    channel_name=self.lidar_topic_channel_dict[self.main_topic],
                    transform_lidar_flag=self.data_config.transform_lidar_flag,
                )
                lidar_data_list.append(
                    {
                        "filename": fusion_lidar_filename,
                        "fileformat": "pcd",
                        "channel": "lidar-fusion",
                        "data": fusion_lidar_msg,
                    }
                )
                # - 保存 camera数据
                camera_data_list = []
                for camera_channel in camera_channel_list:
                    camera_topic = camera_channel_topic_dict[camera_channel]
                    if (
                        camera_topic not in data_by_topic
                        or camera_topic not in closest_time_dict
                    ):
                        continue
                    camera_msg = data_by_topic[camera_topic][
                        closest_time_dict[camera_topic]
                    ]
                    camera_filename = rule.generate_filename(
                        self.scene_name, camera_channel, timestamp, ".jpg"
                    )
                    camera_data_list.append(
                        {
                            "filename": camera_filename,
                            "fileformat": "jpg",
                            "channel": camera_channel,
                            "data": camera_msg,
                        }
                    )
                data_list = camera_data_list + lidar_data_list

                for data in data_list:
                    filename = data["filename"]
                    fileformat = data["fileformat"]
                    channel = data["channel"]
                    msg = data["data"]
                    msg_info = {
                        "height": 0,
                        "width": 0,
                        "filename": "",
                        "fileformat": fileformat,
                        "channel": channel,
                    }
                    sample_data_dict["msg_info_list"].append(msg_info)

                    # get camera resolution
                    default_img_width = 0
                    default_img_height = 0
                    if channel in camera_topic_resolution_dict:
                        default_img_width, default_img_height = (
                            camera_topic_resolution_dict[channel]
                        )

                    # - 到达 sample_interval 时，保存一次 sample 数据
                    # - 其他时候，保存 sweep 数据
                    save_path = ""
                    if self.sweeps_count % self.sample_interval == 0:
                        save_path = os.path.join(samples_path, channel)
                    elif self.save_sweep_data_flag:
                        save_path = os.path.join(sweeps_path, channel)
                    else:
                        continue

                    if channel in camera_channel_list:
                        save_camera(
                            msg,
                            save_path,
                            filename,
                            default_img_width,
                            default_img_height,
                        )
                    elif channel == "lidar-fusion":
                        save_lidar(msg, save_path, filename)
                    else:
                        raise ValueError(f"{channel} not in camera or lidar list")

                self.sweeps_count += 1

    def generate_database(self, save_path):
        """生成nuscenes数据库"""
        # 1. 准备构建数据库所需的必要信息
        # - 对于已有的数据，只需从类中获取即可
        # - 对于还没有的数据，需要从外部获取
        sensor_info_list = generate_sensor_info_list(save_path)
        calibrated_sensor_info_list = generate_calibrated_sensor_info_list(
            self.calib_info_dict
        )
        sample_data_info_list_dict = generate_sample_data_info_list_dict(save_path)
        ego_pose_info_list = self.ego_pose_info_list

        # 通过 sample_data_list_dict 构建 sample_timestamp_list
        sample_timestamp_list = []
        # iter the fist sample_data_dict_list of sample_data_info_list_dict
        _, temp_sample_data_info_list = next(iter(sample_data_info_list_dict.items()))

        for temp_sample_data_info in temp_sample_data_info_list:
            timestamp = temp_sample_data_info["timestamp"]
            is_key_frame = temp_sample_data_info["is_key_frame"]
            if is_key_frame:
                sample_timestamp_list.append(timestamp)

        # 2. 构建数据库
        self.nuscenes_databse_dict = self.build_database(
            scene_name=self.scene_name,
            map_name=self.map_name,
            date_captured=self.date_captured,
            sensor_info_list=sensor_info_list,
            calibrated_sensor_info_list=calibrated_sensor_info_list,
            ego_pose_info_list=ego_pose_info_list,
            sample_data_info_list_dict=sample_data_info_list_dict,
            sample_timestamp_list=sample_timestamp_list,
            description=self.description,
        )

        # 3. 将数据库转换成 json 文件存储
        self.database_sequence_to_json(save_path)

    @staticmethod
    def build_database(
        scene_name,
        map_name,
        date_captured,
        sensor_info_list,
        calibrated_sensor_info_list,
        ego_pose_info_list,
        sample_data_info_list_dict,
        sample_timestamp_list,
        description,
    ):
        """构建nuscenes数据库

        Args:
            car_id (str): 数据所属车辆id (唯一)
            scene_id (_type_): 数据所属场景id (唯一)
            map_name (str): 该场景所属的地图名称
            sensor_info_list (list): 包含所有sensor信息的列表
            calibrated_sensor_info_list (list): 包含所有calibrated_sensor信息的列表
            ego_pose_info_list (list): 包含所有ego_pose信息的列表
            sample_data_info_list_dict (dict): 包含所有sample_data信息的字典
            sample_timestamp_list (list): 包含所有sample的时间戳的列表
        """
        # - log_table :
        # - - 依赖 car_id 和 scene_id 保证唯一性
        # - - 依赖 log_filename ， location 等信息进行内容的填充 但是可以先不填充
        log_table = LogTable(
            scene_name=scene_name,
            logfile="",
            date_captured=date_captured,
            location=map_name,
        )
        # - map_table : 依赖 map_name，但为了直接生成 log_token，所以需要 car_id 和 scene_id
        map_table = MapTable(scene_name=scene_name, map_name=map_name)

        # - sensor_table :
        # - - 依赖 sensor_info_list
        # - - 不依赖 car_id 和 scene_id
        sensor_table = SensorTable(sensor_info_list)

        # - ego_pose_table :
        # - - 依赖 car_id 和 scene_id 保证唯一性
        # - - 依赖 ego_pose_info_list 填充每一帧的信息 包括其中的 timestamp, rotation, translation
        # - - 其中 timestamp 是用于生成 ego_pose_token 的
        ego_pose_table = EgoPoseTable(scene_name, ego_pose_info_list)

        # - calibrated_sensor_table :
        # - - 依赖 car_id 和 scene_id 保证唯一性
        # - - 依赖 calibrated_sensor_info_list 进行内容的填充
        calibrated_sensor_table = CalibratedSensorTable(
            scene_name,
            calibrated_sensor_info_list,
        )

        # - sample_table :
        # - - 依赖 car_id 和 scene_id 保证唯一性
        # - - 依赖 samples_timestamp_list 进行内容的填充 其中包含每一sample的 timestamp
        sample_table = SampleTable(
            scene_name,
            sample_timestamp_list,
        )

        # - scene_table :
        # - - 依赖 car_id 和 scene_id 保证唯一性
        # - - 依赖 samples_timestamp_list 间接计算出 nbr_samples 、 first_sample_token 和 last_sample_token
        scene_table = SceneTable(
            scene_name,
            sample_timestamp_list,
            description,
        )

        # - sample_data_table :
        # - - 依赖 car_id 和 scene_id 保证唯一性
        # - - 依赖 sample_data_dict_list 进行内容的填充
        sample_data_table = SampleDataTable(
            scene_name,
            sample_data_info_list_dict,
        )

        category_table = CategoryTable()
        attribute_table = AttributeTable()
        visibility_table = VisibilityTable()

        return {
            "vehicle": {
                "log": log_table,
                "map": map_table,
                "calibrated_sensor": calibrated_sensor_table,
                "sensor": sensor_table,
            },
            "extraction": {
                "scene": scene_table,
                "sample": sample_table,
                "sample_data": sample_data_table,
                "ego_pose": ego_pose_table,
            },
            "taxonomy": {
                "category": category_table,
                "attribute": attribute_table,
                "visibility": visibility_table,
            },
        }

    def database_sequence_to_json(self, save_path):
        save_path = os.path.join(save_path, "v1.0-all")
        # vehicle sequence to json
        self.nuscenes_databse_dict["vehicle"]["log"].sequence_to_json(
            save_path, "log.json"
        )
        self.nuscenes_databse_dict["vehicle"]["map"].sequence_to_json(
            save_path, "map.json"
        )
        self.nuscenes_databse_dict["vehicle"]["calibrated_sensor"].sequence_to_json(
            save_path, "calibrated_sensor.json"
        )
        self.nuscenes_databse_dict["vehicle"]["sensor"].sequence_to_json(
            save_path, "sensor.json"
        )

        # extraction sequence to json
        if self.nuscenes_databse_dict["extraction"]["scene"]:
            self.nuscenes_databse_dict["extraction"]["scene"].sequence_to_json(
                save_path, "scene.json"
            )

        if self.nuscenes_databse_dict["extraction"]["sample"]:
            self.nuscenes_databse_dict["extraction"]["sample"].sequence_to_json(
                save_path, "sample.json"
            )

        if self.nuscenes_databse_dict["extraction"]["sample_data"]:
            self.nuscenes_databse_dict["extraction"]["sample_data"].sequence_to_json(
                save_path, "sample_data.json"
            )

        if self.nuscenes_databse_dict["extraction"]["ego_pose"]:
            self.nuscenes_databse_dict["extraction"]["ego_pose"].sequence_to_json(
                save_path, "ego_pose.json"
            )

        # taxonomy sequence to json
        self.nuscenes_databse_dict["taxonomy"]["category"].sequence_to_json(
            save_path, "category.json"
        )
        self.nuscenes_databse_dict["taxonomy"]["attribute"].sequence_to_json(
            save_path, "attribute.json"
        )
        self.nuscenes_databse_dict["taxonomy"]["visibility"].sequence_to_json(
            save_path, "visibility.json"
        )

        # annotation sequence to json
        # Note : 目前没有该部分内容 需要等待标注数据的导入 这里先保存一个空的json文件
        instance_path = os.path.join(save_path, "instance.json")
        sample_annotation_path = os.path.join(save_path, "sample_annotation.json")
        with open(instance_path, "w") as f:
            f.write("[]")
        with open(sample_annotation_path, "w") as f:
            f.write("[]")

    def get_calib_info_from_bag(self):
        """从bag包中获取标定信息,同时修正 bag 中 lidar 的真实topic与config中的topic的差异

        Args:
            bag_path (str): bag包路径
            camera_topic_channel_dict (dict): camera topic 和 channel 的映射
            lidar_topic_channel_dict (dict): lidar topic 和 channel 的映射

        Returns:
            calib_info_dict (dict): 标定信息字典
            {
                "topic0": CalibInfo(),
                "topic1": CalibInfo(),
            }

            calib_info = CalibInfo(
                        channel=channel,
                        translation=translation,
                        rotation=rotation,
                        camera_info=camera_info,
                    )
        """

        calib_info_dict = {}

        # rename for easy use
        bag_path = self.scene_bag_file

        # 1. init all sensor calib info
        # 1.1 init lidar-fusion calib info
        lidar_fusion_calib_info = CalibInfo(
            channel="lidar-fusion",
            translation=[0, 0, 0],
            rotation=[1, 0, 0, 0],
            camera_info={},
        )
        calib_info_dict["lidar-fusion"] = lidar_fusion_calib_info

        # 1.2 init camera calib info
        for camera_channel in self.camera_topic_channel_dict.values():
            camera_calib_info = CalibInfo(
                channel=camera_channel,
                translation=[0, 0, 0],
                rotation=[1, 0, 0, 0],
                camera_info="default",
            )
            calib_info_dict[camera_channel] = camera_calib_info

        # 2. update calib info by parse tf info from bag
        # 2.1 get real lidar channel list by parse lidar topic from bag
        lidar_real_topic_channel_dict = {}
        lidar_real_channel_list = []
        # open bag and if error, raise bag_path
        try:
            with rosbag.Bag(bag_path, "r") as bag:
                for topic, msg, t in bag.read_messages():
                    if topic in self.lidar_topic_channel_dict:
                        lidar_channel = self.lidar_topic_channel_dict[topic]
                        lidar_real_channel_list.append(lidar_channel)
                        lidar_real_topic_channel_dict[topic] = lidar_channel
        except rosbag.bag.ROSBagException as e:
            raise RuntimeError(f"Failed to open bag file {bag_path}: {str(e)}")

        # update lidar_topic_channel_dict
        self.lidar_topic_channel_dict = lidar_real_topic_channel_dict

        # 2.2 lidar need get true calib info by parse tf info from bag
        lidar_calib_info_dict = {}
        for lidar_channel in lidar_real_channel_list:
            lidar_calib_info_dict[lidar_channel] = None
        with rosbag.Bag(bag_path, "r") as bag:
            for topic, msg, t in bag.read_messages():
                # check lidar_calib_info_dict is all filled
                if all(lidar_calib_info_dict.values()):
                    break
                if topic == "/tf_static":
                    lidar_topic = msg.child_frame_id
                    lidar_channel = self.lidar_topic_channel_dict[lidar_topic]
                    if lidar_topic in self.lidar_topic_channel_dict:
                        translation = [
                            msg.transform.translation.x,
                            msg.transform.translation.y,
                            msg.transform.translation.z,
                        ]
                        rotation = [
                            msg.transform.rotation.w,
                            msg.transform.rotation.x,
                            msg.transform.rotation.y,
                            msg.transform.rotation.z,
                        ]
                        lidar_calib_info = CalibInfo(
                            channel=lidar_channel,
                            translation=translation,
                            rotation=rotation,
                            camera_info={},
                        )
                        lidar_calib_info_dict[lidar_channel] = lidar_calib_info

        # 3. check lidar_calib_info_dict is all filled
        if not all(lidar_calib_info_dict.values()):
            raise ValueError("lidar calib info not all filled")
        calib_info_dict.update(lidar_calib_info_dict)

        return calib_info_dict

    def parse_bag(self):
        """从rosbag中提取数据,因为解析包耗时非常久,需要保证一次性读取完所有后续所需数据
        所需获取数据包括:
            - calib_info_dict : 标定信息字典,需要构建一个字典, key为channel, value为CalibInfo,
                其中key包括所有camera和所有lidar的channel,以及 lidar-fusion 为key的channel
            - 在bag中真实的lidar channel 和 camera channel,
                因为在config中的channel是一个最大的集合,可能真实的bag中并不包含所有的channel
            - data_by_topic : 从bag中提取的数据,以topic为key,value为时间戳和msg的字典
        """
        bag_path = self.scene_bag_file

        # 1. 构建基本的数据结构
        topic_list = []
        topic_list.extend(self.camera_topic_channel_dict.keys())
        topic_list.extend(self.lidar_topic_channel_dict.keys())
        topic_list.extend(self.pose_topic_channel_dict.keys())

        # 获取默认的标定信息字典
        calib_info_dict = self.get_default_calib_info_dict()
        lidar_real_topic_channel_dict = {}
        data_by_topic = {}

        # 2. 从bag中提取数据
        print("start parse bag")
        try:
            with rosbag.Bag(bag_path, "r") as bag:
                for topic, msg, t in bag.read_messages():
                    # get all data by topic
                    if topic not in data_by_topic:
                        data_by_topic[topic] = {}
                    timestamp_us = ros_timestamp_to_us(t)
                    data_by_topic[topic][timestamp_us] = msg

                    # update lidar_topic_channel_dict
                    if topic in self.lidar_topic_channel_dict:
                        lidar_channel = self.lidar_topic_channel_dict[topic]
                        lidar_real_topic_channel_dict[topic] = lidar_channel

                    # get tf info
                    if topic == "/tf_static":
                        lidar_topic = msg.child_frame_id
                        lidar_channel = self.lidar_topic_channel_dict[lidar_topic]
                        if lidar_topic in self.lidar_topic_channel_dict:
                            translation = [
                                msg.transform.translation.x,
                                msg.transform.translation.y,
                                msg.transform.translation.z,
                            ]
                            rotation = [
                                msg.transform.rotation.w,
                                msg.transform.rotation.x,
                                msg.transform.rotation.y,
                                msg.transform.rotation.z,
                            ]
                            lidar_calib_info = CalibInfo(
                                channel=lidar_channel,
                                translation=translation,
                                rotation=rotation,
                                camera_info={},
                            )
                            calib_info_dict[lidar_channel] = lidar_calib_info
        except rosbag.bag.ROSBagException as e:
            raise RuntimeError(f"Failed to open bag file {bag_path}: {str(e)}")
        print("finish parse bag")
        # 3. update
        # 3.1 use lidar_real_topic_channel_dict replace lidar_topic_channel_dict
        self.lidar_topic_channel_dict = lidar_real_topic_channel_dict
        # 3.2 update calib_info_dict (remove info which is none)
        self.calib_info_dict = {
            key: value for key, value in calib_info_dict.items() if value
        }
        # 3.3 update data_by_topic
        self.data_by_topic = data_by_topic

    def get_default_calib_info_dict(self):
        """获取默认的标定信息字典
        默认的标定信息字典key包括:
            - lidar-fusion : 融合后的lidar channel
            - 所有lidar的channel
            - 所有camera的channel

        上述的channel信息从data_config中获取,但是在真是的bagz中可能并不包含所有的channel信息,
        还需要后续对这些channel信息进行修正

        """
        calib_info_dict = {}

        # - lidar-fusion calib info : use default value
        lidar_fusion_calib_info = CalibInfo(
            channel="lidar-fusion",
            translation=[0, 0, 0],
            rotation=[1, 0, 0, 0],
            camera_info={},
        )
        calib_info_dict["lidar-fusion"] = lidar_fusion_calib_info

        # - all lidar calib info : use default value
        for lidar_channel in self.lidar_topic_channel_dict.values():
            lidar_calib_info = None
            calib_info_dict[lidar_channel] = lidar_calib_info

        # - all camera calib info
        for camera_channel in self.camera_topic_channel_dict.values():
            camera_calib_info = CalibInfo(
                channel=camera_channel,
                translation=[0, 0, 0],
                rotation=[1, 0, 0, 0],
                camera_info="default",
            )
            calib_info_dict[camera_channel] = camera_calib_info

        return calib_info_dict
