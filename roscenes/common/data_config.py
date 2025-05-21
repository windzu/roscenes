import json
import os


from .calib import CalibInfo


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class DataConfig(metaclass=Singleton):
    def __init__(
        self,
        save_pcd_dims: int = 4,
        sample_interval: int = 5,
        save_sweep_data_flag: bool = True,
    ):

        self.save_pcd_dims = save_pcd_dims  # 保存点云的维度
        self.sample_interval = sample_interval  # 采样间隔
        self.save_sweep_data_flag = save_sweep_data_flag  # 是否保存sweep数据
        self.min_bag_duration = 20  # 设置每个bag包的最小时间长度

        self.main_topic = "/lidar_points/top"  # 时间同步的基础topic
        self.main_channel = "lidar-fusion"
        self.lidar_fusion_flag = True  # 是否融合lidar数据
        self.transform_lidar_flag = False  # 是否转换lidar数据

        self.time_diff_threshold = 50  # 时间同步阈值(ms)

        # 相机topic和channel对应关系
        self.camera_topic_channel_dict = {
            "/cam_front_fisheye/compressed": "cam-front-fisheye",
            "/cam_left_fisheye/compressed": "cam-left-fisheye",
            "/cam_right_fisheye/compressed": "cam-right-fisheye",
            "/cam_back_fisheye/compressed": "cam-back-fisheye",
        }

        # lidar topic和channel对应关系
        self.lidar_topic_channel_dict = {
            "/lidar_points/top": "lidar-top",
            "/lidar_points/front": "lidar-front",
            "/lidar_points/left": "lidar-left",
            "/lidar_points/right": "lidar-right",
            "/lidar_points/back": "lidar-back",
        }

        # 位姿topic和channel对应关系
        self.pose_topic_channel_dict = {
            "/localization_result": "ego-pose",
        }

    # 读取标定信息
    @staticmethod
    def parse_calib(calib_path, topic_channel_dict):
        """读取标定信息"""

        cars_calib_info_dict = {}

        if not os.path.exists(calib_path):
            raise Exception("Calib files folder is not exist.")

        # 遍历该文件夹下所有yaml文件
        # yaml 文件的文件名即为车辆名称(car_id)
        calib_files = []
        for dirpath, dirnames, filenames in os.walk(calib_path):
            for filename in filenames:
                if filename.endswith(".json"):
                    calib_files.append(os.path.join(dirpath, filename))

        for calib_file in calib_files:
            car_id = os.path.basename(calib_file).split(".")[0]
            with open(calib_file, "r") as file:
                calib_data = json.load(file)
                calib_info_dict = {}

                for calib in calib_data:
                    topic = calib["topic"]
                    if topic not in topic_channel_dict:
                        continue
                    channel = topic_channel_dict[topic]
                    # check calib info
                    # - must have transform , rotation , camera_info
                    # - camera_info can be null
                    if "transform" not in calib:
                        raise Exception("Calib file is not valid,missing transform.")
                    if "camera_info" not in calib:
                        raise Exception("Calib file is not valid,missing camera_info.")

                    transform = calib["transform"]

                    # check transform
                    if "translation" not in transform:
                        raise Exception("Calib file is not valid,missing translation.")
                    if "rotation" not in transform:
                        raise Exception("Calib file is not valid,missing rotation.")

                    translation = transform["translation"]
                    rotation = transform["rotation"]

                    # camera config parse
                    camera_info = calib["camera_info"]

                    calib_info = CalibInfo(
                        channel=channel,
                        translation=translation,
                        rotation=rotation,
                        camera_info=camera_info,
                    )

                    calib_info_dict[channel] = calib_info

                lidar_fusion_calib_info = CalibInfo(
                    channel="lidar-fusion",
                    translation=[0, 0, 0],
                    rotation=[1, 0, 0, 0],
                    camera_info={},
                )
                calib_info_dict["lidar-fusion"] = lidar_fusion_calib_info
                cars_calib_info_dict[car_id] = calib_info_dict

        return cars_calib_info_dict
