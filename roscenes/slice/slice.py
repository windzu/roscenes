import os
from concurrent.futures import ProcessPoolExecutor, as_completed

from rich.progress import track

from ..nuscenes.nuscenes_info import NuscenesInfo


class Slice:
    """数据切片
    Args:
        path (str):配置文件路径
        data_info_list (list):数据信息列表, ech element is a dict, for example:
            [
                {
                    "scene_name": "0001-0_YC200A01-N1-0001",
                    "rosbag_file_path": "path/to/0001-0_YC200A01-N1-0001.bag",
                    "nuscenes_folder_path": "path/to/0001-0_YC200A01-N1-0001",
                    "start_time": None,
                    "end_time": None,
                    "bag_info": {
                        "map_name": "suzhou",
                        "description": "lidar data",
                        "date_captured": "1970-01-01"
                    }
                }
            ]

    """

    def __init__(self, config, data_info_list: list, max_workers: int = 4):
        self.config = config
        self.data_info_list = data_info_list
        self.max_workers = max_workers

        self._check_data_info_list()

    def _check_data_info_list(self):
        # check if data_info_list is a list
        if not isinstance(self.data_info_list, list):
            raise Exception("data_info_list should be a list")

        # check if data_info_list is not empty
        if not self.data_info_list:
            raise Exception("data_info_list should not be empty")

        # check if each element in data_info_list is a dict
        if not all(isinstance(data_info, dict) for data_info in self.data_info_list):
            raise Exception("each element in data_info_list should be a dict")

        # check if each element in data_info_list has the key : scene_name, rosbag_file_path, nuscenes_folder_path
        if not all(
            all(
                key in data_info
                for key in ["scene_name", "rosbag_file_path", "nuscenes_folder_path"]
            )
            for data_info in self.data_info_list
        ):
            raise Exception(
                "each element in data_info_list should have keys : scene_name, rosbag_file_path, nuscenes_folder_path"
            )

        # check rosabg_file_path
        for data_info in self.data_info_list:
            if not os.path.exists(data_info["rosbag_file_path"]):
                raise Exception(
                    f"rosbag file not exists : {data_info['rosbag_file_path']}"
                )
            if not data_info["rosbag_file_path"].endswith(".bag"):
                raise Exception(
                    f"rosbag file should end with .bag : {data_info['rosbag_file_path']}"
                )

        # check nuscenes_folder_path
        for data_info in self.data_info_list:
            if not os.path.exists(data_info["nuscenes_folder_path"]):
                os.makedirs(data_info["nuscenes_folder_path"])

        # check if each element in data_info_list has the key : bag_info
        # - if not, add default bag_info_dict
        # - if has, check if bag_info_dict has the key : map_name, description, date_captured
        if not all("bag_info" in data_info for data_info in self.data_info_list):
            for data_info in self.data_info_list:
                data_info["bag_info"] = {
                    "map_name": "suzhou",
                    "description": "lidar data",
                    "date_captured": "1970-01-01",
                }
        else:
            for data_info in self.data_info_list:
                if not all(
                    key in data_info["bag_info"]
                    for key in ["map_name", "description", "date_captured"]
                ):
                    raise Exception(
                        "bag_info_dict should have keys : map_name, description, date_captured"
                    )

        # check if each element in data_info_list has the key : start_time, end_time
        # - if not, add default start_time, end_time
        # - if has, check if start_time, end_time is None or a number
        for data_info in self.data_info_list:
            if "start_time" not in data_info:
                data_info["start_time"] = None
            if "end_time" not in data_info:
                data_info["end_time"] = None
            if data_info["start_time"] is not None and not isinstance(
                data_info["start_time"], (int, float)
            ):
                raise Exception("start_time should be None or a number")
            if data_info["end_time"] is not None and not isinstance(
                data_info["end_time"], (int, float)
            ):
                raise Exception("end_time should be None or a number")

    def slice(self):
        print(f"     slice bags with {self.max_workers} processes:")
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self.slice_bag,
                    data_info,
                ): data_info
                for data_info in self.data_info_list
            }
            for future in track(
                as_completed(futures),
                total=len(futures),
                description="slicing",
            ):
                future.result()  # 等待所有任务完成

        # for data_info in self.data_info_list:
        #     self.slice_bag(data_info)

    def slice_bag(self, data_info: dict):
        # 1. build nuscene info
        nuscene_info = NuscenesInfo(
            data_config=self.config,
            scene_name=data_info["scene_name"],
            scene_bag_file=data_info["rosbag_file_path"],
            nuscenes_folder_path=data_info["nuscenes_folder_path"],
            map_name=data_info["bag_info"]["map_name"],
            date_captured=data_info["bag_info"]["date_captured"],
            description=data_info["bag_info"]["description"],
            start_time=data_info["start_time"],
            end_time=data_info["end_time"],
        )
        nuscene_info.slice()
