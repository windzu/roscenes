import json

import numpy as np
import quaternion


class CalibInfo:
    """base calib info class for storing calib info of each sensor

    Args:
        channel (str): frame_id
        translation (list): translation of sensor , [x,y,z]
        rotation (list): rotation of sensor , [w,x,y,z]
        camera_info (dict): camera intrinsic of sensor
        camera_info format:
            {
                "fx": 0.0,
                "fy": 0.0,
                "cx": 0.0,
                "cy": 0.0,
                "model": "pinhole",
                "width": 0,
                "height": 0,
                "k1": 0.0,
                "k2": 0.0,
                "p1": 0.0,
                "p2": 0.0,
                "k3": 0.0,
            }

    """

    def __init__(self, channel, translation, rotation, camera_info):
        self.channel = channel  # frame_id
        self.translation = translation
        self.rotation = rotation
        self.camera_info = camera_info
        self.camera_intrinsic = None

        # camera_info 是可选项,分为三种情况
        # - None  or {}: 无内参,只有外参,例如 lidar , radar 就没有内参
        # - default : 有内参,但是没有具体内容,例如 camera , 但是没有标定,则使用默认值
        # - 具体值 : 有具体的内参,例如 camera , 并且有具体的标定值,此时需要检查内参是否合法

        if self.camera_info:
            if self.camera_info == "default":
                self.camera_intrinsic = self.generate_default_camera_intrinsic()
            else:
                self._camera_info_check()
                self.camera_intrinsic = self.get_camera_intrinsic(self.camera_info)

    def get_transform_matrix(self):
        """获取transform matrix
        Returns:
            np.ndarray: 4x4的transform matrix
        """
        transform_matrix = np.eye(4)

        translation = np.array(self.translation).reshape(3, 1)
        rotation = quaternion.from_float_array(self.rotation)
        rotation_matrix = quaternion.as_rotation_matrix(rotation)

        # combine rotation and translation to transform matrix
        transform_matrix[:3, :3] = rotation_matrix
        transform_matrix[:3, 3] = translation.flatten()

        transform_matrix = np.array(
            [[format(val, ".6f") for val in row] for row in transform_matrix],
            dtype=float,
        )

        return transform_matrix

    def _camera_info_check(self):
        """check camera intrinsic"""
        if self.camera_info is None:
            raise ValueError("camera intrinsic is None")

        def base_info_check(camera_info):
            """这里放宽了对于内参的检查
            - 放宽对 cx 和 cy 的检查 理论上两者应该大于0
            """

            if "fx" not in camera_info:
                raise ValueError("camera intrinsic should have fx")
            else:
                if camera_info["fx"] <= 0:
                    raise ValueError("camera intrinsic fx should be positive")
            if "fy" not in camera_info:
                raise ValueError("camera intrinsic should have fy")
            else:
                if camera_info["fy"] <= 0:
                    raise ValueError("camera intrinsic fy should be positive")
            if "cx" not in camera_info:
                raise ValueError("camera intrinsic should have cx")
            else:
                if camera_info["cx"] < 0:
                    raise ValueError("camera intrinsic cx should be positive")
            if "cy" not in camera_info:
                raise ValueError("camera intrinsic should have cy")
            else:
                if camera_info["cy"] < 0:
                    raise ValueError("camera intrinsic cy should be positive")

        def advanced_info_check(camera_info):
            # advanced info check
            # - model : pinhole , fisheye
            # - width,height
            # - k1,k2,p1,p2,k3

            # check model
            if "model" not in camera_info:
                raise ValueError("camera intrinsic should have model")
            else:
                if camera_info["model"] not in ["pinhole", "fisheye"]:
                    raise ValueError(
                        "camera intrinsic model should be pinhole or fisheye"
                    )

            # check width,height
            if "width" not in camera_info:
                raise ValueError("camera intrinsic should have width")
            else:
                if camera_info["width"] <= 0:
                    raise ValueError("camera intrinsic width should be positive")
            if "height" not in camera_info:
                raise ValueError("camera intrinsic should have height")
            else:
                if camera_info["height"] <= 0:
                    raise ValueError("camera intrinsic height should be positive")

            # check k1,k2,p1,p2,k3
            if "k1" not in camera_info:
                raise ValueError("camera intrinsic should have k1")
            if "k2" not in camera_info:
                raise ValueError("camera intrinsic should have k2")
            if "p1" not in camera_info:
                raise ValueError("camera intrinsic should have p1")
            if "p2" not in camera_info:
                raise ValueError("camera intrinsic should have p2")
            if "k3" not in camera_info:
                raise ValueError("camera intrinsic should have k3")

        # base info check
        # - fx,fy,cx,cy
        base_info_check(self.camera_info)

        # advanced info check
        # - model : pinhole , fisheye
        # - width,height
        # - k1,k2,p1,p2,k3
        # Note : tmp not use
        # advanced_info_check(self.camera_info)

    def get_camera_intrinsic(self, camera_info):
        """获取相机内参
        Note : 后续提供的内参格式可能会有变化，需要根据实际情况进行修改
        Returns:
            np.ndarray: 相机内参矩阵
        """
        camera_intrinsic = np.eye(3)
        camera_intrinsic[0, 0] = camera_info["fx"]
        camera_intrinsic[1, 1] = camera_info["fy"]
        camera_intrinsic[0, 2] = camera_info["cx"]
        camera_intrinsic[1, 2] = camera_info["cy"]

        return camera_intrinsic

    def __repr__(self):
        return f"CalibInfo(channel={self.channel}, transform_matrix={self.transform_matrix})"

    def get_extrinsic(self):
        """获取外参矩阵
        Returns:
            np.ndarray: 相机外参矩阵
        """
        return self.get_transform_matrix()

    def get_intrinsic(self):
        """获取内参矩阵
        Returns:
            np.ndarray: 相机内参矩阵
        """
        return self.camera_intrinsic

    @staticmethod
    def generate_default_camera_intrinsic():
        camera_intrinsic = np.eye(3)
        camera_intrinsic[0, 0] = 1280
        camera_intrinsic[1, 1] = 720
        camera_intrinsic[0, 2] = 640
        camera_intrinsic[1, 2] = 360
        return camera_intrinsic


class NuscenesCalibratedSensor:
    def __init__(self, path):
        self.path = path

        self.calib_info_dict = self.parse()

    def parse(self):
        """解析calibrated_sensor.json文件
        Returns:
            dict: 解析后的数据
        """
        calibrated_sensor_path = self.path
        with open(calibrated_sensor_path, "r") as f:
            calibrated_sensor = json.load(f)

        calib_info_dict = {}
        for sensor in calibrated_sensor:
            channel = sensor["channel"]
            translation = sensor["translation"]
            rotation = sensor["rotation"]
            camera_info = {}

            # check camera intrinsic if exist
            if sensor["camera_intrinsic"]:
                camera_intrinsic = sensor["camera_intrinsic"]

                camera_info["fx"] = camera_intrinsic[0][0]
                camera_info["fy"] = camera_intrinsic[1][1]
                camera_info["cx"] = camera_intrinsic[0][2]
                camera_info["cy"] = camera_intrinsic[1][2]

            calib_info_dict[channel] = CalibInfo(
                channel=channel,
                translation=translation,
                rotation=rotation,
                camera_info=camera_info,
            )

        return calib_info_dict

    def get_calib_info(self, channel):
        """获取指定channel的calib info
        Args:
            channel (str): frame_id
        Returns:
            CalibInfo: calib info
        """
        if channel not in self.calib_info_dict:
            raise ValueError(f"channel {channel} not in calib info dict")

        return self.calib_info_dict[channel]
