"""
Author: wind windzu1@gmail.com
Date: 2023-10-24 18:58:26
LastEditors: wind windzu1@gmail.com
LastEditTime: 2023-11-10 20:22:15
Description:
Copyright (c) 2023 by windzu, All Rights Reserved.
"""

import os

import cv2
import numpy as np

from . import rule
from .utils import save_to_json
from .rule import get_car_id_from_scene_name


class LogTable:
    """LogTable 是 Log 的集合, 用于存储 Log 的信息 与 scene 一一对应
    token : uuid(${car_id}-${scene_id}-log)
    """

    def __init__(self, scene_name, logfile, date_captured, location):
        self.scene_name = scene_name
        self.log_list = [
            Log(
                scene_name=scene_name,
                logfile=logfile,
                date_captured=date_captured,
                location=location,
            )
        ]

    def sequence_to_json(self, path, filename):
        result = []
        for log in self.log_list:
            result.append(log.sequence_to_json())

        save_to_json(result, path, filename)


class Log:
    def __init__(self, scene_name, logfile, date_captured, location):
        self.scene_name = scene_name
        self.token = rule.generate_log_token(scene_name)

        self.logfile = logfile
        self.vehicle = get_car_id_from_scene_name(scene_name)
        self.date_captured = str(date_captured)
        self.location = str(location)

        self.result = {
            "token": self.token,
            "logfile": self.logfile,
            "vehicle": self.vehicle,
            "date_captured": self.date_captured,
            "location": self.location,
        }

    def sequence_to_json(self):
        return self.result


class MapTable:
    """_summary_ : MapTable 是 Map 的集合, 用于存储 Map 的信息 与 map filename 一一对应
    map_token : uuid(${filename})
    """

    def __init__(self, scene_name, map_name=None, category="default"):
        self.scene_name = scene_name
        self.map_list = [Map(scene_name_list=[scene_name], map_name=map_name)]
        self.filename = self.map_list[0].filename

    def sequence_to_json(self, path, filename):
        result = []
        for map in self.map_list:
            map.save_fake_map(os.path.dirname(path))
            result.append(map.sequence_to_json())

        save_to_json(result, path, filename)


class Map:
    def __init__(
        self,
        scene_name_list,
        map_name,
        category="default",
    ):
        self.scene_name_list = scene_name_list

        if not map_name:
            map_name = "suzhou"
        self.map_name = map_name
        self.token = rule.generate_map_token(self.map_name)
        self.filename = "maps/" + self.token + ".png"

        self.log_tokens = [
            rule.generate_log_token(scene_name) for scene_name in scene_name_list
        ]
        self.category = category

    def sequence_to_json(self):
        result = {
            "filename": self.filename,
            "token": self.token,
            "log_tokens": self.log_tokens,
            "category": self.category,
        }
        return result

    def save_fake_map(self, path):
        # use cv create a png file
        random_map = np.random.randint(0, 255, (100, 100, 3), dtype=np.uint8)
        cv2.imwrite(os.path.join(path, self.filename), random_map)


class CalibratedSensorTable:
    """_summary_ : CalibratedSensorTable 是 CalibratedSensor 的集合,
    在nuscens中 一个scene重新标定一次传感器 所以与 scene 对应,并且有多个传感器 还需要与channel相关联

    token : uuid(${scene_name}-calibrated_sensor-${channel})
    """

    def __init__(self, scene_name, calibrated_sensor_info_list):
        self.scene_name = scene_name
        self.calibrated_sensor_info_list = calibrated_sensor_info_list
        self.calibrated_sensor_list = self._calibrated_sensor_list_init()

    def _calibrated_sensor_list_init(self):
        calibrated_sensor_list = []
        for calibrated_sensor_info in self.calibrated_sensor_info_list:
            channel = calibrated_sensor_info["channel"]
            translation = calibrated_sensor_info["translation"]
            rotation = calibrated_sensor_info["rotation"]
            camera_intrinsic = calibrated_sensor_info["camera_intrinsic"]
            calibrated_sensor = CalibratedSensor(
                self.scene_name,
                channel,
                translation,
                rotation,
                camera_intrinsic,
            )
            calibrated_sensor_list.append(calibrated_sensor)
        return calibrated_sensor_list

    def sequence_to_json(self, path, filename):
        result = []
        for calibrated_sensor in self.calibrated_sensor_list:
            result.append(calibrated_sensor.sequence_to_json())

        save_to_json(result, path, filename)


class CalibratedSensor:
    def __init__(
        self,
        scene_name,
        channel,
        translation,
        rotation,
        camera_intrinsic,
    ):
        self.scene_name = scene_name
        self.channel = channel
        self.token = rule.generate_calibrated_sensor_token(
            self.scene_name,
            self.channel,
        )

        self.sensor_token = rule.generate_sensor_token(self.channel)
        self.translation = translation
        self.rotation = rotation
        self.camera_intrinsic = camera_intrinsic

    def sequence_to_json(self):
        translation = (
            self.translation.tolist()
            if isinstance(self.translation, np.ndarray)
            else self.translation
        )
        rotation = (
            self.rotation.tolist()
            if isinstance(self.rotation, np.ndarray)
            else self.rotation
        )
        camera_intrinsic = (
            self.camera_intrinsic.tolist()
            if isinstance(self.camera_intrinsic, np.ndarray)
            else self.camera_intrinsic
        )
        if camera_intrinsic is None:
            camera_intrinsic = []

        result = {
            "token": self.token,
            "sensor_token": self.sensor_token,
            "translation": translation,
            "rotation": rotation,
            "camera_intrinsic": camera_intrinsic,
            "channel": self.channel,
        }
        return result


class SensorTable:
    """_summary_ : SensorTable 是 Sensor 的集合, 用于存储 Sensor 的信息 与 channel 一一对应
    token : uuid(${channel})
    """

    def __init__(self, sensor_info_list):
        self.sensor_info_list = sensor_info_list
        self.sensor_list = []

        for sensor_info in self.sensor_info_list:
            sensor = Sensor(sensor_info["channel"], sensor_info["modality"])
            self.sensor_list.append(sensor)

    def sequence_to_json(self, path, filename):
        result = []
        for sensor in self.sensor_list:
            result.append(sensor.sequence_to_json())

        save_to_json(result, path, filename)


class Sensor:
    def __init__(self, channel, modality):
        # Note : sensor token does not need prefix to ensure uniqueness

        self.channel = channel
        self.modality = modality
        self.token = rule.generate_sensor_token(self.channel)

        self.result = {
            "token": self.token,
            "channel": self.channel,
            "modality": self.modality,
        }

    def sequence_to_json(self):
        return self.result
