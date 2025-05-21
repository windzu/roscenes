"""
Author: wind windzu1@gmail.com
Date: 2023-10-24 18:58:26
LastEditors: wind windzu1@gmail.com
LastEditTime: 2023-11-10 20:25:38
Description: 
Copyright (c) 2023 by windzu, All Rights Reserved. 
"""
import os
from typing import List

import numpy as np

from . import rule
from .utils import save_to_json


class SceneTable:
    def __init__(self, scene_name, samples_timestamp_list, description):
        self.scene_name = scene_name
        # Note : 一个scene只有一个Scene记录
        self.scene_list = [
            Scene(
                scene_name=self.scene_name,
                samples_timestamp_list=samples_timestamp_list,
                name=self.scene_name,
                description=description,
            )
        ]

    def sequence_to_json(self, path, filename):
        result = []
        for scene in self.scene_list:
            result.append(scene.sequence_to_json())

        save_to_json(result, path, filename)


class Scene:
    """_summary_ : 一个scene包含多个sample, 通过 car_id 和 scene_id 来确定
    token : uuid(${car_id}-${scene_id}-scene)
    """

    def __init__(
        self,
        scene_name,
        samples_timestamp_list,
        name,
        description,
    ):
        self.scene_name = scene_name
        self.samples_timestamp_list = samples_timestamp_list

        self.token = rule.generate_scene_token(self.scene_name)
        self.log_token = rule.generate_log_token(self.scene_name)

        if name is None:
            name = scene_name
        self.name = name
        if not description:
            description = ""
        self.description = description
        self.nbr_samples = len(self.samples_timestamp_list)
        self.first_sample_token = rule.generate_sample_token(
            self.scene_name,
            self.samples_timestamp_list[0],
        )
        self.last_sample_token = rule.generate_sample_token(
            self.scene_name,
            self.samples_timestamp_list[-1],
        )

        self.result = {
            "token": self.token,
            "name": self.name,
            "description": str(self.description),
            "log_token": self.log_token,
            "nbr_samples": self.nbr_samples,
            "first_sample_token": self.first_sample_token,
            "last_sample_token": self.last_sample_token,
        }

    def sequence_to_json(self):
        return self.result


class SampleTable:
    """SampleTable 是一个 Sample 的集合, 通过 car_id 和 scene_id 来确定
    而对于每一个具体的 sample, 再通过 timestamp 来确定sample_token
    sample_token =  uuid(${car_id}-${scene_id}-sample-${timestamp})
    """

    def __init__(self, scene_name, samples_timestamp_list):
        self.scene_name = scene_name
        self.samples_timestamp_list = samples_timestamp_list
        self.sample_list = self._sample_list_init()

    def _sample_list_init(self):
        sample_list = []

        # Note : 这里的5是一个magic number, 用来保证sample_list的长度大于5
        if len(self.samples_timestamp_list) < 5:
            return sample_list

        for i, timestamp in enumerate(self.samples_timestamp_list):
            prev_timestamp = None
            next_timestamp = None
            if i == 0:
                prev_timestamp = None
                next_timestamp = self.samples_timestamp_list[i + 1]
            elif i == len(self.samples_timestamp_list) - 1:
                prev_timestamp = self.samples_timestamp_list[i - 1]
                next_timestamp = None
            else:
                prev_timestamp = self.samples_timestamp_list[i - 1]
                next_timestamp = self.samples_timestamp_list[i + 1]
            sample = Sample(
                self.scene_name,
                timestamp,
                prev_timestamp,
                next_timestamp,
            )
            sample_list.append(sample)

        return sample_list

    def sequence_to_json(self, path, filename):
        result = []
        for sample in self.sample_list:
            result.append(sample.sequence_to_json())

        save_to_json(result, path, filename)


class Sample:
    def __init__(
        self,
        scene_name,
        timestamp,
        prev_timestamp,
        next_timestamp,
    ):
        self.scene_name = scene_name
        self.timestamp = timestamp

        self.token = rule.generate_sample_token(
            self.scene_name,
            self.timestamp,
        )
        self.scene_token = rule.generate_scene_token(self.scene_name)
        self.prev = ""
        if prev_timestamp is not None:
            self.prev = rule.generate_sample_token(
                self.scene_name,
                prev_timestamp,
            )
        self.next = ""
        if next_timestamp is not None:
            self.next = rule.generate_sample_token(
                self.scene_name,
                next_timestamp,
            )

        self.result = {
            "token": self.token,
            "scene_token": self.scene_token,
            "timestamp": self.timestamp,
            "prev": self.prev,
            "next": self.next,
        }

    def sequence_to_json(self):
        return self.result


class SampleDataTable:
    """SampleDataTable 是一个 SampleData 的集合, 通过 car_id 和 scene_id 来确定
    而对于每一个具体的 sample_data, 再通过 timestamp 和 channel 来确定sample_token
    sample_data_token =  uuid(${car_id}-${scene_id}-sample_data-${timestamp}-${channel})
    """

    def __init__(self, scene_name, sample_data_info_list_dict):
        self.scene_name = scene_name
        self.sample_data_info_list_dict = sample_data_info_list_dict
        self.sample_data_list = self._sample_data_list_init()

    def _sample_data_list_init(self):
        sample_data_list = []

        for channel, sample_data_info_list in self.sample_data_info_list_dict.items():
            last_sample_timestamp = None
            current_sample_timestamp = None

            for i, sample_data_info in enumerate(sample_data_info_list):
                scene_name = self.scene_name
                timestamp = sample_data_info["timestamp"]
                channel = sample_data_info["channel"]
                filename = sample_data_info["filename"]
                fileformat = sample_data_info["fileformat"]
                width = sample_data_info["width"]
                height = sample_data_info["height"]

                # judge is_key_frame
                is_key_frame = sample_data_info["is_key_frame"]
                if is_key_frame:
                    current_sample_timestamp = timestamp
                    last_sample_timestamp = current_sample_timestamp
                else:
                    current_sample_timestamp = last_sample_timestamp

                # judge prev_timestamp and next_timestamp
                if i == 0:
                    prev_timestamp = None
                    next_timestamp = sample_data_info_list[i + 1]["timestamp"]
                elif i == len(sample_data_info_list) - 1:
                    prev_timestamp = sample_data_info_list[i - 1]["timestamp"]
                    next_timestamp = None
                else:
                    prev_timestamp = sample_data_info_list[i - 1]["timestamp"]
                    next_timestamp = sample_data_info_list[i + 1]["timestamp"]

                sample_data = SampleData(
                    scene_name,
                    timestamp,
                    channel,
                    filename,
                    fileformat,
                    width,
                    height,
                    is_key_frame,
                    current_sample_timestamp,
                    prev_timestamp,
                    next_timestamp,
                )
                sample_data_list.append(sample_data)
        return sample_data_list

    def sequence_to_json(self, path, filename):
        result = []
        for sample_data in self.sample_data_list:
            result.append(sample_data.sequence_to_json())

        save_to_json(result, path, filename)


class SampleData:
    """_summary_ : 一个sample_data包含一个sample, 一个ego_pose, 一个calibrated_sensor

    sample_data_token =  uuid(${car_id}-${scene_id}-sample_data-${timestamp}-${channel})
    """

    def __init__(
        self,
        scene_name,
        timestamp,
        channel,
        filename,
        fileformat,
        width,
        height,
        is_key_frame,
        sample_timestamp,
        prev_timestamp,
        next_timestamp,
    ):
        self.scene_name = scene_name
        self.timestamp = int(timestamp)
        self.channel = channel
        self.token = rule.generate_sample_data_token(
            self.scene_name,
            self.timestamp,
            self.channel,
        )

        self.sample_token = rule.generate_sample_token(
            self.scene_name,
            sample_timestamp,
        )
        self.ego_pose_token = rule.generate_ego_pose_token(
            self.scene_name,
            self.timestamp,
        )
        self.calibrated_sensor_token = rule.generate_calibrated_sensor_token(
            self.scene_name,
            self.channel,
        )

        if is_key_frame:
            # filename should be samples/channel/xxx
            # check if filename have samples/channel
            if "samples" not in filename:
                filename = filename.split("/")[-1]
                filename = os.path.join("samples", channel, filename)
        else:
            # filename should be sweeps/channel/xxx
            # check if filename have sweeps/channel
            if "sweeps" not in filename:
                filename = filename.split("/")[-1]
                filename = os.path.join("sweeps", channel, filename)
        self.filename = filename

        self.fileformat = fileformat
        self.width = width
        self.height = height

        self.is_key_frame = is_key_frame
        self.next = ""
        if next_timestamp is not None:
            self.next = rule.generate_sample_data_token(
                self.scene_name,
                next_timestamp,
                self.channel,
            )
        self.prev = ""
        if prev_timestamp is not None:
            self.prev = rule.generate_sample_data_token(
                self.scene_name,
                prev_timestamp,
                self.channel,
            )

        self.result = {
            "token": self.token,
            "sample_token": self.sample_token,
            "ego_pose_token": self.ego_pose_token,
            "calibrated_sensor_token": self.calibrated_sensor_token,
            "filename": self.filename,
            "fileformat": self.fileformat,
            "width": self.width,
            "height": self.height,
            "timestamp": self.timestamp,
            "is_key_frame": self.is_key_frame,
            "next": self.next,
            "prev": self.prev,
        }

    def sequence_to_json(self):
        return self.result


class EgoPoseTable:
    """EgoPoseTable 是一个 EgoPose 的集合, 通过 car_id 和 scene_id 来确定
    而对于每一个具体的 ego_pose, 再通过 timestamp 来确定token
    ego_pose_token =  uuid(${car_id}-${scene_id}-ego_pose-${timestamp})
    """

    def __init__(self, scene_name, ego_pose_info_list):
        self.scene_name = scene_name
        self.ego_pose_info_list = ego_pose_info_list
        self.ego_pose_list: List[EgoPose] = []

        self._ego_pose_list_init()

    def _ego_pose_list_init(self):
        for ego_pose_info in self.ego_pose_info_list:
            timestamp = ego_pose_info["timestamp"]
            translation = ego_pose_info["translation"]
            rotation = ego_pose_info["rotation"]
            ego_pose = EgoPose(self.scene_name, timestamp, translation, rotation)
            self.ego_pose_list.append(ego_pose)

    def sequence_to_json(self, path, filename):
        result = []
        for ego_pose in self.ego_pose_list:
            result.append(ego_pose.sequence_to_json())

        save_to_json(result, path, filename)


class EgoPose:
    def __init__(self, scene_name, timestamp, translation, rotation):
        self.scene_name = scene_name
        self.timestamp = timestamp
        self.token = rule.generate_ego_pose_token(
            self.scene_name,
            self.timestamp,
        )

        self.translation = translation
        self.rotation = rotation

    def sequence_to_json(self):
        token = str(self.token)
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
        timestamp = self.timestamp

        result = {
            "token": token,
            "translation": translation,
            "rotation": rotation,
            "timestamp": timestamp,
        }
        return result
