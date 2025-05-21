import os

from . import rule
from .utils import save_to_json


class SceneTable:
    def __init__(self, car_id, scene_id, samples_timestamp_list):
        self.car_id = car_id
        self.scene_id = scene_id
        # Note : 一个scene只有一个Scene记录
        self.scene_list = [Scene(self.car_id, self.scene_id, samples_timestamp_list)]

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
        name="default",
        description="default",
    ):
        self.scene_name = scene_name
        self.samples_timestamp_list = samples_timestamp_list

        self.token = rule.generate_scene_token(self.scene_name)
        self.log_token = rule.generate_log_token(self.scene_name)
        self.name = name
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
            "token": None,
            "name": None,
            "description": None,
            "log_token": None,
            "nbr_samples": None,
            "first_sample_token": None,
            "last_sample_token": None,
        }

    def sequence_to_json(self):
        if self.log_token is None:
            self.log_token = ""
        result = {
            "token": self.token,
            "name": self.name,
            "description": self.description,
            "log_token": self.log_token,
            "nbr_samples": self.nbr_samples,
            "first_sample_token": self.first_sample_token,
            "last_sample_token": self.last_sample_token,
        }
        return result


class InstanceTable:
    def __init__(self, instance_info_list):
        self.instance_list = []

        for instance_info in instance_info_list:
            self.instance_list.append(
                Instance(
                    instance_info["scene_name"],
                    instance_info["track_id"],
                    instance_info["category"],
                    instance_info["nbr_annotations"],
                    instance_info["first_annotation_timestamp"],
                    instance_info["first_annotation_object_id"],
                    instance_info["last_annotation_timestamp"],
                    instance_info["last_annotation_object_id"],
                )
            )

    def sequence_to_json(self, path, filename):
        result = []
        for instance in self.instance_list:
            result.append(instance.sequence_to_json())

        save_to_json(result, path, filename)


class Instance:
    """_summary_
    token : uuid(${car_id}-${scene_id}-instance-${track_id})
    """

    def __init__(
        self,
        scene_name,
        track_id,
        category,
        nbr_annotations,
        first_annotation_timestamp,
        first_annotation_object_id,
        last_annotation_timestamp,
        last_annotation_object_id,
    ):
        self.scene_name = scene_name
        self.track_id = track_id
        self.category = category
        self.nbr_annotations = nbr_annotations

        self.token = rule.generate_instance_token(self.scene_name, self.track_id)
        self.category_token = rule.generate_category_token(category)

        self.first_annotation_token = rule.generate_sample_annotation_token(
            self.scene_name,
            first_annotation_timestamp,
            first_annotation_object_id,
        )
        self.last_annotation_token = rule.generate_sample_annotation_token(
            self.scene_name,
            last_annotation_timestamp,
            last_annotation_object_id,
        )

        self.result = {
            "token": self.token,
            "category_token": self.category_token,
            "nbr_annotations": self.nbr_annotations,
            "first_annotation_token": self.first_annotation_token,
            "last_annotation_token": self.last_annotation_token,
        }

    def sequence_to_json(self):
        return self.result


class LidarsegTable:
    def __init__(self) -> None:
        pass


class Lidarseg:
    def __init__(self) -> None:
        pass


class SampleAnnotationTable:
    def __init__(self, sample_annotation_info_list):
        self.sample_annotation_list = []

        for sample_annotation_info in sample_annotation_info_list:
            sample_annotation = SampleAnnotation(
                sample_annotation_info["scene_name"],
                sample_annotation_info["timestamp"],
                sample_annotation_info["object_id"],
                sample_annotation_info["track_id"],
                sample_annotation_info["attribute_name_list"],
                sample_annotation_info["visibility"],
                sample_annotation_info["translation"],
                sample_annotation_info["size"],
                sample_annotation_info["rotation"],
                sample_annotation_info["num_lidar_pts"],
                sample_annotation_info["pre_timestamp"],
                sample_annotation_info["pre_object_id"],
                sample_annotation_info["next_timestamp"],
                sample_annotation_info["next_object_id"],
            )
            self.sample_annotation_list.append(sample_annotation)

    def sequence_to_json(self, path, filename):
        result = []
        for sample_annotation in self.sample_annotation_list:
            result.append(sample_annotation.sequence_to_json())

        save_to_json(result, path, filename)


class SampleAnnotation:
    """_summary_
    token : uuid(${car_id}-${scene_id}-sample_annotation-${timestamp}-${object_id})
    """

    def __init__(
        self,
        scene_name,
        timestamp,
        object_id,
        track_id,
        attribute_name_list,
        visibility,
        translation,
        size,
        rotation,
        num_lidar_pts,
        pre_timestamp,
        pre_object_id,
        next_timestamp,
        next_object_id,
    ):
        self.scene_name = scene_name
        self.timestamp = timestamp
        self.object_id = object_id
        self.track_id = track_id
        self.attribute_name_list = attribute_name_list
        self.visibility = visibility

        self.translation = translation
        self.size = size
        self.rotation = rotation

        self.num_lidar_pts = num_lidar_pts
        self.pre_timestamp = pre_timestamp
        self.pre_object_id = pre_object_id
        self.next_timestamp = next_timestamp
        self.next_object_id = next_object_id

        self.token = rule.generate_sample_annotation_token(
            self.scene_name,
            timestamp,
            object_id,
        )
        # debug
        if not self.token:
            print("scene_name", self.scene_name)
            print("timestamp", timestamp)
            print("object_id", object_id)
            print("token", self.token)

        self.sample_token = rule.generate_sample_token(self.scene_name, timestamp)
        self.instance_token = rule.generate_instance_token(self.scene_name, track_id)
        self.attribute_tokens = [
            rule.generate_attribute_token(attribute_name)
            for attribute_name in attribute_name_list
        ]
        self.visibility_token = rule.generate_visibility_token(visibility)

        self.pre_token = rule.generate_sample_annotation_token(
            self.scene_name,
            pre_timestamp,
            pre_object_id,
        )
        self.next_token = rule.generate_sample_annotation_token(
            self.scene_name,
            next_timestamp,
            next_object_id,
        )
        self.result = {
            "token": self.token,
            "sample_token": self.sample_token,
            "instance_token": self.instance_token,
            "attribute_tokens": self.attribute_tokens,
            "visibility_token": self.visibility_token,
            "translation": self.translation,
            "size": self.size,
            "rotation": self.rotation,
            "num_lidar_pts": self.num_lidar_pts,
            "num_radar_pts": 0,
            "prev": self.pre_token,
            "next": self.next_token,
        }

    def sequence_to_json(self):
        return self.result
