"""
Author: wind windzu1@gmail.com
Date: 2023-10-24 18:58:26
LastEditors: wind windzu1@gmail.com
LastEditTime: 2023-11-03 18:34:06
Description: 
Copyright (c) 2023 by windzu, All Rights Reserved. 
"""

from .utils import generate_uuid_from_input


def generate_filename(scene_name, channel, timestamp, suffix):
    filename = str(scene_name) + "_" + str(channel) + "_" + str(timestamp) + str(suffix)
    return filename


def parse_filename(filename):
    fileformat = filename.split(".")[-1]

    filename_without_suffix = filename.split(".")[0]

    def check_filename_without_suffix(filename_without_suffix):
        filename_without_suffix_list = filename_without_suffix.split("_")
        if len(filename_without_suffix_list) < 4:
            return False
        return True

    if not check_filename_without_suffix(filename_without_suffix):
        raise Exception("filename : {} is not valid.".format(filename))

    filename_without_suffix_list = filename_without_suffix.split("_")
    scene_name = filename_without_suffix_list[0] + "_" + filename_without_suffix_list[1]

    scene_id = get_scene_id_from_scene_name(scene_name)
    car_id = get_car_id_from_scene_name(scene_name)
    channel = filename_without_suffix_list[2]
    timestamp = int(filename_without_suffix_list[3])

    scene_name = scene_id + "_" + car_id

    return scene_name, channel, timestamp, fileformat


def get_scene_id_from_scene_name(scene_name):
    """获取场景id

    默认场景名称格式为scene_id_car_id,但是在一些特殊场景下,scene_name可能不符合这个格式
    所以需要做更多的校验以保障正确性

    scene_id 有一些规则,可以辅助判断 scene_id 是否符合规则:
    1. scene_id 由数字和 - 组成
    2. scene_id 以数字开头
    3. scene_id 以数字结尾
    4. scene_id 中间可以有 -
    5. scene_id 中不包含其他字符

    Args:
        scene_name (str): 场景名称
    """
    scene_name_list = scene_name.split("_")

    # check list length
    if len(scene_name_list) < 2:
        raise Exception("scene_name : {} is not valid.".format(scene_name))

    # default the first element is scene_id
    scene_id = scene_name_list[0]

    # check scene_id
    def check_scene_id(scene_id):
        scene_id_list = scene_id.split("-")
        for scene_id_item in scene_id_list:
            if not scene_id_item.isdigit():
                return False
        return True

    if check_scene_id(scene_id):
        return scene_id
    else:
        raise Exception("scene_id : {} is not valid.".format(scene_id))


def get_car_id_from_scene_name(scene_name: str):
    """parse car_id from scene_name

    scene_name : scene-id_car-id

    Args:
        scene_name (str): scene_name
    """
    scene_name_list = scene_name.split("_")

    # check list length
    if len(scene_name_list) != 2:
        raise Exception("scene_name : {} is not valid.".format(scene_name))

    return scene_name_list[1]

    # default the second element is car_id
    car_id = scene_name_list[1]

    # check car_id
    def check_car_id(car_id):
        car_id_list = car_id.split("-")
        # for car_id_item in car_id_list:
        #     if not car_id_item.isdigit() and not car_id_item.isalpha():
        #         return False
        # # check the last element is digit
        # if not car_id_list[-1].isdigit():
        #     return False
        # check the first element is strart with YC
        if not car_id_list[0].startswith("YC"):
            return False
        return True

    if check_car_id(car_id):
        return car_id
    else:
        raise Exception("car_id : {} is not valid.".format(car_id))


# vehicle
def generate_log_token(scene_name):
    """生成log token

    Args:
        scene_name (str): 数据采集场景名称

    Returns:
        str: log token
    """
    input = scene_name + "-log"
    return generate_uuid_from_input(input)


def generate_map_token(map_filename):
    return generate_uuid_from_input(map_filename)


def generate_calibrated_sensor_token(scene_name, channel):
    input = scene_name + "-calibrated_sensor-" + channel
    return generate_uuid_from_input(input)


def generate_sensor_token(channel):
    """生成sensor token

    Args:
        channel (str): 传感器channel名称

    Returns:
        str: sensor token
    """
    return generate_uuid_from_input(channel)


# extraction
def generate_scene_token(scene_name):
    input = scene_name + "-scene"
    return generate_uuid_from_input(input)


def generate_sample_token(scene_name, timestamp):
    input = scene_name + "-sample-" + str(timestamp)
    return generate_uuid_from_input(input)


def generate_sample_data_token(scene_name, timestamp, channel):
    input = scene_name + "-sample_data-" + str(timestamp) + "-" + channel
    return generate_uuid_from_input(input)


def generate_ego_pose_token(scene_name, timestamp):
    input = scene_name + "-ego_pose-" + str(timestamp)
    return generate_uuid_from_input(input)


# annotation
def generate_instance_token(scene_name, track_id):
    if track_id is None:
        return ""

    input = scene_name + "-instance-" + str(track_id)
    return generate_uuid_from_input(input)


def generate_sample_annotation_token(scene_name, timestamp, object_id):
    if timestamp is None or object_id is None:
        return ""

    input = scene_name + "-sample_annotation-" + str(timestamp) + "-" + str(object_id)
    return generate_uuid_from_input(input)


# taxonomy
def generate_category_token(category_name):
    return generate_uuid_from_input(category_name)


def generate_attribute_token(attribute_name):
    if attribute_name is None:
        return ""
    return generate_uuid_from_input(attribute_name)


def generate_visibility_token(visibility):
    if visibility == "v0-40":
        return 1
    elif visibility == "v40-60":
        return 2
    elif visibility == "v60-80":
        return 3
    elif visibility == "v80-100":
        return 4
    else:
        return 4
