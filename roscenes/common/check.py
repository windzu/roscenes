import json
import os


from .constant import ERROR_MESSAGES, ErrorCode

target_sensor_list = [
    "cam-front-fisheye",
    "cam-left-fisheye",
    "cam-right-fisheye",
    "cam-back-fisheye",
    "lidar-fusion",
]


########### nuscenes data check ##########


def base_nuscenes_data_valid_check(path: str):
    """check base nuscenes data is valid

    - samples data check
    - v1.0-all base data check

    Args:
        path (str): nuscenes data path
    """

    # check samples data
    # - shoule have 5 sensors
    # - check file num in samples of ech sensor
    samples_data_path = os.path.join(path, "samples")
    if not os.path.exists(samples_data_path):
        raise ValueError("samples_data_path not exists: ", samples_data_path)

    sensor_list = os.listdir(samples_data_path)
    if len(sensor_list) != len(target_sensor_list):
        raise ValueError(path, "sensor num not equal 5")

    # check file num in samples of ech sensor
    sensor_file_num_dict = {}
    for sensor in sensor_list:
        sensor_data_path = os.path.join(samples_data_path, sensor)
        if not os.path.exists(sensor_data_path):
            raise ValueError("sensor_data_path not exists: ", sensor_data_path)
        file_num = len(os.listdir(sensor_data_path))
        if file_num == 0:
            raise ValueError("file_num == 0: ", sensor_data_path)
        sensor_file_num_dict[sensor] = file_num

    # compare file num in each sensor
    file_num_list = list(sensor_file_num_dict.values())
    if len(set(file_num_list)) != 1:
        raise ValueError(path, "file num not equal")

    # check v1.0-all base data
    # - should have 13 files
    # - token check

    v1_0_all_data_path = os.path.join(path, "v1.0-all")
    if not os.path.exists(v1_0_all_data_path):
        raise ValueError("v1.0-all_data_path not exists: ", v1_0_all_data_path)

    v1_0_all_file_num = len(os.listdir(v1_0_all_data_path))
    if v1_0_all_file_num != 13:
        raise ValueError(path, "v1.0-all file num not equal 13")

    # check token
    # - scene.json and sample.json
    scene_and_sample_token_check(path)

    return True


def nuscenes_data_valid_check(path: str):
    """check nuscenes data is valid
    - base nuscenes data check
    - sample_annotation.json check which should not be empty

    Args:
        path (str): nuscenes data path
    """
    # check base nuscenes data
    if not base_nuscenes_data_valid_check(path):
        raise ValueError(path, "base nuscenes data invalid")

    # check label data to make sure each sample has label
    sample_token_and_annotation_token_check_dict = {}
    sample_data_path = os.path.join(path, "v1.0-all", "sample.json")
    with open(sample_data_path, "r") as f:
        sample_data = json.load(f)
    for sample in sample_data:
        sample_token = sample["token"]
        sample_token_and_annotation_token_check_dict[sample_token] = []
    if len(sample_token_and_annotation_token_check_dict) == 0:
        raise ValueError(path, "sample size == 0")

    annotation_data_path = os.path.join(path, "v1.0-all", "sample_annotation.json")
    with open(annotation_data_path, "r") as f:
        annotation_data = json.load(f)
    if len(annotation_data) == 0:
        raise ValueError(path, "annotation size == 0")
    for annotation in annotation_data:
        sample_token = annotation["sample_token"]
        sample_token_and_annotation_token_check_dict[sample_token].append(
            annotation["token"]
        )
    for (
        sample_token,
        annotation_token_list,
    ) in sample_token_and_annotation_token_check_dict.items():
        if len(annotation_token_list) == 0:
            raise ValueError(path, "sample token has no annotation")

    return True


########## sus data check ##########
def sus_sensor_data_check(path: str):
    """check sus sensor data is valid

    - check sus data path
    - camera sensor num check
    - lidar sensor num check
    - files num in each sensor which should not be 0 and should be equal

    Args:
        path (str): sus data path
    """
    # 1. check sus data path
    if not os.path.exists(path):
        raise ValueError("sus_data_path not exists: ", path)

    # 2. camera sensor data check
    camera_sensor_data_path = os.path.join(path, "camera")
    if not os.path.exists(camera_sensor_data_path):
        raise ValueError(
            "camera_sensor_data_path not exists: ", camera_sensor_data_path
        )
    camera_sensor_list = os.listdir(camera_sensor_data_path)
    if not camera_sensor_list:
        raise ValueError("camera_sensor_list is empty: ", camera_sensor_data_path)

    # 3. fils num in each sensor which should not be 0 and should be equal
    lidar_sensor_data_path = os.path.join(path, "lidar")
    if not os.path.exists(lidar_sensor_data_path):
        raise ValueError("lidar_sensor_data_path not exists: ", lidar_sensor_data_path)

    lidar_file_num = len(os.listdir(lidar_sensor_data_path))
    if lidar_file_num == 0:
        raise ValueError("lidar file num == 0: ", lidar_sensor_data_path)
    for camera in camera_sensor_list:
        camera_data_path = os.path.join(camera_sensor_data_path, camera)
        camera_file_num = len(os.listdir(camera_data_path))
        if camera_file_num != lidar_file_num:
            raise ValueError(
                "camera file num not equal to lidar file num: ", camera_data_path
            )


def sus_label_data_check(path: str):
    """check sus label data is valid

    - check sus data path
    - label data check which should not be empty
    - label validation check rules
        - check each object scale which should not be less than 0.05
        - check each object have num_lidar_pts and should larger than 0

    Args:
        path (str): nuscenes data path

    Returns:
        dict: 异常信息字典 {label_file: [{错误信息及错误码}]}
    """
    # 1. check sus data path
    if not os.path.exists(path):
        raise ValueError("sus_data_path not exists: ", path)

    # 2. label data check
    label_data_path = os.path.join(path, "label")
    if not os.path.exists(label_data_path):
        raise ValueError("label_data_path not exists: ", label_data_path)
    label_file_list = os.listdir(label_data_path)

    # 3. label file check
    scene_abnormal_data = {}
    for label_file in label_file_list:
        label_file_path = os.path.join(label_data_path, label_file)
        abnormal_objects = []

        try:
            with open(label_file_path, "r") as f:
                labels = json.load(f)

                for obj_idx, obj in enumerate(labels):
                    # - 检查每个对象 scale 是否合法（小于0.05）
                    if "psr" in obj and "scale" in obj["psr"]:
                        scale = obj["psr"]["scale"]
                        # 检查任何维度是否小于0.05
                        if scale["x"] < 0.05 or scale["y"] < 0.05 or scale["z"] < 0.05:
                            # 记录异常对象的信息
                            abnormal_info = {
                                "error_code": ErrorCode.LABEL_SCALE_TOO_SMALL,
                                "obj_idx": obj_idx,
                                "obj_id": obj.get("obj_id", "unknown"),
                                "obj_type": obj.get("obj_type", "unknown"),
                                "scale": scale,
                            }
                            abnormal_objects.append(abnormal_info)

                    # - 检查每个对象是否有 num_lidar_pts 字段 并且点数是否大于0
                    if "num_lidar_pts" in obj:
                        num_lidar_pts = obj["num_lidar_pts"]
                        # 检查点数是否大于0
                        if num_lidar_pts <= 0:
                            # 记录异常对象的信息
                            abnormal_info = {
                                "error_code": ErrorCode.LABEL_NO_LIDAR_POINTS,
                                "obj_idx": obj_idx,
                                "obj_id": obj.get("obj_id", "unknown"),
                                "obj_type": obj.get("obj_type", "unknown"),
                                "num_lidar_pts": num_lidar_pts,
                            }
                            abnormal_objects.append(abnormal_info)
                    else:
                        # 如果没有 num_lidar_pts 字段，添加异常信息
                        abnormal_info = {
                            "error_code": ErrorCode.SUS_LABEL_MISSING_LIDAR_POINTS_FIELD,
                            "obj_idx": obj_idx,
                            "obj_id": obj.get("obj_id", "unknown"),
                            "obj_type": obj.get("obj_type", "unknown"),
                        }
                        abnormal_objects.append(abnormal_info)
        except json.JSONDecodeError:
            # 如果JSON解析失败，添加文件解析错误信息
            abnormal_objects.append(
                {
                    "error_code": ErrorCode.LABEL_JSON_PARSE_ERROR,
                    "error": "JSON parsing failed",
                }
            )

        # 如果找到异常对象，则添加到此标签文件的记录中
        if abnormal_objects:
            scene_abnormal_data[label_file] = abnormal_objects

    return scene_abnormal_data


def sus_data_check(path: str):
    """check sus data

    1. check sus data path
    2. camera sensor num check
    3. fils num in each sensor which should not be 0 and should be equal

    Args:
        path (str): sus data path
    """

    # 1. check sus data path
    if not os.path.exists(path):
        raise ValueError("sus_data_path not exists: ", path)

    # 2. sensor data check
    sus_sensor_data_check(path)

    # 3. label data check
    scene_abnormal_data = sus_label_data_check(path)

    return scene_abnormal_data


########## nuscenes data check ##########
def nuscenes_sensor_data_check(path: str):
    """check nuscenes sensor data is valid

    - check nuscenes data path
    - samples all sensor num check , can not be empty
    - sweeps all sensor num check , can be empty
    - files num in each sensor which should not be 0 and should be equal

    Args:
        path (str): nuscenes data path
    """
    # 1. check nuscenes data path
    if not os.path.exists(path):
        raise ValueError("nuscenes_data_path not exists: ", path)

    # 2. samples sensor data check
    samples_data_path = os.path.join(path, "samples")
    if not os.path.exists(samples_data_path):
        raise ValueError("samples_data_path not exists: ", samples_data_path)

    # 检查samples下的传感器数量
    sensor_list = os.listdir(samples_data_path)

    # 检查每个传感器的文件数量
    sensor_file_counts = {}
    for sensor in sensor_list:
        sensor_path = os.path.join(samples_data_path, sensor)
        if not os.path.isdir(sensor_path):
            continue

        files = os.listdir(sensor_path)
        file_count = len(files)
        if file_count == 0:
            raise ValueError(f"No files found for sensor: {sensor}")
        sensor_file_counts[sensor] = file_count

    # 检查所有传感器的文件数量是否一致
    file_counts = list(sensor_file_counts.values())
    if len(set(file_counts)) > 1:
        raise ValueError(
            f"Inconsistent file counts across sensors: {sensor_file_counts}"
        )

    # 3. sweeps sensor data check (如果存在)
    sweeps_data_path = os.path.join(path, "sweeps")
    sweep_file_counts = {}
    if os.path.exists(sweeps_data_path):
        sweep_sensor_list = os.listdir(sweeps_data_path)

        # 检查每个sweep传感器的文件数量
        for sensor in sweep_sensor_list:
            sensor_path = os.path.join(sweeps_data_path, sensor)
            if not os.path.isdir(sensor_path):
                continue

            files = os.listdir(sensor_path)
            file_count = len(files)
            sweep_file_counts[sensor] = file_count

    # 检查所有sweep传感器的文件数量是否一致
    if len(set(sweep_file_counts.values())) > 1:
        raise ValueError(
            f"Inconsistent sweep file counts across sensors: {sweep_file_counts}"
        )

    return True


def nuscenes_scene_and_sample_token_check(path: str):
    """scene and sample token check

    Args:
        path (str): nuscenes data path
    """
    # check path exists
    if not os.path.exists(path):
        raise ValueError("path not exists: ", path)
    scene_json_file_path = os.path.join(path, "v1.0-all", "scene.json")
    sample_json_file_path = os.path.join(path, "v1.0-all", "sample.json")

    # check file exists
    if not os.path.exists(scene_json_file_path):
        raise ValueError("scene_json_file_path not exists: ", scene_json_file_path)
    if not os.path.exists(sample_json_file_path):
        raise ValueError("sample_json_file_path not exists: ", sample_json_file_path)

    # get scene token , first sample token, last sample token
    with open(scene_json_file_path, "r") as f:
        scene_data = json.load(f)
    scene_token_list = [scene["token"] for scene in scene_data]
    first_sample_token_list = [scene["first_sample_token"] for scene in scene_data]
    last_sample_token_list = [scene["last_sample_token"] for scene in scene_data]

    # check list should be 1
    if len(set(first_sample_token_list)) != 1:
        raise ValueError("first_sample_token_list not equal 1")
    if len(set(last_sample_token_list)) != 1:
        raise ValueError("last_sample_token_list not equal 1")
    if len(set(scene_token_list)) != len(scene_token_list):
        raise ValueError("scene_token_list not unique")
    scene_token = scene_token_list[0]
    first_sample_token = first_sample_token_list[0]
    last_sample_token = last_sample_token_list[0]

    # get sample token list
    sample_token_list = []
    scene_token_list_from_sample = []
    with open(sample_json_file_path, "r") as f:
        sample_data = json.load(f)
    for sample in sample_data:
        sample_token_list.append(sample["token"])
        scene_token_list_from_sample.append(sample["scene_token"])

    # check
    # - sample token list should be unique
    # - first sample token should be the first sample token in scene
    # - last sample token should be the last sample token in scene
    # - scene_token_list_from_sample should be unique and equal to scene_token
    if len(set(sample_token_list)) != len(sample_token_list):
        raise ValueError("sample_token_list not unique")
    if first_sample_token != sample_token_list[0]:
        raise ValueError("first_sample_token not equal")
    if last_sample_token != sample_token_list[-1]:
        raise ValueError("last_sample_token not equal")
    if len(set(scene_token_list_from_sample)) != 1:
        raise ValueError("scene_token_list_from_sample not unique")
    if scene_token_list_from_sample[0] != scene_token:
        raise ValueError("scene_token_list_from_sample not equal to scene_token")

    return True


def nuscenes_label_data_check(path: str):
    """check nuscenes label data is valid

    - check nuscenes data path
    - label data check which should not be empty
    - label validation check rules
        - each sample should have label
        - check each object scale which should not be less than 0.05
        - check each object have num_lidar_pts and should larger than 0

    Args:
        path (str): nuscenes data path

    Returns:
        dict: 异常信息字典 {label_file: [{错误信息及错误码}]}
    """
    # 1. 检查nuscenes数据路径
    if not os.path.exists(path):
        raise ValueError("nuscenes_data_path not exists: ", path)

    # 2. 检查标注数据文件
    annotation_file_path = os.path.join(path, "v1.0-all", "sample_annotation.json")
    if not os.path.exists(annotation_file_path):
        raise ValueError("annotation_file_path not exists: ", annotation_file_path)

    # 3. 获取所有sample token
    sample_file_path = os.path.join(path, "v1.0-all", "sample.json")
    with open(sample_file_path, "r") as f:
        sample_data = json.load(f)

    sample_tokens = {sample["token"] for sample in sample_data}
    if not sample_tokens:
        raise ValueError("No samples found in sample.json")

    # 4. 读取和检查标注数据
    abnormal_data = {}
    try:
        with open(annotation_file_path, "r") as f:
            annotations = json.load(f)

        if not annotations:
            abnormal_data["sample_annotation.json"] = [
                {
                    "error_code": ErrorCode.NUSCENES_LABEL_NO_ANNOTATIONS,
                    "error": "No annotations found",
                }
            ]
            return abnormal_data

        # 统计每个sample的标注数量
        sample_annotation_counts = {token: 0 for token in sample_tokens}

        # 检查每个annotation
        for idx, annotation in enumerate(annotations):
            sample_token = annotation.get("sample_token")
            if sample_token in sample_tokens:
                sample_annotation_counts[sample_token] += 1

            # 收集此annotation的异常
            annotation_abnormal = []

            # 检查尺寸是否太小
            size = annotation.get("size", [0, 0, 0])
            if any(dim < 0.05 for dim in size):
                annotation_abnormal.append(
                    {
                        "error_code": ErrorCode.NUSCENES_LABEL_SCALE_TOO_SMALL,
                        "annotation_idx": idx,
                        "annotation_token": annotation.get("token", "unknown"),
                        "size": size,
                    }
                )

            # 检查激光雷达点数
            num_lidar_pts = annotation.get("num_lidar_pts", 0)
            if num_lidar_pts <= 0:
                annotation_abnormal.append(
                    {
                        "error_code": ErrorCode.NUSCENES_LABEL_NO_LIDAR_POINTS,
                        "annotation_idx": idx,
                        "annotation_token": annotation.get("token", "unknown"),
                        "num_lidar_pts": num_lidar_pts,
                    }
                )

            # 如果有异常，添加到结果中
            if annotation_abnormal:
                annotation_key = f"annotation_{annotation.get('token', idx)}"
                abnormal_data[annotation_key] = annotation_abnormal

        # 检查是否每个sample都有标注
        for sample_token, count in sample_annotation_counts.items():
            if count == 0:
                sample_key = f"sample_{sample_token}"
                abnormal_data[sample_key] = [
                    {
                        "error_code": ErrorCode.NUSCENES_SAMPLE_NO_ANNOTATIONS,
                        "sample_token": sample_token,
                        "error": "Sample has no annotations",
                    }
                ]

    except json.JSONDecodeError:
        abnormal_data["sample_annotation.json"] = [
            {
                "error_code": ErrorCode.NUSCENES_LABEL_JSON_PARSE_ERROR,
                "error": "JSON parsing failed",
            }
        ]

    return abnormal_data


def nuscenes_data_check(path: str):
    """check nuscenes data

    1. check nuscenes data path
    2. check sensor data
    3. check scene and sample token
    4. check label data

    Args:
        path (str): nuscenes data path

    Returns:
        bool: True if valid, False otherwise
    """
    # 1. check nuscenes data path
    if not os.path.exists(path):
        raise ValueError("nuscenes_data_path not exists: ", path)

    # 2. check sensor data
    if not nuscenes_sensor_data_check(path):
        raise ValueError(path, "sensor data invalid")

    # 3. check scene and sample token
    if not nuscenes_scene_and_sample_token_check(path):
        raise ValueError(path, "scene and sample token invalid")

    # 4. check label data
    scene_abnormal_data = nuscenes_label_data_check(path)

    return scene_abnormal_data


def scene_check(path: str):
    """check scene data

    1. check nuscenes data
    2. check sus data

    Args:
        path (str): nuscenes data path

    Returns:
        dict: 错误信息字典 {label_file: {error_code: count}}，如果没有错误则为空字典
    """
    error_info = {}

    ############ sus data check ############
    sus_data_path = os.path.join(path, "sus")
    try:
        scene_abnormal_data = sus_data_check(sus_data_path)
        if scene_abnormal_data:
            # 整理错误信息，保留原始错误码
            for label_file, abnormal_objects in scene_abnormal_data.items():
                # 统计不同类型的错误
                error_code_counts = {}

                # 将异常对象按错误码分组
                for obj in abnormal_objects:
                    error_code = obj.get("error_code", ErrorCode.GENERAL_ERROR)
                    error_code_counts[error_code] = (
                        error_code_counts.get(error_code, 0) + 1
                    )

                # 保存原始错误码和数量
                error_info[label_file] = error_code_counts
    except ValueError as e:
        # 对于ValueError异常，使用特殊错误码
        error_info["general_error"] = {ErrorCode.SUS_PATH_NOT_EXIST: 1}

    ############ nuscenes data check ############
    nuscenes_data_path = os.path.join(path, "nuscenes")
    try:
        scene_abnormal_data = nuscenes_data_check(nuscenes_data_path)
        if scene_abnormal_data:
            # 整理错误信息，保留原始错误码
            for label_file, abnormal_objects in scene_abnormal_data.items():
                # 统计不同类型的错误
                error_code_counts = {}

                # 将异常对象按错误码分组
                for obj in abnormal_objects:
                    error_code = obj.get("error_code", ErrorCode.GENERAL_ERROR)
                    error_code_counts[error_code] = (
                        error_code_counts.get(error_code, 0) + 1
                    )

                # 保存原始错误码和数量
                error_info[label_file] = error_code_counts
    except ValueError as e:
        # 对于ValueError异常，使用特殊错误码
        error_info["general_error"] = {ErrorCode.NUS_PATH_NOT_EXIST: 1}

    return error_info
