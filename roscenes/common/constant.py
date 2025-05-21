class SUSToNuscenesMap:
    obj_type_to_category_name_map = {
        ############################################
        # vehicle
        ## cycle
        "bicycle": None,  # 暂时弃用此类别 "vehicle.bicycle"
        "motorcycle": None,  # 暂时弃用此类别 "vehicle.motorcycle"
        "tricycle": None,  # nuscenes 暂无对应,暂做兼容处理 暂时弃用此类别 "vehicle.bicycle"
        "cycle_group": None,  # nuscenes 暂无对应,暂做兼容处理 暂时弃用此类别 "vehicle.bicycle"
        "rider": "vehicle.bicycle",  # nuscenes 暂无对应,暂做兼容处理
        "cycle": None,  # nuscenes 暂无对应,暂做兼容处理 暂时弃用此类别 "vehicle.bicycle"
        ## car
        "car": "vehicle.car",
        ## truck
        "van": "vehicle.truck",
        "pickup": "vehicle.truck",  # nuscenes 无对应,暂做兼容处理
        "cargo": "vehicle.truck",  # nuscenes 无对应,暂做兼容处理
        "truck": "vehicle.truck",
        "trailer": "vehicle.truck",  # 暂时将所有非car,bus的车统一为truck "vehicle.trailer"
        ## bus
        "micro_bus": "vehicle.bus.rigid",  # nuscenes 无对应,暂做兼容处理
        "mini_bus": "vehicle.bus.rigid",  # nuscenes 无对应,暂做兼容处理
        "bus": "vehicle.bus.rigid",
        ## construction
        "construction_vehicle": "vehicle.truck",  # 暂时将所有非car,bus的车统一为truck "vehicle.construction"
        ############################################
        # human
        "pedestrian": "human.pedestrian.adult",
        ############################################
        # animal
        "animal": "animal",  # !! nuscenes 暂无对应,暂不做兼容处理
        ############################################
        # static_object
        "barrier": "movable_object.barrier",
        "traffic_cone": "movable_object.trafficcone",
        "stone": "movable_object.trafficcone",  # nuscenes 暂无对应,暂做兼容处理
        "chair": None,  # !! nuscenes 暂无对应,暂不做兼容处理
        "trash_can": None,  # !! nuscenes 暂无对应,暂不做兼容处理
    }

    not_used_obj_type_list = [
        "chair",
        "trash_can",
    ]

    category_name_to_attribute_name_list_map = {
        "vehicle.car": ["vehicle.moving"],
        "vehicle.truck": ["vehicle.moving"],
        "vehicle.trailer": ["vehicle.parked"],
        "vehicle.bus.rigid": ["vehicle.moving"],
        "vehicle.construction": ["vehicle.parked"],
        "vehicle.bicycle": ["vehicle.parked"],
        "vehicle.motorcycle": ["vehicle.parked"],
        "human.pedestrian.adult": ["pedestrian.standing"],
        "movable_object.trafficcone": [],
        "movable_object.barrier": [],
    }

    extend_attribute_dict = {
        "stopped": "vehicle.stopped",
        "parked": "vehicle.parked",
        "with_rider": "cycle.with_rider",
        "without_rider": "cycle.without_rider",
        "sitting_lying_down": "pedestrian.sitting_lying_down",
        "standing": "pedestrian.standing",
    }

    @classmethod
    def get_category_name_by_obj_type(cls, obj_type):
        obj_type = obj_type.lower()
        # check if obj_type is in the map
        if obj_type not in cls.obj_type_to_category_name_map:
            return None
        else:
            return cls.obj_type_to_category_name_map[obj_type]

    @classmethod
    def get_attribute_name_list_by_category_name(cls, category_name):
        attribute_name_list = []

        # check if category is in the map
        if category_name not in cls.category_name_to_attribute_name_list_map:
            return attribute_name_list
        else:
            return cls.category_name_to_attribute_name_list_map[category_name]

    @classmethod
    def get_extended_attribute(cls, attribute):
        return cls.extend_attribute_dict.get(attribute)


class FusionLidarFilterRangeMap:
    """多lidar点云融合时所需过滤范围映射表"""

    car_brand_filter_range_map = {
        "yc200": {},
        "yc800": {
            "lidar-front": {
                "x": -5,
                "y": 5,
                "z": 0,
                "l": 10,
                "w": 10,
                "h": 10,
                "yaw": 0,
            },
            "lidar-back": {
                "x": 5,
                "y": -5,
                "z": 0,
                "l": 10,
                "w": 10,
                "h": 10,
                "yaw": 0,
            },
        },
        "yc1000": {
            "lidar-front": {
                "x": -5,
                "y": 5,
                "z": 0,
                "l": 10,
                "w": 10,
                "h": 10,
                "yaw": 0,
            },
            "lidar-back": {
                "x": 5,
                "y": -5,
                "z": 0,
                "l": 10,
                "w": 10,
                "h": 10,
                "yaw": 0,
            },
        },
    }

    @classmethod
    def get_filter_range_by_car_brand(cls, car_brand):
        car_brand = car_brand.lower()
        # check if obj_type is in the map
        if car_brand not in cls.car_brand_filter_range_map:
            return None
        else:
            filter_range = cls.car_brand_filter_range_map[car_brand]
            if not filter_range:
                return None
            else:
                return filter_range


# 定义错误码常量
class ErrorCode:
    # SUS数据相关错误
    SUS_PATH_NOT_EXIST = 1001
    SUS_CAMERA_PATH_NOT_EXIST = 1002
    SUS_LIDAR_PATH_NOT_EXIST = 1003
    SUS_FILE_COUNT_MISMATCH = 1004
    SUS_LABEL_PATH_NOT_EXIST = 1005

    # sus 标签数据相关错误
    LABEL_JSON_PARSE_ERROR = 2001
    LABEL_SCALE_TOO_SMALL = 2002
    LABEL_NO_LIDAR_POINTS = 2003
    SUS_LABEL_MISSING_LIDAR_POINTS_FIELD = 2004

    # nuscenes数据相关错误
    NUSCENES_PATH_NOT_EXIST = 3001
    NUSCENES_CAMERA_PATH_NOT_EXIST = 3002
    NUSCENES_LIDAR_PATH_NOT_EXIST = 3003
    NUSCENES_FILE_COUNT_MISMATCH = 3004
    NUSCENES_LABEL_PATH_NOT_EXIST = 3005

    # nuscenes 标签数据相关错误
    NUSCENES_LABEL_JSON_PARSE_ERROR = 4001
    NUSCENES_LABEL_NO_ANNOTATIONS = 4002
    NUSCENES_LABEL_SCALE_TOO_SMALL = 4003
    NUSCENES_LABEL_NO_LIDAR_POINTS = 4004
    NUSCENES_LABEL_MISSING_LIDAR_POINTS_FIELD = 4005
    NUSCENES_SAMPLE_NO_ANNOTATIONS = 4006

    # 其他错误
    GENERAL_ERROR = 9999


# 错误码到错误信息的映射
ERROR_MESSAGES = {
    # sus 相关错误
    ErrorCode.SUS_PATH_NOT_EXIST: "SUS数据路径不存在",
    ErrorCode.SUS_CAMERA_PATH_NOT_EXIST: "SUS相机数据路径不存在",
    ErrorCode.SUS_LIDAR_PATH_NOT_EXIST: "SUS激光雷达数据路径不存在",
    ErrorCode.SUS_FILE_COUNT_MISMATCH: "SUS传感器文件数量不匹配",
    ErrorCode.SUS_LABEL_PATH_NOT_EXIST: "SUS标签路径不存在",
    ErrorCode.LABEL_JSON_PARSE_ERROR: "标签JSON解析失败",
    ErrorCode.LABEL_SCALE_TOO_SMALL: "标签对象尺寸过小(小于0.05)",
    ErrorCode.LABEL_NO_LIDAR_POINTS: "标签对象激光雷达点数为零",
    ErrorCode.SUS_LABEL_MISSING_LIDAR_POINTS_FIELD: "sus 标签对象缺少激光雷达点数字段",
    ErrorCode.GENERAL_ERROR: "一般错误",
    # nuscenes 相关错误
    ErrorCode.NUSCENES_PATH_NOT_EXIST: "nuscenes数据路径不存在",
    ErrorCode.NUSCENES_CAMERA_PATH_NOT_EXIST: "nuscenes相机数据路径不存在",
    ErrorCode.NUSCENES_LIDAR_PATH_NOT_EXIST: "nuscenes激光雷达数据路径不存在",
    ErrorCode.NUSCENES_FILE_COUNT_MISMATCH: "nuscenes传感器文件数量不匹配",
    ErrorCode.NUSCENES_LABEL_PATH_NOT_EXIST: "nuscenes标签路径不存在",
    ErrorCode.NUSCENES_LABEL_JSON_PARSE_ERROR: "nuscenes标签JSON解析失败",
    ErrorCode.NUSCENES_LABEL_NO_ANNOTATIONS: "nuscenes标签对象没有标注结果",
    ErrorCode.NUSCENES_LABEL_SCALE_TOO_SMALL: "nuscenes标签对象尺寸过小(小于0.05)",
    ErrorCode.NUSCENES_LABEL_NO_LIDAR_POINTS: "nuscenes标签对象激光雷达点数为零",
    ErrorCode.NUSCENES_LABEL_MISSING_LIDAR_POINTS_FIELD: "nuscenes 标签对象缺少激光雷达点数字段",
    ErrorCode.NUSCENES_SAMPLE_NO_ANNOTATIONS: "nuscenes样本没有标注结果",
}

FILTER_RULES = {
    "car": {
        "min_size": [1.0, 1.0, 1.0],
        "min_points": 20,
    },
    "truck": {
        "min_size": [1.0, 1.0, 1.0],
        "min_points": 20,
    },
    "pedestrian": {
        "min_size": [0.1, 0.1, 1.0],
        "min_points": 20,
    },
    "rider": {
        "min_size": [0.1, 0.1, 1.0],
        "min_points": 20,
    },
    "traffic_cone": {
        "min_size": [0.1, 0.1, 0.1],
        "min_points": 5,
    },
}
