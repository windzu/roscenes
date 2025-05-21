import numpy as np
import quaternion


class NuscenesObject:
    def __init__(
        self,
        scene_name,
        timestamp,
        object_id,
        track_id,
        category,
        translation,
        size,
        rotation,
        visibility,
        attribute_name_list,
        num_lidar_pts=0,
    ):
        self.scene_name = scene_name
        self.timestamp = timestamp
        self.object_id = object_id
        self.track_id = track_id
        self.category = category

        self.translation = translation  # x, y, z
        self.size = size  # l, w, h
        self.rotation = rotation  # quaternion

        self.visibility = visibility
        self.attribute_name_list = attribute_name_list

        self.num_lidar_pts = num_lidar_pts
        self.num_radar_pts = 0

    def transform_to_global(self, global_rotation, global_translation):
        """
        将ego的坐标系下的3d bbox转换到global坐标系下

        Args:
            global_rotation: car's rotation quaternion in global coordinate
            global_translation: car's translation vector in global coordinate
        """

        ego_rotation = quaternion.from_float_array(self.rotation)

        global_rotation = quaternion.from_float_array(global_rotation)

        # rotation
        rotation_combined = global_rotation * ego_rotation

        ego_translation = np.array(self.translation)
        global_translation = np.array(global_translation)

        # 使用四元数旋转ego的平移
        rotated_translation = quaternion.rotate_vectors(
            global_rotation, ego_translation
        )

        # 组合平移
        translation_combined = rotated_translation + global_translation

        # 保存
        self.translation = translation_combined.tolist()

        self.rotation = [
            rotation_combined.w,
            rotation_combined.x,
            rotation_combined.y,
            rotation_combined.z,
        ]
