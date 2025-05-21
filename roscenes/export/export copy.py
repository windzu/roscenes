"""
Author: wind windzu1@gmail.com
Date: 2023-08-27 18:34:41
LastEditors: wind windzu1@gmail.com
LastEditTime: 2023-08-29 12:01:45
Description:
Copyright (c) 2023 by windzu, All Rights Reserved.
"""

import json
import os

from rich.progress import track

from .sus import ExportToSUS


class Export:
    """导出数据集

    Args:
        config (DataConfig): 数据集配置
        ws_raw_path (str): 数据集根目录

    """

    def __init__(self, input_path_list: list, output_path_list: list):

        self._export_init()

    def _export_init(self):
        """初始化导出文件夹"""
        if not self.config.cml_mode:
            output_path = None
            # 1. 确认导出文件夹是否存在
            if self.config.export_format == "sus":
                output_path = os.path.join(self.ws_raw_path, "sus")
            else:
                raise NotImplementedError  # 其他格式暂未实现
            if not os.path.exists(output_path):
                os.makedirs(output_path)

            # 3. 完善现有的INFO.json文件
            self.update_info()

    def export(self):
        self.export_scene()

        # 非cml模式下，需要创建软链接
        if not self.config.cml_mode:
            self.update_soft_link()

    def export_scene(self):
        # 1. 获取所有需要导出的 scene 列表
        print("1. Get all need export scene name list")
        scene_name_list = []
        if self.config.cml_mode:
            scene_name_list.append(os.path.basename(self.config.input_path))
        else:
            input_path = os.path.join(self.ws_raw_path, "frames")
            scene_name_list = self.get_scene_name_list(input_path)

        # 2. generate all need export scene path
        print("2. Generate all need export scene path")
        source_path_list = []
        target_path_list = []
        input_path = None
        output_path = None
        if self.config.cml_mode:
            # input_path = os.path.dirname(self.config.input_path)
            # output_path = os.path.dirname(self.config.output_path)
            source_path_list.append(self.config.input_path)
            target_path_list.append(self.config.output_path)
        else:
            input_path = os.path.join(self.ws_raw_path, "frames")
            output_path = os.path.join(self.ws_raw_path, self.config.export_format)
            for scene_name in scene_name_list:
                source_path_list.append(os.path.join(input_path, scene_name))
                target_path_list.append(os.path.join(output_path, scene_name))

        # 3. export to specific format
        print("3. Export to specific format")
        if self.config.export_format == "sus":
            print("export to sus format")
            for source_path, target_path in track(
                zip(source_path_list, target_path_list)
            ):
                export_to_sus = ExportToSUS(
                    config=self.config,
                    source_path=source_path,
                    target_path=target_path,
                )
                export_to_sus.export()
        else:
            raise NotImplementedError

        # 4. 导出完成后，更新INFO.json文件(for workspace mode)
        if not self.config.cml_mode:
            self.update_info()

    def update_soft_link(self):
        """为所有符合条件的scene创建软链接,同时删除不需要的软链接

        Note : 此功能只在 workspace mode 下使用

        需要创建软链接的条件如下：
        1. 已导出scene的INFO.json文件中的anno_completed字段为False,则需要创建软链接

        删除不需要的软链接条件如下：
        1. 已导出scene的INFO.json文件中的anno_completed字段为True,则需要删除对应的软链接

        """
        # parse INFO.json to get need link scene name
        need_link_scene_name_list, no_need_link_scene_name_list = (
            self.get_need_and_no_need_link_scene_name_list()
        )

        # rename path for better understanding
        link_source_path = os.path.join(self.ws_raw_path, self.config.export_format)
        link_target_path = self.config.export_soft_link_target_path
        if not link_target_path:
            raise ValueError("link_target_path is not set in config file")
        link_target_path = os.path.expanduser(link_target_path)

        # check link_target_path exists
        if not os.path.exists(link_target_path):
            print(f"link_target_path not exists : {link_target_path}")
            print(
                f"please check if you have set the correct link_target_path"
                f" in your config file"
            )
            print(f"will not create soft link for all scene in {link_source_path}")
            return

        # create soft link to specific folder for all target_path
        # 1. check link_output_path exists
        # 2. create soft link
        # 3. delete no need link scene
        for scene_name in need_link_scene_name_list:
            scene_source_path = os.path.join(link_source_path, scene_name)
            scene_target_path = os.path.join(link_target_path, scene_name)

            if os.path.islink(scene_target_path):
                # 如果已经存在一个同名的软链接，删除
                os.unlink(scene_target_path)
            if not os.path.exists(scene_target_path):
                os.symlink(scene_source_path, scene_target_path)

        for no_need_link_scene_name in no_need_link_scene_name_list:
            scene_target_path = os.path.join(link_target_path, no_need_link_scene_name)
            if os.path.islink(scene_target_path):
                # echo
                print(f"delete no need link scene : {scene_target_path}")
                # 如果已经存在一个同名的软链接，删除
                os.unlink(scene_target_path)

    def get_need_and_no_need_link_scene_name_list(self):
        """获取所有需要创建和不需要创建软链接的 scene 列表 , 通过如下方式判断是否需要创建软链接

        Note : 此功能只在 workspace mode 下使用

        已导出的scene但未标注完成,该scene中的INFO.json文件中的anno_completed字段为False,通过判断该字段决定是否需要创建软链接

        INFO.json 结构如下：
        [
            {
                "scene_name": "scene_name",
                "anno_completed": True
            }
        ]

        """

        # 1. get all scene name in output path
        output_path = os.path.join(self.ws_raw_path, self.config.export_format)
        scene_name_list = self.get_scene_name_list(output_path)

        # 2. iter all scene in output path and check if need link by parse INFO.json
        need_link_scene_name_list = []
        no_need_link_scene_name_list = []
        for scene_name in scene_name_list:
            info_path = os.path.join(output_path, scene_name, "INFO.json")
            if not os.path.exists(info_path):
                raise FileNotFoundError
            with open(info_path, "r") as f:
                info = json.load(f)
                for scene in info:
                    if not scene["anno_completed"]:
                        need_link_scene_name_list.append(scene_name)
                    else:
                        no_need_link_scene_name_list.append(scene_name)

        # check repeat scene name in need_link_scene_name_list
        need_link_scene_name_list = list(set(need_link_scene_name_list))
        no_need_link_scene_name_list = list(set(no_need_link_scene_name_list))

        # check len of need_link_scene_name_list and no_need_link_scene_name_list
        assert len(need_link_scene_name_list) + len(
            no_need_link_scene_name_list
        ) == len(scene_name_list)

        return need_link_scene_name_list, no_need_link_scene_name_list

    def update_info(self):
        """为那些已经导出的scene但是不包含INFO.json文件的scene创建INFO.json文件


        INFO.json 结构如下：
        [
            {
                "scene_name": "scene_name",
                "anno_completed": True
            }
        ]
        """
        export_path = os.path.join(self.ws_raw_path, self.config.export_format)
        # 1. first check
        if not os.path.exists(export_path):
            raise FileNotFoundError

        # 2. get all export scene name list which is folder
        all_export_scene_name_list = os.listdir(export_path)
        all_export_scene_name_list = [
            scene_name
            for scene_name in all_export_scene_name_list
            if os.path.isdir(os.path.join(export_path, scene_name))
        ]

        # 3. iter all scene in export path and check if need create INFO.json
        for scene_name in all_export_scene_name_list:
            # check if export scene fodler exists
            if os.path.exists(os.path.join(export_path, scene_name)):
                info_path = os.path.join(export_path, scene_name, "INFO.json")
                if not os.path.exists(info_path):
                    with open(info_path, "w") as f:
                        json.dump(
                            [{"scene_name": scene_name, "anno_completed": False}],
                            f,
                            indent=4,
                        )

        return True

    @staticmethod
    def get_scene_name_list(root_path):
        """获取所有需要导出的 scene 列表

        Args:
            root_path (str): frames根目录其中包含多个scene
            该目录下结构如下：
            root_path
            ├── scene_name_xx
            ├── scene_name_xx
            └── ...

        Returns:
                scene_list (list): 需要导出的 scene 列表
        """
        # check root_path
        assert os.path.exists(root_path)

        # get all scene name
        scene_name_list = os.listdir(root_path)
        # remove not folder file
        scene_name_list = [
            scene_name
            for scene_name in scene_name_list
            if os.path.isdir(os.path.join(root_path, scene_name))
        ]

        return scene_name_list

    @staticmethod
    def get_all_symlink_list(root_path):
        # check root_path
        assert os.path.exists(root_path)

        # get all symlink name
        symlink_name_list = os.listdir(root_path)
        # remove not symlink file
        symlink_name_list = [
            symlink_name
            for symlink_name in symlink_name_list
            if os.path.islink(os.path.join(root_path, symlink_name))
        ]

        all_symlink_list = []
        for symlink_name in symlink_name_list:
            all_symlink_list.append(os.path.join(root_path, symlink_name))

        return all_symlink_list
