import os
from argparse import Action, ArgumentParser

from ..common.data_config import DataConfig
from .slice import Slice


class ParseList(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


def main(args, unknown):
    # parse unknown args
    # -i/--input_rosbag_file_path_list : input rosbag file path list
    # -o/--output_folder_path_list : output folder path list
    # -s/--scene_name_list : scene name list
    # --sample_interval
    # --time_list
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-i",
        "--input_rosbag_file_path_list",
        type=str,
        required=True,
        nargs="+",
        action=ParseList,
    )
    parser.add_argument(
        "-o",
        "--output_folder_path_list",
        type=str,
        required=True,
        nargs="+",
        action=ParseList,
    )
    parser.add_argument(
        "-s",
        "--scene_name_list",
        type=str,
        required=True,
        nargs="+",
        action=ParseList,
    )
    parser.add_argument("--sample_interval", type=int, default=500)
    parser.add_argument("--time_list", type=str, default="")

    args, unknown = parser.parse_known_args(unknown)

    input_rosbag_file_path_list = args.input_rosbag_file_path_list
    output_folder_path_list = args.output_folder_path_list
    scene_name_list = args.scene_name_list
    sample_interval = args.sample_interval
    time_list = args.time_list

    # 1. parse and check args
    # check input_rosbag_file_path_list and output_path_list length
    if len(input_rosbag_file_path_list) != len(output_folder_path_list):
        raise Exception(
            "input_rosbag_file_path_list length should be equal to output_folder_path_list length."
        )
    # check input_rosbag_file_path_list and scene_name_list length
    if len(input_rosbag_file_path_list) != len(scene_name_list):
        raise Exception(
            "input_rosbag_file_path_list length should be equal to scene_name_list length."
        )
    # check all input_rosbag_file_path valid
    for input_rosbag_file_path in input_rosbag_file_path_list:
        if not os.path.exists(input_rosbag_file_path):
            raise Exception(f"{input_rosbag_file_path} not exists.")

    # convert sample_interval from ms to times of 100ms
    # check sample_interval valid
    if sample_interval < 100:
        raise Exception("sample_interval should be greater than 100ms.")
    sample_interval = int(sample_interval / 100)

    # parse time_list which format is "[[start_time,end_time],[start_time,end_time]]"
    if not time_list:
        time_list = []
    else:
        time_list = time_list.replace("[", "").replace("]", "").split(",")
        time_list = [
            [int(time_list[i]), int(time_list[i + 1])]
            for i in range(0, len(time_list), 2)
        ]

    # build config
    config = DataConfig(sample_interval=sample_interval)

    # build data info list
    # each data info is a dict
    # - scene_name
    # - input_rosbag_file_path
    # - output_folder_path
    # - time_list
    data_info_list = []
    for i in range(len(input_rosbag_file_path_list)):
        data_info = {
            "scene_name": scene_name_list[i],
            "rosbag_file_path": input_rosbag_file_path_list[i],
            "nuscenes_folder_path": output_folder_path_list[i],
            "time_list": time_list,
        }
        data_info_list.append(data_info)

    print("----------------------")
    print("----    slice     ----")
    print("----------------------")
    slice = Slice(config=config, data_info_list=data_info_list)
    slice.slice()


# roscenes
# roscenes record2bag --cml \
# -i /input/path/3769_YC200B01-M1-0007 \
# -o /output/path/3769_YC200B01-M1-0007.bag \
# --container_name yczx_dev

# python -m debugpy --listen 4567 --wait-for-client roscenes/slice/main.py
