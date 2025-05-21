"""
Author: wind windzu1@gmail.com
Date: 2023-08-27 18:34:41
LastEditors: wind windzu1@gmail.com
LastEditTime: 2023-08-28 19:14:33
Description: 
Copyright (c) 2023 by windzu, All Rights Reserved. 
"""

import os
from argparse import Action, ArgumentParser

from .load import Load


class ParseList(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


def get_default_paths(current_path):
    """use current path as default path"""
    input_path_list = []
    output_path_list = []
    # 1. get all scenes folder in current path
    scenes_names = [
        name
        for name in os.listdir(current_path)
        if os.path.isdir(os.path.join(current_path, name))
    ]
    # 2. generate input and output paths
    for name in scenes_names:
        input_path = os.path.join(current_path, name, "sus")
        output_path = os.path.join(current_path, name, "nuscenes")
        # 2.1. check if input and output paths exist
        if not os.path.exists(input_path) or not os.path.exists(output_path):
            print(f"Input or output path does not exist: {input_path} or {output_path}")
            continue

        # 2.2 make sure input have annotation files
        sus_label_path = os.path.join(input_path, "label")
        if not os.path.exists(sus_label_path):
            print(f"Input path does not have annotation files: {input_path}")
            continue
        # 2.3 make sure sus label path is not empty
        sus_label_files = os.listdir(sus_label_path)
        if not sus_label_files:
            print(f"Input path does not have annotation files: {input_path}")
            continue

        # 2.2. add input and output paths to list
        input_path_list.append(input_path)
        output_path_list.append(output_path)

    return input_path_list, output_path_list


def main(args, unknown):

    # parse args
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-i",
        "--input_path_list",
        type=str,
        required=False,
        nargs="+",
        action=ParseList,
    )
    parser.add_argument(
        "-o",
        "--output_path_list",
        type=str,
        required=False,
        nargs="+",
        action=ParseList,
    )
    parser.add_argument(
        "-f",
        "--filter",
        action="store_true",
        help="Enable filtering during loading process",
    )
    args, unknown = parser.parse_known_args(unknown)

    print("----------------------")
    print("----     load     ----")
    print("----------------------")

    # check args
    input_path_list = args.input_path_list
    output_path_list = args.output_path_list
    filter_enabled = args.filter

    if filter_enabled:
        print("Filtering is enabled during loading.")

    if input_path_list and output_path_list:
        if len(input_path_list) != len(output_path_list):
            raise ValueError(
                "The number of input paths and output paths must be the same."
            )
    # if input_path_list and output_path_list are empty, use default paths
    if not args.input_path_list or not args.output_path_list:
        print("Input and output paths are empty, using default paths.")
        root_path = os.getcwd()
        input_path_list, output_path_list = get_default_paths(root_path)

    load = Load(
        input_path_list=input_path_list,
        output_path_list=output_path_list,
        filter_enabled=filter_enabled,
    )
    load.load()
