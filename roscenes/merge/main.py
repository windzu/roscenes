from .merge_cml import Merge
from argparse import ArgumentParser, Action


class ParseList(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


def main(args, unknown):

    # parse args
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-i",
        "--source_scene_path_list",
        type=str,
        required=True,
        nargs="+",
        action=ParseList,
    )
    parser.add_argument("-o", "--target_nuscenes_path", type=str, required=True)
    parser.add_argument("-t", "--target_type", type=str, required=True)
    parser.add_argument("-c", "--main_channel", type=str, required=True)
    args, unknown = parser.parse_known_args(unknown)

    # debug
    # echo source_scene_path_list
    print("source_scene_path_list: ", args.source_scene_path_list)

    print("----------------------")
    print("----    merge     ----")
    print("----------------------")
    merge = Merge(
        source_scene_path_list=args.source_scene_path_list,
        target_nuscenes_path=args.target_nuscenes_path,
        target_type=args.target_type,
        main_channel=args.main_channel,
    )
    merge.merge()
