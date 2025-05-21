from argparse import ArgumentParser

from .check import check
from .info import echo_nuscenes_info
from .record2bag import record2bag


def main():
    parser = ArgumentParser(description="roscenes")
    subparsers = parser.add_subparsers(title="commands")

    # record2bag
    init_parser = subparsers.add_parser(
        "record2bag", help="convert cyber record to rosbag"
    )
    init_parser.set_defaults(func=record2bag)

    # check
    init_parser = subparsers.add_parser("check", help="check data")
    init_parser.add_argument("--path", type=str, help="data path")
    init_parser.add_argument(
        "--fix", action="store_true", default=False, help="fix data"
    )
    init_parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="verbose output",
    )
    init_parser.set_defaults(func=check)

    # echo nuscenes info
    info_parser = subparsers.add_parser("info", help="echo nuscenes info")
    info_parser.set_defaults(func=echo_nuscenes_info)

    # slice
    from .slice import main as slice_main

    slice_parser = subparsers.add_parser("slice", help="capture commands")
    slice_parser.set_defaults(func=slice_main)

    # export
    from .export import main as export_main

    export_parser = subparsers.add_parser("export", help="export data to other format")
    export_parser.set_defaults(func=export_main)

    # load
    from .load import main as load_main

    load_parser = subparsers.add_parser("load", help="load data from other format")
    load_parser.set_defaults(func=load_main)

    # merge
    from .merge import main as merge_main

    merge_parser = subparsers.add_parser("merge", help="merge to nuscenes format")
    merge_parser.set_defaults(func=merge_main)


    args, unknown = parser.parse_known_args()

    if hasattr(args, "func"):
        args.func(args, unknown)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
