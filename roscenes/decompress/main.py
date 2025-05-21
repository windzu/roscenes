import os
from argparse import ArgumentParser

from .decompress import Decompress


def parse_args(argv):
    parser = ArgumentParser()
    parser.add_argument(
        "--input_path",
        "-i",
        type=str,
        help="input path",
    )
    parser.add_argument(
        "--output_path",
        "-o",
        type=str,
        help="output path",
    )
    parser.add_argument(
        "--suffix",
        "-s",
        type=str,
        default=".tgz",
        help="output path",
    )
    parser.add_argument(
        "--worker_num",
        "-j",
        type=int,
        default=4,
        help="worker num",
    )

    args = parser.parse_args(argv)

    return args


def main(args, unknown):
    args = parse_args(unknown)

    decompress = Decompress(
        input_path=args.input_path,
        output_path=args.output_path,
        suffix=args.suffix,
        worker_num=args.worker_num,
    )
    decompress.decompress()


if __name__ == "__main__":
    main()
