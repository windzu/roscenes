from argparse import Action, ArgumentParser

from .export import Export


class ParseList(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


def main(args, unknown):

    # parse args
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-i",
        "--input_path_list",
        type=str,
        required=True,
        nargs="+",
        action=ParseList,
    )
    parser.add_argument(
        "-o",
        "--output_path_list",
        type=str,
        required=True,
        nargs="+",
        action=ParseList,
    )
    args, unknown = parser.parse_known_args(unknown)

    print("----------------------")
    print("----   export     ----")
    print("----------------------")

    export = Export(
        input_path_list=args.input_path_list,
        output_path_list=args.output_path_list,
    )
    export.export()
