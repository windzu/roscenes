import json
import os


from rich.progress import track


from .common.check import scene_check
from .common.constant import ERROR_MESSAGES


def echo_error_result(invalid_scenes, verbose=False):
    """格式化输出检查结果中的错误信息

    Args:
        invalid_scenes (dict): 不合法场景信息字典 {scene_name: scene_check_result}
        verbose (bool): 是否输出详细信息
    """
    if not invalid_scenes:
        print("\n所有场景数据均合法。")
        return

    print(f"\n发现 {len(invalid_scenes)} 个不合法场景")

    if verbose:
        print("\n详细错误信息:")

        # 按场景名称排序
        sorted_scenes = sorted(invalid_scenes.keys())

        for scene_name in sorted_scenes:
            scene_result = invalid_scenes[scene_name]
            # echo scene_name when scene_result is not None
            # 如果场景结果不为空，输出场景名称
            if scene_result:
                print(f"\n场景 '{scene_name}':")

            # 如果是字符串列表（兼容旧格式）
            if isinstance(scene_result, list) and all(
                isinstance(item, str) for item in scene_result
            ):
                for reason in scene_result:
                    print(f"  - {reason}")
            # 如果是错误码字典
            elif isinstance(scene_result, dict):
                for label_file, error_codes in scene_result.items():
                    for error_code, count in error_codes.items():
                        error_msg = ERROR_MESSAGES.get(
                            error_code, f"未知错误(代码:{error_code})"
                        )
                        print(
                            f"  - 标签文件 '{label_file}': {error_msg} ({count}个对象)"
                        )
            else:
                print(f"  - 未能识别的错误格式: {type(scene_result)}")


def check(args, unknown):
    print("----------------------")
    print("----    check    ----")
    print("----------------------")
    check_path = args.path
    if check_path is None:
        # if check_path is None, set check_path to current directory
        check_path = os.getcwd()
    print(f"check path: {check_path}")

    # 获取指定路径下的所有子目录(场景)
    scene_dirs = [
        f for f in os.listdir(check_path) if os.path.isdir(os.path.join(check_path, f))
    ]
    print(f"找到 {len(scene_dirs)} 个场景目录")

    invalid_scenes = {}  # 存储不合法的场景及原因

    for scene_dir in track(scene_dirs, description="检查场景数据"):
        scene_path = os.path.join(check_path, scene_dir)

        # 检查是否同时包含 nuscenes 和 sus 文件夹
        nuscenes_path = os.path.join(scene_path, "nuscenes")
        sus_path = os.path.join(scene_path, "sus")

        if not os.path.exists(nuscenes_path):
            raise ValueError(f"缺少 nuscenes 文件夹: {nuscenes_path}")

        if not os.path.exists(sus_path):
            raise ValueError(f"缺少 sus 文件夹: {sus_path}")

        # 如果基本目录结构正确，继续使用scene_check检查
        scene_check_result = scene_check(scene_path)
        if scene_check_result:
            invalid_scenes[scene_dir] = scene_check_result

    # 输出检查结果
    print("\n检查完成!")
    verbose = args.verbose
    echo_error_result(invalid_scenes, verbose)

    # 如果处于修复模式，可以在这里添加修复逻辑
    if args.fix and invalid_scenes:
        print("\n正在尝试修复问题...")
        from .common.fix import echo_fix_results, fix_invalid_scenes

        fix_results = fix_invalid_scenes(check_path, invalid_scenes)
        echo_fix_results(fix_results)
        print("修复完成!")
