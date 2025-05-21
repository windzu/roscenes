import os


def nuscenes_check(path):
    # 1. check if nuscenes path is valid
    if not os.path.exists(path):
        return False

    # 2. check if have some folders
    # - maps
    # - samples
    # - sweeps
    # - v1.0-trainval
    # - v1.0-test
    if not os.path.exists(os.path.join(path, "maps")):
        return False
    if not os.path.exists(os.path.join(path, "samples")):
        return False
    if not os.path.exists(os.path.join(path, "sweeps")):
        return False
    if not os.path.exists(os.path.join(path, "v1.0-trainval")):
        return False
    if not os.path.exists(os.path.join(path, "v1.0-test")):
        return False

    return True
