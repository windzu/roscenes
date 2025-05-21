import json
import os


def scene_check(scene_path):
    # 1. check scene_path should be valid
    if not os.path.exists(scene_path):
        raise FileNotFoundError(f"{scene_path} not found")

    # 2. check all json file valid in scene_path/nuscenes/v1.0-all folder
    json_file_list = os.listdir(os.path.join(scene_path, "v1.0-all"))
    if len(json_file_list) == 0:
        raise FileNotFoundError(f"{scene_path} has no json file")
    for json_file in json_file_list:
        if not json_file.endswith(".json"):
            raise FileNotFoundError(f"{scene_path} has invalid json file")
        # check json file valid
        with open(os.path.join(scene_path, "v1.0-all", json_file), "r") as f:
            try:
                json.load(f)
            except json.JSONDecodeError:
                raise ValueError(f"{scene_path} has invalid json file")

    # 3. check imgs and pcds data completeness
    # - have same number of imgs and pcds in samples and sweeps

    # check samples
    # get all folders in scene_path/samples
    samples_folder = os.path.join(scene_path, "samples")
    samples_folder_list = os.listdir(samples_folder)
    if len(samples_folder_list) == 0:
        raise FileNotFoundError(f"{scene_path} has no samples folder")
    # check number of imgs and pcds in each sample
    samples_file_num_dict = {}
    for sample_folder in samples_folder_list:
        # get all files in sample_folder
        sample_folder_path = os.path.join(samples_folder, sample_folder)
        # count number of files in sample_folder
        samples_file_num_dict[sample_folder] = len(os.listdir(sample_folder_path))
    # check all samples have same number of files
    samples_file_num_list = list(samples_file_num_dict.values())
    if len(set(samples_file_num_list)) != 1:
        # debug : tmp return False
        return False
        raise ValueError(f"{scene_path} samples have different number of files")

    # check sweeps
    # get all folders in scene_path/sweeps
    # Note: sweeps folder may not exist or be empty,don't need to check that
    sweeps_folder = os.path.join(scene_path, "sweeps")
    sweeps_folder_list = os.listdir(sweeps_folder)
    if len(sweeps_folder_list) == 0:
        pass
    else:
        # check number of imgs and pcds in each sweep
        sweeps_file_num_dict = {}
        for sweep_folder in sweeps_folder_list:
            # get all files in sweep_folder
            sweep_folder_path = os.path.join(sweeps_folder, sweep_folder)
            # count number of files in sweep_folder
            sweeps_file_num_dict[sweep_folder] = len(os.listdir(sweep_folder_path))
        # check all sweeps have same number of files
        sweeps_file_num_list = list(sweeps_file_num_dict.values())
        if len(set(sweeps_file_num_list)) != 1:
            raise ValueError(f"{scene_path} sweeps have different number of files")

    return True
