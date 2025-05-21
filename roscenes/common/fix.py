import json
import os
from typing import Any, Dict, List

import numpy as np
import quaternion
from rich.progress import track

from .check import ErrorCode
from .utils import get_points_num


def fix_scale_too_small(scene_path: str, error_info: Dict[str, Dict[int, int]]) -> Dict[str, Dict[str, Any]]:
    """修复标签中尺寸过小（小于0.05）的对象
    
    Args:
        scene_path (str): 场景路径
        error_info (dict): 错误信息字典 {label_file: {error_code: count}}
    
    Returns:
        dict: 修复结果统计
    """
    fix_results = {}
    
    sus_path = os.path.join(scene_path, "sus")
    label_path = os.path.join(sus_path, "label")
    
    # 处理每个有问题的文件
    for label_file, error_codes in error_info.items():
        # 跳过不包含尺寸过小错误的文件
        if ErrorCode.LABEL_SCALE_TOO_SMALL not in error_codes:
            continue
            
        label_file_path = os.path.join(label_path, label_file)
        if not os.path.exists(label_file_path):
            fix_results[label_file] = {"error": "文件不存在"}
            continue
            
        try:
            # 读取标签文件
            with open(label_file_path, 'r') as f:
                labels = json.load(f)
                
            fixed_count = 0
            
            # 检查每个对象的尺寸
            for obj in labels:
                if "psr" in obj and "scale" in obj["psr"]:
                    scale = obj["psr"]["scale"]
                    modified = False
                    
                    # 检查并修复尺寸
                    if scale["x"] < 0.05:
                        scale["x"] = 0.05
                        modified = True
                    if scale["y"] < 0.05:
                        scale["y"] = 0.05
                        modified = True
                    if scale["z"] < 0.05:
                        scale["z"] = 0.05
                        modified = True
                        
                    if modified:
                        fixed_count += 1
            
            # 保存修改后的文件
            if fixed_count > 0:
                with open(label_file_path, 'w') as f:
                    json.dump(labels, f, indent=2)
                    
                fix_results[label_file] = {
                    "fixed": fixed_count,
                    "total": error_codes[ErrorCode.LABEL_SCALE_TOO_SMALL]
                }
            else:
                fix_results[label_file] = {"fixed": 0, "reason": "未找到需要修复的对象"}
                
        except Exception as e:
            fix_results[label_file] = {"error": f"修复过程出错: {str(e)}"}
    
    return fix_results

def fix_no_lidar_points(scene_path: str, error_info: Dict[str, Dict[int, int]]) -> Dict[str, Dict[str, Any]]:
    """修复标签中激光雷达点数为0的问题（通过删除这些对象）
    
    Args:
        scene_path (str): 场景路径
        error_info (dict): 错误信息字典 {label_file: {error_code: count}}
    
    Returns:
        dict: 修复结果统计
    """
    fix_results = {}

    # debug
    return fix_results
    
    # 检查是否有需要修复的文件
    no_pts_files = {file_name: errors for file_name, errors in error_info.items() 
                   if ErrorCode.LABEL_NO_LIDAR_POINTS in errors}
    
    if not no_pts_files:
        return fix_results
    
    sus_path = os.path.join(scene_path, "sus")
    label_path = os.path.join(sus_path, "label")
    
    # 处理每个有问题的文件
    for label_file, errors in no_pts_files.items():
        expected_fixes = errors.get(ErrorCode.LABEL_NO_LIDAR_POINTS, 0)
        if expected_fixes <= 0:
            continue
            
        label_file_path = os.path.join(label_path, label_file)
        if not os.path.exists(label_file_path):
            fix_results[label_file] = {"error": "文件不存在"}
            continue
            
        try:
            # 读取标签文件
            with open(label_file_path, 'r') as f:
                labels = json.load(f)
                
            original_count = len(labels)
            objects_to_remove = []
            
            # 检查每个对象的点数，标记需要删除的对象
            for idx, obj in enumerate(labels):
                if "num_lidar_pts" in obj and obj["num_lidar_pts"] <= 0:
                    objects_to_remove.append(idx)
            
            # 从后向前删除对象，避免索引变化问题
            objects_to_remove.sort(reverse=True)
            for idx in objects_to_remove:
                del labels[idx]
            
            # 保存修改后的文件
            if objects_to_remove:
                with open(label_file_path, 'w') as f:
                    json.dump(labels, f, indent=2)
                    
                fix_results[label_file] = {
                    "removed": len(objects_to_remove),
                    "before": original_count,
                    "after": len(labels),
                    "expected_fixes": expected_fixes
                }
            else:
                fix_results[label_file] = {"removed": 0, "reason": "未找到需要删除的对象"}
                
        except Exception as e:
            fix_results[label_file] = {"error": f"修复过程出错: {str(e)}"}
    
    return fix_results

def fix_sus_label_missing_num_lidar_pts(scene_path: str, error_info: Dict[str, Dict[int, int]]) -> Dict[str, Dict[str, Any]]:
    """修复缺少num_lidar_pts字段的标签文件
    
    Args:
        scene_path (str): 场景根路径
        error_info (dict): 错误信息字典 {label_file: {error_code: count}}
    
    Returns:
        dict: 修复结果统计 {label_file: {"before": 原对象总数, "after": 修复后对象总数, "fixed": 修复的对象数}}
    """
    fix_results = {}
    
    # 检查是否有需要修复的文件
    missing_pts_files = {file_name: errors for file_name, errors in error_info.items() 
                       if ErrorCode.LABEL_MISSING_LIDAR_POINTS_FIELD in errors}
    
    if not missing_pts_files:
        return fix_results
    
    sus_path = os.path.join(scene_path, "sus")
    lidar_path = os.path.join(sus_path, "lidar")
    label_path = os.path.join(sus_path, "label")
    
    # 获取所有点云文件的路径
    if not os.path.exists(lidar_path):
        print(f"警告: 无法找到激光雷达数据路径 {lidar_path}")
        return fix_results
        
    lidar_files = {f.split('.')[0]: os.path.join(lidar_path, f) 
                  for f in os.listdir(lidar_path) if f.endswith('.pcd') or f.endswith('.bin')}
    
    # 修复每个问题标签文件
    for label_file, errors in missing_pts_files.items():
        expected_fixes = errors.get(ErrorCode.LABEL_MISSING_LIDAR_POINTS_FIELD, 0)
        if expected_fixes <= 0:
            continue
            
        label_file_path = os.path.join(label_path, label_file)
        file_stem = label_file.split('.')[0]  # 去掉文件扩展名
        
        # 查找对应的点云文件
        lidar_file = lidar_files.get(file_stem)
        if not lidar_file:
            print(f"警告: 无法找到与标签文件 {label_file} 对应的点云文件")
            fix_results[label_file] = {"error": "找不到对应的点云文件"}
            continue
        
        try:  
            # 读取标签文件
            with open(label_file_path, 'r') as f:
                labels = json.load(f)
            
            # 记录修复前数据
            original_count = len(labels)
            fixed_count = 0
            
            # 处理每个对象，检查是否缺少num_lidar_pts
            for obj_idx, obj in enumerate(labels):
                if "num_lidar_pts" not in obj:
                    # 确保对象有完整的PSR信息
                    if "psr" in obj and all(key in obj["psr"] for key in ["position", "rotation", "scale"]):
                        # 提取PSR信息
                        position = obj["psr"]["position"]
                        rotation = obj["psr"]["rotation"]
                        scale = obj["psr"]["scale"]
                        
                        # 将数据转换为get_points_num需要的格式
                        position_list = [position["x"], position["y"], position["z"]]
                        scale_list = [scale["x"], scale["y"], scale["z"]]
                        
                        # 将欧拉角转换为四元数
                        rotation_quat = quaternion.from_euler_angles(
                            rotation["x"], rotation["y"], rotation["z"]
                        )
                        rotation_quat_list = [
                            rotation_quat.w, rotation_quat.x, rotation_quat.y, rotation_quat.z
                        ]
                        
                        # 计算点数
                        num_points = get_points_num(
                            lidar_file, scale_list, position_list, rotation_quat_list
                        )
                        
                        # 更新标签对象
                        obj["num_lidar_pts"] = int(num_points)
                        fixed_count += 1
            
            # 如果有修复，保存文件
            if fixed_count > 0:
                with open(label_file_path, 'w') as f:
                    json.dump(labels, f, indent=2)
                
                fix_results[label_file] = {
                    "before": original_count,
                    "fixed": fixed_count,
                    "expected_fixes": expected_fixes
                }
            else:
                fix_results[label_file] = {
                    "status": "无需修复",
                    "reason": "未找到缺少num_lidar_pts的对象"
                }
            
        except Exception as e:
            fix_results[label_file] = {"error": f"修复过程出错: {str(e)}"}
    
    return fix_results

def fix_invalid_scenes(check_path: str, invalid_scenes: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """修复所有不合法的场景
    
    Args:
        check_path (str): 检查路径
        invalid_scenes (dict): 不合法场景信息 {scene_name: scene_check_result}
    
    Returns:
        dict: 修复结果统计 {scene_name: {fix_type: fix_result}}
    """
    from .check import scene_check
    
    fix_summary = {}
    
    # 按场景名称排序
    sorted_scenes = sorted(invalid_scenes.keys())
    
    for scene_name in track(sorted_scenes, description="修复异常场景"):
        scene_path = os.path.join(check_path, scene_name)
        scene_fix_results = {}
        
        try:
            # 获取场景的错误详情
            scene_errors = scene_check(scene_path)
            if not scene_errors:
                print(f"场景 '{scene_name}' 未检测到需要修复的问题")
                continue
                
            # 1. 修复尺寸过小的问题 (通过删除对象)
            scale_fix_needed = any(ErrorCode.LABEL_SCALE_TOO_SMALL in errors 
                                for errors in scene_errors.values())
            if scale_fix_needed:
                scale_fix_results = fix_scale_too_small(scene_path, scene_errors)
                scene_fix_results["scale_too_small"] = scale_fix_results
            
            # 2. 修复点数为0的问题 (通过删除对象)
            no_pts_fix_needed = any(ErrorCode.LABEL_NO_LIDAR_POINTS in errors 
                                for errors in scene_errors.values())
            if no_pts_fix_needed:
                no_pts_results = fix_no_lidar_points(scene_path, scene_errors)
                scene_fix_results["no_lidar_pts"] = no_pts_results
            
            # 3. 修复缺少点数字段的问题 (添加点数字段)
            # 重新检查场景，因为前两步可能已经修改了文件
            scene_errors = scene_check(scene_path)
            missing_pts_fix_needed = any(ErrorCode.LABEL_MISSING_LIDAR_POINTS_FIELD in errors 
                                      for errors in scene_errors.values())
            if missing_pts_fix_needed:
                missing_pts_results = fix_sus_label_missing_num_lidar_pts(scene_path, scene_errors)
                scene_fix_results["missing_lidar_pts"] = missing_pts_results
            
            # 记录场景的修复结果
            fix_summary[scene_name] = scene_fix_results
            
            # 检查是否所有错误都已修复
            remaining_errors = scene_check(scene_path)
            if not remaining_errors:
                print(f"场景 '{scene_name}' 所有错误已修复")
            else:
                print(f"场景 '{scene_name}' 仍有 {len(remaining_errors)} 个文件存在错误")
                
        except Exception as e:
            fix_summary[scene_name] = {"error": f"修复过程出错: {str(e)}"}
            print(f"修复场景 '{scene_name}' 时出错: {str(e)}")
    
    return fix_summary

def echo_fix_results(fix_results: Dict[str, Dict[str, Any]]):
    """输出修复结果的摘要
    
    Args:
        fix_results (dict): 修复结果统计
    """
    if not fix_results:
        print("没有执行任何修复操作。")
        return
    
    print("\n修复结果摘要:")
    total_fixed = 0
    total_failed = 0
    
    for scene_name, scene_results in fix_results.items():
        if "error" in scene_results:
            print(f"\n场景 '{scene_name}': 修复失败 - {scene_results['error']}")
            total_failed += 1
            continue
            
        scene_fixed = False
        print(f"\n场景 '{scene_name}':")
        
        # 检查尺寸修复结果
        if "scale_too_small" in scene_results:
            scale_results = scene_results["scale_too_small"]
            total_fixed_scale = sum(r.get("fixed", 0) for r in scale_results.values() if isinstance(r, dict) and "fixed" in r)
            if total_fixed_scale > 0:
                print(f"  - 已修复 {total_fixed_scale} 个尺寸过小的对象")
                scene_fixed = True
        
        # 检查点数字段修复结果
        if "missing_lidar_pts" in scene_results:
            pts_results = scene_results["missing_lidar_pts"]
            total_fixed_pts = sum(r.get("fixed", 0) for r in pts_results.values() if isinstance(r, dict) and "fixed" in r)
            if total_fixed_pts > 0:
                print(f"  - 已为 {total_fixed_pts} 个对象添加点数字段")
                scene_fixed = True
        
        # 检查点数为0的修复结果
        if "no_lidar_pts" in scene_results:
            print("  - 无法修复点数为0的对象，需要重新采集数据")
            
        # 检查JSON解析错误
        if "json_parse_error" in scene_results:
            print("  - 无法修复JSON解析错误，需要手动检查文件")
        
        if scene_fixed:
            total_fixed += 1
        else:
            total_failed += 1
            print("  - 未执行任何修复操作")
    
    print(f"\n总计: 成功修复 {total_fixed} 个场景，{total_failed} 个场景未能完全修复")