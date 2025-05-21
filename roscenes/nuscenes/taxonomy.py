"""
Author: wind windzu1@gmail.com
Date: 2023-10-24 18:58:26
LastEditors: wind windzu1@gmail.com
LastEditTime: 2023-11-03 18:04:52
Description: 
Copyright (c) 2023 by windzu, All Rights Reserved. 
"""
from . import rule
from .utils import generate_uuid_from_input, save_to_json


class CategoryTable:
    def __init__(self):
        self.category_list = [
            Category("human", "pedestrian", "adult", "Adult subcategory."),
            Category("human", "pedestrian", "child", "Child subcategory."),
            Category(
                "human",
                "pedestrian",
                "wheelchair",
                "Wheelchairs. If a person is in the wheelchair, include in the annotation.",
            ),
            Category(
                "human",
                "pedestrian",
                "stroller",
                "Strollers. If a person is in the stroller, include in the annotation.",
            ),
            Category(
                "human",
                "pedestrian",
                "personal_mobility",
                "A small electric or self-propelled vehicle, e.g. skateboard, segway, or scooters, on which the person typically travels in a upright position. Driver and (if applicable) rider should be included in the bounding box along with the vehicle.",
            ),
            Category("human", "pedestrian", "police_officer", "Police officer."),
            Category(
                "human", "pedestrian", "construction_worker", "Construction worker"
            ),
            Category(
                "animal", "", "", "All animals, e.g. cats, rats, dogs, deer, birds."
            ),
            Category(
                "vehicle",
                "car",
                "",
                "Vehicle designed primarily for personal use, e.g. sedans, hatch-backs, wagons, vans, mini-vans, SUVs and jeeps. If the vehicle is designed to carry more than 10 people use vehicle.bus. If it is primarily designed to haul cargo use vehicle.truck. ",
            ),
            Category(
                "vehicle",
                "motorcycle",
                "",
                "Vehicle designed primarily for personal use, e.g. sedans, hatch-backs, wagons, vans, mini-vans, SUVs and jeeps. If the vehicle is designed to carry more than 10 people use vehicle.bus. If it is primarily designed to haul cargo use vehicle.truck. ",
            ),
            Category(
                "vehicle",
                "bicycle",
                "",
                "Vehicle designed primarily for personal use, e.g. sedans, hatch-backs, wagons, vans, mini-vans, SUVs and jeeps. If the vehicle is designed to carry more than 10 people use vehicle.bus. If it is primarily designed to haul cargo use vehicle.truck. ",
            ),
            Category(
                "vehicle",
                "bus",
                "bendy",
                "Bendy bus subcategory. Annotate each section of the bendy bus individually.",
            ),
            Category(
                "vehicle",
                "bus",
                "rigid",
                "Rigid bus subcategory.",
            ),
            Category(
                "vehicle",
                "truck",
                "",
                "Vehicles primarily designed to haul cargo including pick-ups, lorrys, trucks and semi-tractors. Trailers hauled after a semi-tractor should be labeled as vehicle.trailer",
            ),
            Category(
                "vehicle",
                "construction",
                "",
                "Vehicles primarily designed for construction. Typically very slow moving or stationary. Cranes and extremities of construction vehicles are only included in annotations if they interfere with traffic. Trucks used to haul rocks or building materials are considered vehicle.truck rather than construction vehicles.",
            ),
            Category(
                "vehicle",
                "emergency",
                "ambulance",
                "All types of ambulances.",
            ),
            Category(
                "vehicle",
                "emergency",
                "police",
                "All types of police vehicles including police bicycles and motorcycles.",
            ),
            Category(
                "vehicle",
                "trailer",
                "",
                "Any vehicle trailer, both for trucks, cars and bikes.",
            ),
            Category(
                "movable_object",
                "barrier",
                "",
                "Temporary road barrier placed in the scene in order to redirect traffic. Commonly used at construction sites. This includes concrete barrier, metal barrier and water barrier. No fences.",
            ),
            Category(
                "movable_object",
                "trafficcone",
                "",
                "All types of traffic cone.",
            ),
            Category(
                "movable_object",
                "pushable_pullable",
                "",
                "Objects that a pedestrian may push or pull. For example dolleys, wheel barrows, garbage-bins, or shopping carts.",
            ),
            Category(
                "movable_object",
                "debris",
                "",
                "Movable object that is left on the driveable surface that is too large to be driven over safely, e.g tree branch, full trash bag etc.",
            ),
            Category(
                "static_object",
                "bicycle_rack",
                "",
                "Area or device intended to park or secure the bicycles in a row. It includes all the bikes parked in it and any empty slots that are intended for parking bikes.",
            ),
        ]

        self.category_name_list = [category.name for category in self.category_list]

    def sequence_to_json(self, path, filename):
        result = []
        for category in self.category_list:
            result.append(category.sequence_to_json())

        save_to_json(result, path, filename)


class Category:
    def __init__(self, c0, c1="", c2="", description=""):
        components = [c0, c1, c2]
        non_empty_components = [c for c in components if c]
        self.name = ".".join(non_empty_components)
        self.token = generate_uuid_from_input(self.name)
        self.description = description

    def sequence_to_json(self):
        result = {
            "token": self.token,
            "name": self.name,
            "description": self.description,
        }
        return result


class AttributeTable:
    def __init__(self):
        self.attribute_list = [
            Attribute("vehicle", "moving", "Vehicle is moving."),
            Attribute(
                "vehicle",
                "stopped",
                "Vehicle, with a driver/rider in/on it, is currently stationary but has an intent to move.",
            ),
            Attribute(
                "vehicle",
                "parked",
                "Vehicle is stationary (usually for longer duration) with no immediate intent to move.",
            ),
            Attribute(
                "cycle",
                "with_rider",
                "There is a rider on the bicycle or motorcycle.",
            ),
            Attribute(
                "cycle",
                "without_rider",
                "There is no rider on the bicycle or motorcycle.",
            ),
            Attribute(
                "pedestrian",
                "sitting_lying_down",
                "The human is sitting or lying down.",
            ),
            Attribute(
                "pedestrian",
                "standing",
                "The human is standing.",
            ),
            Attribute(
                "pedestrian",
                "moving",
                "The human is moving.",
            ),
        ]

        self.attribute_name_list = [attribute.name for attribute in self.attribute_list]

    def sequence_to_json(self, path, filename):
        result = []
        for attribute in self.attribute_list:
            result.append(attribute.sequence_to_json())

        save_to_json(result, path, filename)


class Attribute:
    def __init__(self, category, attribute, description=""):
        self.name = category + "." + attribute
        self.token = generate_uuid_from_input(self.name)
        self.description = description

    def sequence_to_json(self):
        result = {
            "token": self.token,
            "name": self.name,
            "description": self.description,
        }
        return result


class VisibilityTable:
    def __init__(self):
        self.visibility_list = [
            Visibility(1, "v0-40", "visibility of whole object is between 0 and 40%"),
            Visibility(2, "v40-60", "visibility of whole object is between 40 and 60%"),
            Visibility(3, "v60-80", "visibility of whole object is between 60 and 80%"),
            Visibility(
                4, "v80-100", "visibility of whole object is between 80 and 100%"
            ),
        ]

    def sequence_to_json(self, path, filename):
        result = []
        for visibility in self.visibility_list:
            result.append(visibility.sequence_to_json())

        save_to_json(result, path, filename)


class Visibility:
    def __init__(self, token, level, description=None):
        self.token = token
        self.level = level
        self.description = description

    def sequence_to_json(self):
        result = {
            "token": self.token,
            "level": self.level,
            "description": self.description,
        }
        return result
