from ..nuscenes.taxonomy import AttributeTable, CategoryTable, VisibilityTable


def get_nuscenes_category_name_list():
    category_table = CategoryTable()
    category_name_list = []
    for category in category_table.category_list:
        category_name_list.append(category.name)
    return category_name_list


def get_nuscenes_attribute_name_list():
    attribute_table = AttributeTable()
    attribute_name_list = []
    for attribute in attribute_table.attribute_list:
        attribute_name_list.append(attribute.name)
    return attribute_name_list


def get_nuscenes_visibility_list():
    visibility_table = VisibilityTable()
    visibility_list = []
    for visibility in visibility_table.visibility_list:
        visibility_list.append(visibility.level)
    return visibility_list
