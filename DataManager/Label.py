# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Defines a label object
'''

import Utils.Settings as st


class Label:

    def __init__(self, label_id, name, header_list, operation_list):
        self.__label_id = label_id
        self.__name = name
        self.__header_list = header_list
        self.__operation_list = operation_list

    # Definition of get Methods for label Objects
    def get_labelID(self):
        return self.__label_id

    def get_name(self):
        return self.__name

    def get_header_list(self):
        return self.__header_list

    def get_operation_list(self):
        return self.__operation_list

    # Setter Methods for certain Attributes of label Objects

    def set_name(self, name: str):
        self.__name = name

    def set_header_list(self, header_list):
        self.__header_list = header_list

    def set_operation_list(self, operation_list):
        self.__operation_list = operation_list
