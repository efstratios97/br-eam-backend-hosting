# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Defines a DataCleaner Object
'''

import Utils.Settings as st


class Cleanser:

    def __init__(self, cleanser_id, name, description, header_list, datasets, cleanser_operation_types):
        self.__cleanser_id = cleanser_id
        self.__name = name
        self.__description = description
        self.__header_list = header_list
        self.__datasets = datasets
        self.__cleanser_operation_types = cleanser_operation_types

    # Definition of get Methods for Cleaner Objects
    def get_cleanserID(self):
        return self.__cleanser_id

    def get_name(self):
        return self.__name

    def get_description(self):
        return self.__description

    def get_header_list(self):
        return self.__header_list

    def get_datasets(self):
        return self.__datasets

    def get_cleanser_operation_types(self):
        return self.__cleanser_operation_types

    # Setter Methods for certain Attributes of Cleaner Objects
    def set_name(self, name: str):
        self.__name = name

    def set_description(self, description: str):
        self.__description = description

    def set_header_list(self, header_list: str):
        self.__header_list = header_list

    def set_datasets(self, datasets: str):
        self.__datasets = datasets

    def set_datasets(self, cleanser_operation_types):
        self.__cleanser_operation_types = cleanser_operation_types
