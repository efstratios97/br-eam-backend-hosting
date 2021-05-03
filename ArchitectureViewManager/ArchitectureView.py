# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Defines a ArchitectureView Object
'''

import Utils.Settings as st


class ArchitectureView:

    def __init__(self, architecture_view_id, name, description, components):
        self.__architecture_view_id = architecture_view_id
        self.__name = name
        self.__description = description
        self.__components = components

    # Definition of get Methods for Cleaner Objects
    def get_architecture_viewID(self):
        return self.__architecture_view_id

    def get_name(self):
        return self.__name

    def get_description(self):
        return self.__description

    def get_components(self):
        return self.__components

    # Setter Methods for certain Attributes of Cleaner Objects

    def set_name(self, name: str):
        self.__name = name

    def set_description(self, description: str):
        self.__description = description

    def set_components(self, components: str):
        self.__components = components
