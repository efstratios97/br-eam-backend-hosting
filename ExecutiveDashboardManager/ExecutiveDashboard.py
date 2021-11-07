# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Defines a ExecutiveDashboard Object
'''

import Utils.Settings as st


class ExecutiveDashboard:

    def __init__(self, executive_dashboard_id, name, description, access_user_list, access_business_unit_list, components, dataset):
        self.__executive_dashboard_id = executive_dashboard_id
        self.__name = name
        self.__description = description
        self.__access_user_list = access_user_list
        self.__access_business_unit_list = access_business_unit_list
        self.__components = components
        self.__dataset = dataset

    # Definition of get Methods for Cleaner Objects
    def get_executive_dashboardID(self):
        return self.__executive_dashboard_id

    def get_name(self):
        return self.__name

    def get_description(self):
        return self.__description

    def get_access_user_list(self):
        return self.__access_user_list

    def get_access_business_unit_list(self):
        return self.__access_business_unit_list

    def get_components(self):
        return self.__components

    def get_dataset(self):
        return self.__dataset
