# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Defines a DataSet object
'''

import Utils.Settings as st


class DataSet:

    def __init__(self, datasetID, name: str, owner, hash_of_dataset,
                 cleaned, access_user_list: str, access_business_unit_list: str, description: str,
                 storage_type, data, size=0):
        self.__datasetID = datasetID
        self.__name = name
        self.__owner = owner
        self.__size = size
        self.__hash_of_dataset = hash_of_dataset
        self.__cleaned = self.enum_cleaned(cleaned)
        self.__access_user_list = access_user_list
        self.__access_business_unit_list = access_business_unit_list
        self.__description = description
        self.__data = data
        self.__storage_type = storage_type

    # Enums
    def enum_cleaned(self, cleaned):
        enum_cleaned = {
            'cleaned': 1,
            'not_cleaned': 0,
            True: 1,
            False: 0,
            'true': 1,
            'false': 0,
            'True': 1,
            'False': 0,
            1: 1,
            0: 0,
            '1': 1,
            '0': 0
        }
        try:
            res = enum_cleaned[cleaned]
        except:
            res = 0
            print('Not Able to identify if DataSet cleaned\n' +
                  'Line: ' + str(st.get_linenumber_of_occured_error())
                  + 'File: ' + str(st.get_filename_of_occured_error()))
        return res

    # Definition of get Methods for DataSet Objects
    def get_datasetID(self):
        return self.__datasetID

    def get_name(self):
        return self.__name

    def get_owner(self):
        return self.__owner

    def get_size(self):
        return self.__size

    def get_hash_of_dataset(self):
        return self.__hash_of_dataset

    def get_access_user_list(self):
        return self.__access_user_list

    def get_access_business_unit_list(self):
        return self.__access_business_unit_list

    def get_description(self):
        return self.__description

    def get_storage_type(self):
        return self.__storage_type

    def get_cleaned(self):
        return self.__cleaned

    def get_data(self):
        return self.__data

    # Setter Methods for certain Attributes of DataSet Objects
    def set_name(self, name: str):
        self.__name = name

    def set_owner(self, owner):
        self.__owner = owner

    def set_cleaned(self, cleaned: bool):
        self.__cleaned = cleaned

    def set_access_user_list(self, access_user_list: list):
        self.__access_user_list = access_user_list

    def set_access_business_unit_list(self, access_business_unit_list: list):
        self.__access_business_unit_list = access_business_unit_list

    def set_description(self, description: str):
        self.__description = description

    def set_storage_type(self, storage_type):
        self.__storage_type = storage_type

    def set_data(self, data):
        self.__data = data

    # Add business unit to access list
    def add_access_business_unit_list(self, business_unit: str):
        self.__access_business_unit_list.append(business_unit)

    # Add user to access list
    def add_access_user_list(self, user_id: str):
        self.__access_user_list = self.__access_user_list + ',' + user_id
