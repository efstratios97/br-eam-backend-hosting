# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Various Helpermethods and Attributes for re-use
'''

from inspect import currentframe, getframeinfo
import uuid
import hashlib
import pandas as pd
import functools

# ATHENA_CLOUD_DB_HOST = 'localhost'
# ATHENA_CLOUD_DB_USER = 'br'
# ATHENA_CLOUD_DB_PW = 'BRApp12345!'
# ATHENA_CLOUD_DB_DBNAME = 'BRArchitectureManager'
ATHENA_CLOUD_DB_HOST = 'mysql.efspah.dreamhosters.com'
ATHENA_CLOUD_DB_USER = 'efspahdreamhoste'
ATHENA_CLOUD_DB_PW = 'hiathena'
ATHENA_CLOUD_DB_DBNAME = 'projectathena'

# Table names
TABLE_DATASET = 'datasets'
TABLE_USER = 'users'
TABLE_USER_DATASET_ACCESS_RELATION = "user_dataset_access_relation"
TABLE_DEPARTMENT_DATASET_ACCESS_RELATION = "department_dataset_access_relation"
TABLE_CLEANSER_DATASET_COMPATIBILITY = "cleanser_dataset_compatibility"
TABLE_CLEANSER = 'cleansers'
TABLE_DEPARTMENTS = "departments"
TABLE_ARCHITECTURE_VIEWS = "architecture_views"
TABLE_EXECUTIVE_DASHBOARDS = "executive_dashboards"
TABLE_PLOTS = "executive_dashboards_plots"
TABLE_EXECUTIVE_DASHBOARDS_PLOTS_RELATION = "executive_dashboard_plot_relation"


# Dataset Table columns
TB_DATASET_COL_CLEANED = 'CLEANED'
TB_DATASET_COL_DATASET_ID = 'DATASET_ID'
TB_DATASET_COL_NAME = 'NAME'
TB_DATASET_COL_ACCESS_USER_LIST = 'ACCESS_USER_LIST'
TB_DATASET_COL_ACCESS_BUSINESS_UNIT_LIST = 'ACCESS_BUSINESS_UNIT_LIST'
TB_DATASET_COL_DESCRIPTION = 'DESCRIPTION'
TB_DATASET_COL_OWNER = 'OWNER'
TB_DATASET_COL_HASH_OF_DATASET = 'HASH_OF_DATASET'
TB_DATASET_COL_SIZE = 'SIZE'
TB_DATASET_COL_STORAGE_TYPE = 'STORAGE_TYPE'
TB_DATASET_COL_CREATED_AT = 'CREATED_AT'

# User Table Columns
TB_USER_COL_USER_ID = 'USER_ID'
TB_USER_COL_FIRST_NAME = 'FIRST_NAME'
TB_USER_COL_LAST_NAME = 'LAST_NAME'
TB_USER_COL_EMAIL = 'EMAIL'
TB_USER_COL_PASSWORD = 'PASSWORD'
TB_USER_COL_BUSINESS_UNIT = 'BUSINESS_UNIT'
TB_USER_COL_ACCESS_RIGHTS = 'ACCESS_RIGHTS'
TB_USER_COL_ADMIN = 'ADMIN'
TB_USER_COL_ROLE_MANAGER = 'ROLE_MANAGER'
TB_USER_COL_USER_DIRECTORY_PATH = 'USER_DIRECTORY_PATH'
TB_USER_COL_CREATED_AT = 'CREATED_AT'

# Cleanser Table Columns
TB_CLEANSER_COL_CLEANSER_ID = 'CLEANSER_ID'
TB_CLEANSER_COL_NAME = 'NAME'
TB_CLEANSER_COL_DESCRIPTION = 'DESCRIPTION'
TB_CLEANSER_COL_HEADER_LIST = 'HEADER_LIST'
TB_CLEANSER_COL_DATASETIDS = 'DATASETS'
TB_CLEANSER_OPERATION_TYPES = 'CLEANSER_OPERATION_TYPES'
TB_CLEANSER_COL_CREATED_AT = 'CREATED_AT'

# Departments Table Columns
TB_DEPARTMENTS_COL_DEPARTMENT_ID = "DEPARTMENT_ID"
TB_DEPARTMENTS_COL_NAME = "NAME"
TB_DEPARTMENTS_COL_CREATED_AT = "CREATED_AT"

# Architecture Views Table Columns
TB_ARCHITECTURE_VIEWS_COL_ARCHITECTURE_VIEW_ID = "ARCHITECTURE_VIEW_ID"
TB_ARCHITECTURE_VIEWS_COL_ARCHITECTURE_VIEW_NAME = "ARCHITECTURE_VIEW_NAME"
TB_ARCHITECTURE_VIEWS_COL_ARCHITECTURE_VIEW_DESCRIPTION = "ARCHITECTURE_VIEW_DESCRIPTION"
TB_ARCHITECTURE_VIEWS_COL_ARCHITECTURE_VIEW_COMPONENTS = "ARCHITECTURE_VIEW_ID_COMPONENTS"
TB_ARCHITECTURE_VIEWS_COL_CREATED_AT = "CREATED_AT"

# Executive Dashboards Table Columns
TB_EXECUTIVE_DASHBOARDS_COL_ID = "EXECUTIVE_DASHBOARD_ID"
TB_EXECUTIVE_DASHBOARDS_COL_NAME = "EXECUTIVE_DASHBOARD_NAME"
TB_EXECUTIVE_DASHBOARDS_COL_DESCRIPTION = "EXECUTIVE_DASHBOARD_DESCRIPTION"
TB_EXECUTIVE_DASHBOARDS_COL_ACCESS_USER_LIST = 'ACCESS_USER_LIST'
TB_EXECUTIVE_DASHBOARDS_COL_ACCESS_BUSINESS_UNIT_LIST = 'ACCESS_BUSINESS_UNIT_LIST'
TB_EXECUTIVE_DASHBOARDS_COL_COMPONENTS = "EXECUTIVE_DASHBOARD_COMPONENTS"
TB_EXECUTIVE_DASHBOARDS_COL_DATASET = "EXECUTIVE_DASHBOARD_DATASET"
TB_EXECUTIVE_DASHBOARDS_COL_CREATED_AT = "CREATED_AT"

# Executive Dashboards' Plots Table Columns
TB_PLOTS_COL_ID = "PLOT_ID"
TB_PLOTS_COL_NAME = "PLOT_NAME"
TB_PLOTS_COL_DESCRIPTION = "PLOT_DESCRIPTION"
TB_PLOTS_COL_TYPE = "PLOT_TYPE"
TB_PLOTS_COL_PARAMETERS = "EXECUTIVE_DASHBOARD_PARAMETERS"
TB_PLOTS_COL_CREATED_AT = "CREATED_AT"

# Other Global variables
DEPARTMENT_GENESIS = "DEPARTMENT_GENESIS_6aba48df0cb55992803d864977c3aa204520d659"


def get_filename_of_occured_error():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return filename


def get_linenumber_of_occured_error():
    cf = currentframe()
    return cf.f_back.f_lineno

# Creates a unique and privacy safe id


def create_id():
    u_id = uuid.uuid4()
    return u_id.hex

# Create a hash out of DataFrame


def hash_data(data: pd.DataFrame):
    hash_of_dataset = hashlib.sha256(
        pd.util.hash_pandas_object(data).values).hexdigest()
    return hash_of_dataset

# Create Password with sha512 encoding


def create_hash_password_sha512(password, complementary_input):
    hash_pw = hashlib.sha512(
        str(password+complementary_input).encode('utf-8')).hexdigest()
    return hash_pw


def check_list_identical(list_1, list_2):
    list_1.sort()
    list_2.sort()
    if list_1 == list_2:
        return True
    else:
        return False
    # if functools.reduce(lambda i, j: i and j, map(lambda m, k: m == k, list_1, list_2), True):
    #     return False
    # else:
    #     return True


def make_list_to_str(list_1):
    list_str = ''
    for el in list_1:
        list_str += el + ','
    list_1 = list_str[:-1]
    return list_1


def enum_storage_type(storage_type):
    enum_storage_type = {
        'local': 'local',
        'cloud': 'cloud',
        True: 'local',
        False: 'cloud',
        'true': 'local',
        'false': 'cloud',
        'lokal': 'local',
        str(1): 'local',
        str(0): 'cloud',
        1: 'local',
        0: 'cloud'
    }
    try:
        if isinstance(storage_type, str):
            res = enum_storage_type[storage_type.lower()]
        else:
            res = enum_storage_type[storage_type]
    except:
        res = 0
        print('Not Able to identify storage_type of DataSet\n' +
              'Line: ' + str(get_linenumber_of_occured_error())
              + 'File: ' + str(get_filename_of_occured_error()))
    return res


def enum_storage_type_bool(storage_type):
    enum_storage_type = {
        'local': True,
        'cloud': False,
        'True': True,
        'False': False,
        'true': True,
        'false': False,
        'lokal': True,
        str(1): True,
        str(0): False,
        1: True,
        0: False
    }
    try:
        if isinstance(storage_type, str):
            res = enum_storage_type[storage_type.lower()]
        else:
            res = enum_storage_type[storage_type]
    except:
        res = 0
        print('Not Able to identify storage_type of DataSet\n' +
              'Line: ' + str(get_linenumber_of_occured_error())
              + 'File: ' + str(get_filename_of_occured_error()))
    return res
