# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: SQL Statements for creating the Database Architecture
'''

import Utils.Settings as st


class DataBaseSQL:

    # Returns SQL statement for creating the DataSet Table
    def create_DataSet_table_sql(self):
        sql = ('CREATE TABLE IF NOT EXISTS {table} ('
               + '{col_DATASET_ID} VARCHAR(255) PRIMARY KEY,'
               + '{col_NAME} VARCHAR(255) NOT NULL,'
               + '{col_OWNER} VARCHAR(255) NOT NULL,'
               + '{col_HASH_OF_DATASET} VARCHAR(255) NOT NULL,'
               + '{col_CLEANED} INT NOT NULL,'
               + '{col_SIZE} INT NOT NULL,'
               + '{col_ACCESS_USER_LIST} VARCHAR(65535) NOT NULL,'
               + '{col_ACCESS_BUSINESS_UNIT_LIST} VARCHAR(65535) NOT NULL,'
               + '{col_STORAGE_TYPE} VARCHAR(45) NOT NULL,'
               + '{col_DESCRIPTION} VARCHAR(65535),'
               + '{col_CREATED_AT} TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)')
        sql = sql.format(table=st.TABLE_DATASET, col_DATASET_ID=st.TB_DATASET_COL_DATASET_ID, col_NAME=st.TB_DATASET_COL_NAME,
                         col_OWNER=st.TB_DATASET_COL_OWNER, col_HASH_OF_DATASET=st.TB_DATASET_COL_HASH_OF_DATASET,
                         col_CLEANED=st.TB_DATASET_COL_CLEANED, col_SIZE=st.TB_DATASET_COL_SIZE,
                         col_ACCESS_USER_LIST=st.TB_DATASET_COL_ACCESS_USER_LIST, col_ACCESS_BUSINESS_UNIT_LIST=st.TB_DATASET_COL_ACCESS_BUSINESS_UNIT_LIST,
                         col_STORAGE_TYPE=st.TB_DATASET_COL_STORAGE_TYPE, col_DESCRIPTION=st.TB_DATASET_COL_DESCRIPTION,
                         col_CREATED_AT=st.TB_DATASET_COL_CREATED_AT)
        return sql

    # Returns SQL statement for creating the User Table
    def create_User_table_sql(self):
        sql = ('CREATE TABLE IF NOT EXISTS {table} ('
               + '{col_USER_ID} VARCHAR(255) NOT NULL PRIMARY KEY,'
               + '{col_FIRST_NAME} VARCHAR(255) NOT NULL,'
               + '{col_LAST_NAME} VARCHAR(255) NOT NULL,'
               + '{col_EMAIL} VARCHAR(255) NOT NULL UNIQUE,'
               + '{col_PASSWORD} VARCHAR(513) NOT NULL,'
               + '{col_BUSINESS_UNIT} VARCHAR(255) NOT NULL,'
               + '{col_ACCESS_RIGHTS} VARCHAR(255),'
               + '{col_ADMIN} INT NOT NULL DEFAULT 0,'
               + '{col_ROLE_MANAGER} INT NOT NULL DEFAULT 0,'
               + '{col_USER_DIRECTORY_PATH} VARCHAR(255) DEFAULT "C:\\USERS\\YOUR_USER",'
               + '{col_CREATED_AT} TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL);')
        sql = sql.format(table=st.TABLE_USER, col_USER_ID=st.TB_USER_COL_USER_ID, col_FIRST_NAME=st.TB_USER_COL_FIRST_NAME,
                         col_LAST_NAME=st.TB_USER_COL_LAST_NAME, col_EMAIL=st.TB_USER_COL_EMAIL,
                         col_PASSWORD=st.TB_USER_COL_PASSWORD, col_BUSINESS_UNIT=st.TB_USER_COL_BUSINESS_UNIT,
                         col_ACCESS_RIGHTS=st.TB_USER_COL_ACCESS_RIGHTS, col_ADMIN=st.TB_USER_COL_ADMIN,
                         col_ROLE_MANAGER=st.TB_USER_COL_ROLE_MANAGER, col_USER_DIRECTORY_PATH=st.TB_USER_COL_USER_DIRECTORY_PATH,
                         col_CREATED_AT=st.TB_USER_COL_CREATED_AT)
        return sql

    # Returns SQL statement for creating the Cleanser Table
    def create_Cleanser_table_sql(self):
        sql = ('CREATE TABLE IF NOT EXISTS {table} ('
               + '{col_CLEANSER_ID} VARCHAR(255) NOT NULL PRIMARY KEY,'
               + '{col_NAME} VARCHAR(512) NOT NULL UNIQUE,'
               + '{col_DESCRIPTION} VARCHAR(65535) NOT NULL,'
               + '{col_HEADER_LIST} TEXT(4294967295) NOT NULL,'
               + '{col_DATASETIDS} VARCHAR(65535) NOT NULL,'
               + '{col_CLEANSER_OPERATION_TYPES} VARCHAR(65535) NOT NULL,'
               + '{col_CREATED_AT} TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL);')
        sql = sql.format(table=st.TABLE_CLEANSER, col_CLEANSER_ID=st.TB_CLEANSER_COL_CLEANSER_ID, col_NAME=st.TB_CLEANSER_COL_NAME,
                         col_DESCRIPTION=st.TB_CLEANSER_COL_DESCRIPTION, col_HEADER_LIST=st.TB_CLEANSER_COL_HEADER_LIST,
                         col_DATASETIDS=st.TB_CLEANSER_COL_DATASETIDS, col_CLEANSER_OPERATION_TYPES=st.TB_CLEANSER_OPERATION_TYPES,
                         col_CREATED_AT=st.TB_CLEANSER_COL_CREATED_AT)
        return sql

    # Returns SQL statement for creating the Department Table
    def create_department_table_sql(self):
        sql = ('CREATE TABLE IF NOT EXISTS {table} ('
               + '{col_DEPARTMENT_ID} VARCHAR(255) NOT NULL PRIMARY KEY,'
               + '{col_NAME} VARCHAR(255) NOT NULL UNIQUE,'
               + '{col_CREATED_AT} TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL);')
        sql = sql.format(table=st.TABLE_DEPARTMENTS, col_DEPARTMENT_ID=st.TB_DEPARTMENTS_COL_DEPARTMENT_ID,
                         col_NAME=st.TB_DEPARTMENTS_COL_NAME,
                         col_CREATED_AT=st.TB_DEPARTMENTS_COL_CREATED_AT)
        return sql

    def create_architecture_views_table_sql(self):
        sql = ('CREATE TABLE IF NOT EXISTS {table} ('
               + '{col_ARCHITECTURE_VIEW_ID} VARCHAR(255) NOT NULL PRIMARY KEY,'
               + '{col_NAME} VARCHAR(255) NOT NULL UNIQUE,'
               + '{col_DESCRIPTION} VARCHAR(65535) NOT NULL,'
               + '{col_ARCHITECTURE_VIEW_COMPONENTS} TEXT(4294967295) NOT NULL,'
               + '{col_CREATED_AT} TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL);')
        sql = sql.format(table=st.TABLE_ARCHITECTURE_VIEWS, col_ARCHITECTURE_VIEW_ID=st.TB_ARCHITECTURE_VIEWS_COL_ARCHITECTURE_VIEW_ID,
                         col_NAME=st.TB_ARCHITECTURE_VIEWS_COL_ARCHITECTURE_VIEW_NAME,
                         col_DESCRIPTION=st.TB_ARCHITECTURE_VIEWS_COL_ARCHITECTURE_VIEW_DESCRIPTION,
                         col_ARCHITECTURE_VIEW_COMPONENTS=st.TB_ARCHITECTURE_VIEWS_COL_ARCHITECTURE_VIEW_COMPONENTS,
                         col_CREATED_AT=st.TB_ARCHITECTURE_VIEWS_COL_CREATED_AT)
        return sql

    # Returns SQL statement for creating a many-to-may relation table
    # for datasets and user to define access rights
    def create_User_Dataset_access_relation_table_sql(self):
        sql = ('CREATE TABLE IF NOT EXISTS {table} ('
               + '{foreign_key_1} VARCHAR(255) NOT NULL,'
               + '{foreign_key_2} VARCHAR(255) NOT NULL,'
               + 'PRIMARY KEY ({foreign_key_1}, {foreign_key_2}), '
               + 'FOREIGN KEY ({foreign_key_1}) REFERENCES {table_foreign_key_1}({foreign_key_1}), '
               + 'FOREIGN KEY ({foreign_key_2}) REFERENCES {table_foreign_key_2}({foreign_key_2}), '
               + 'CONSTRAINT {foreign_key_1} '
               + 'FOREIGN KEY ({foreign_key_1}) '
               + 'REFERENCES {table_foreign_key_1} ({foreign_key_1}) '
               + 'ON DELETE CASCADE '
               + 'ON UPDATE CASCADE, '
               + 'CONSTRAINT {foreign_key_2} '
               + 'FOREIGN KEY ({foreign_key_2}) '
               + 'REFERENCES {table_foreign_key_2} ({foreign_key_2}) '
               + 'ON DELETE CASCADE '
               + 'ON UPDATE CASCADE, '
               + 'UNIQUE ({foreign_key_1}, {foreign_key_2}));')
        sql = sql.format(table=st.TABLE_USER_DATASET_ACCESS_RELATION,
                         table_foreign_key_1=st.TABLE_DATASET, table_foreign_key_2=st.TABLE_USER,
                         foreign_key_1=st.TB_DATASET_COL_DATASET_ID, foreign_key_2=st.TB_USER_COL_USER_ID)
        return sql

    def create_Department_Dataset_access_relation_table_sql(self):
        sql = ('CREATE TABLE IF NOT EXISTS {table} ('
               + '{foreign_key_1} VARCHAR(255) NOT NULL,'
               + '{foreign_key_2} VARCHAR(255) NOT NULL,'
               + 'PRIMARY KEY ({foreign_key_1}, {foreign_key_2}), '
               + 'FOREIGN KEY ({foreign_key_1}) REFERENCES {table_foreign_key_1}({foreign_key_1}), '
               + 'FOREIGN KEY ({foreign_key_2}) REFERENCES {table_foreign_key_2}({foreign_key_2}), '
               + 'CONSTRAINT {foreign_key_1}_fk_n3 '
               + 'FOREIGN KEY ({foreign_key_1}) '
               + 'REFERENCES {table_foreign_key_1} ({foreign_key_1}) '
               + 'ON DELETE CASCADE '
               + 'ON UPDATE CASCADE, '
               + 'CONSTRAINT {foreign_key_2}_fk_n3 '
               + 'FOREIGN KEY ({foreign_key_2}) '
               + 'REFERENCES {table_foreign_key_2} ({foreign_key_2}) '
               + 'ON DELETE CASCADE '
               + 'ON UPDATE CASCADE, '
               + 'UNIQUE ({foreign_key_1}, {foreign_key_2}));')
        sql = sql.format(table=st.TABLE_DEPARTMENT_DATASET_ACCESS_RELATION,
                         table_foreign_key_1=st.TABLE_DATASET, table_foreign_key_2=st.TABLE_DEPARTMENTS,
                         foreign_key_1=st.TB_DATASET_COL_DATASET_ID, foreign_key_2=st.TB_DEPARTMENTS_COL_DEPARTMENT_ID)
        return sql

    # Returns SQL statement for creating a many-to-may relation table
    # for cleansers and datasetsd to define compatibility
    def create_Cleanser_Dataset_compatibility_relation_table_sql(self):
        sql = ('CREATE TABLE IF NOT EXISTS {table} ('
               + '{foreign_key_1} VARCHAR(255) NOT NULL,'
               + '{foreign_key_2} VARCHAR(255) NOT NULL,'
               + 'PRIMARY KEY ({foreign_key_1}, {foreign_key_2}), '
               + 'FOREIGN KEY ({foreign_key_1}) REFERENCES {table_foreign_key_1}({foreign_key_1}), '
               + 'FOREIGN KEY ({foreign_key_2}) REFERENCES {table_foreign_key_2}({foreign_key_2}), '
               + 'CONSTRAINT {foreign_key_1}_fk_n2  '
               + 'FOREIGN KEY ({foreign_key_1}) '
               + 'REFERENCES {table_foreign_key_1} ({foreign_key_1}) '
               + 'ON DELETE CASCADE '
               + 'ON UPDATE CASCADE, '
               + 'CONSTRAINT {foreign_key_2}_fk_n2 '
               + 'FOREIGN KEY ({foreign_key_2}) '
               + 'REFERENCES {table_foreign_key_2} ({foreign_key_2}) '
               + 'ON DELETE CASCADE '
               + 'ON UPDATE CASCADE, '
               + 'UNIQUE ({foreign_key_1}, {foreign_key_2}));')
        sql = sql.format(table=st.TABLE_CLEANSER_DATASET_COMPATIBILITY,
                         table_foreign_key_1=st.TABLE_DATASET, table_foreign_key_2=st.TABLE_CLEANSER,
                         foreign_key_1=st.TB_DATASET_COL_DATASET_ID, foreign_key_2=st.TB_CLEANSER_COL_CLEANSER_ID)
        return sql

    def insert_user_dataset_access_relation_values(self, dataset_id, user_id):
        sql = ('INSERT INTO {table} ({col_DATASET_ID}, {col_USER_ID}) '
               + 'VALUES ("{DATASET_ID}", "{USER_ID}");')
        sql = sql.format(table=st.TABLE_USER_DATASET_ACCESS_RELATION,
                         col_DATASET_ID=st.TB_DATASET_COL_DATASET_ID,
                         col_USER_ID=st.TB_USER_COL_USER_ID,
                         DATASET_ID=dataset_id, USER_ID=user_id)
        return sql

    def insert_department_dataset_access_relation_values(self, dataset_id, department_id):
        sql = ('INSERT INTO {table} ({col_DATASET_ID}, {col_DEPARTMENT_ID}) '
               + 'VALUES ("{DATASET_ID}", "{DEPARTMENT_ID}");')
        sql = sql.format(table=st.TABLE_DEPARTMENT_DATASET_ACCESS_RELATION,
                         col_DATASET_ID=st.TB_DATASET_COL_DATASET_ID,
                         col_DEPARTMENT_ID=st.TB_DEPARTMENTS_COL_DEPARTMENT_ID,
                         DATASET_ID=dataset_id, DEPARTMENT_ID=department_id)
        return sql

    def insert_cleanser_dataset_compatability_relation_values(self, dataset_id, cleanser_id):
        sql = ('INSERT INTO {table} ({col_DATASET_ID}, {col_CLEANSER_ID}) '
               + 'VALUES ("{DATASET_ID}", "{CLEANSER_ID}");')
        sql = sql.format(table=st.TABLE_CLEANSER_DATASET_COMPATIBILITY,
                         col_DATASET_ID=st.TB_DATASET_COL_DATASET_ID,
                         col_CLEANSER_ID=st.TB_CLEANSER_COL_CLEANSER_ID,
                         DATASET_ID=dataset_id, CLEANSER_ID=cleanser_id)
        return sql

    # Returns SQL statement for inserting a architecture_view in architecture_view table
    def insert_architecture_view_values(self, architecture_viewID, architecture_view_name,
                                        architecture_view_description, architecture_view_components):
        sql = ('INSERT INTO {table} ({col_ARCHITECTURE_VIEW_ID}, {col_NAME}, {col_DESCRIPTION}, {col_ARCHITECTURE_VIEW_COMPONENTS}) '
               + 'VALUES ("{ARCHITECTURE_VIEW_ID}", "{NAME}", "{DESCRIPTION}", "{ARCHITECTURE_VIEW_COMPONENTS}");')
        sql = sql.format(table=st.TABLE_ARCHITECTURE_VIEWS, col_ARCHITECTURE_VIEW_ID=st.TB_ARCHITECTURE_VIEWS_COL_ARCHITECTURE_VIEW_ID,
                         col_NAME=st.TB_ARCHITECTURE_VIEWS_COL_ARCHITECTURE_VIEW_NAME,
                         col_DESCRIPTION=st.TB_ARCHITECTURE_VIEWS_COL_ARCHITECTURE_VIEW_DESCRIPTION,
                         col_ARCHITECTURE_VIEW_COMPONENTS=st.TB_ARCHITECTURE_VIEWS_COL_ARCHITECTURE_VIEW_COMPONENTS,
                         ARCHITECTURE_VIEW_ID=architecture_viewID, NAME=architecture_view_name,
                         DESCRIPTION=architecture_view_description, ARCHITECTURE_VIEW_COMPONENTS=architecture_view_components)
        return sql

    # Returns SQL statement for inserting a datasets in dataset table
    def insert_datasets_values(self, dataset_id, name, owner, hash_of_dataset, size,
                               cleaned, access_user_list, access_business_unit_list, description, storage_type):
        sql = ('INSERT INTO {table} ({col_DATASET_ID}, {col_NAME}, {col_OWNER}, {col_HASH_OF_DATASET}, {col_CLEANED},'
               + '{col_SIZE}, {col_ACCESS_USER_LIST}, {col_ACCESS_BUSINESS_UNIT_LIST}, {col_STORAGE_TYPE}, {col_DESCRIPTION}) '
               + 'VALUES ("{DATASET_ID}", "{NAME}", "{OWNER}", "{HASH_OF_DATASET}", {CLEANED},'
               + '{SIZE}, "{ACCESS_USER_LIST}", "{ACCESS_BUSINESS_UNIT_LIST}", "{STORAGE_TYPE}", "{DESCRIPTION}");')
        sql = sql.format(table=st.TABLE_DATASET, col_DATASET_ID=st.TB_DATASET_COL_DATASET_ID, col_NAME=st.TB_DATASET_COL_NAME,
                         col_OWNER=st.TB_DATASET_COL_OWNER, col_HASH_OF_DATASET=st.TB_DATASET_COL_HASH_OF_DATASET,
                         col_CLEANED=st.TB_DATASET_COL_CLEANED, col_SIZE=st.TB_DATASET_COL_SIZE,
                         col_ACCESS_USER_LIST=st.TB_DATASET_COL_ACCESS_USER_LIST, col_ACCESS_BUSINESS_UNIT_LIST=st.TB_DATASET_COL_ACCESS_BUSINESS_UNIT_LIST,
                         col_STORAGE_TYPE=st.TB_DATASET_COL_STORAGE_TYPE, col_DESCRIPTION=st.TB_DATASET_COL_DESCRIPTION,
                         DATASET_ID=dataset_id, NAME=name, OWNER=owner, HASH_OF_DATASET=hash_of_dataset,
                         SIZE=size, ACCESS_USER_LIST=access_user_list,
                         ACCESS_BUSINESS_UNIT_LIST=access_business_unit_list, STORAGE_TYPE=storage_type,
                         CLEANED=cleaned, DESCRIPTION=description)
        return sql

    # Returns SQL statement for inserting a user in users table
    def insert_user_values(self, userID, first_name, last_name, email,
                           password, business_unit, access_rights_pillars, admin,
                           role_manager, user_directory_path='C:\\USERS\\YOUR_USER'):
        sql = ('INSERT INTO {table} ({col_USER_ID}, {col_FIRST_NAME}, {col_LAST_NAME}, {col_EMAIL}, {col_PASSWORD},'
               + '{col_BUSINESS_UNIT}, {col_ACCESS_RIGHTS}, {col_ADMIN}, {col_ROLE_MANAGER}, {col_USER_DIRECTORY_PATH}) '
               + 'VALUES ("{USER_ID}", "{FIRST_NAME}", "{LAST_NAME}", "{EMAIL}", "{PASSWORD}",'
               + '"{BUSINESS_UNIT}", "{ACCESS_RIGHTS}", {ADMIN}, {ROLE_MANAGER}, "{USER_DIRECTORY_PATH}");')
        sql = sql.format(table=st.TABLE_USER, col_USER_ID=st.TB_USER_COL_USER_ID, col_FIRST_NAME=st.TB_USER_COL_FIRST_NAME,
                         col_LAST_NAME=st.TB_USER_COL_LAST_NAME, col_EMAIL=st.TB_USER_COL_EMAIL,
                         col_PASSWORD=st.TB_USER_COL_PASSWORD, col_BUSINESS_UNIT=st.TB_USER_COL_BUSINESS_UNIT,
                         col_ACCESS_RIGHTS=st.TB_USER_COL_ACCESS_RIGHTS, col_ADMIN=st.TB_USER_COL_ADMIN,
                         col_ROLE_MANAGER=st.TB_USER_COL_ROLE_MANAGER, col_USER_DIRECTORY_PATH=st.TB_USER_COL_USER_DIRECTORY_PATH,
                         USER_ID=userID, FIRST_NAME=first_name, LAST_NAME=last_name, EMAIL=email,
                         PASSWORD=password, BUSINESS_UNIT=business_unit,
                         ACCESS_RIGHTS=access_rights_pillars, ADMIN=admin,
                         ROLE_MANAGER=role_manager, USER_DIRECTORY_PATH=user_directory_path)
        return sql

    # Returns SQL statement for inserting a department in department table
    def insert_department_values(self, departmentID, department_name):
        sql = ('INSERT INTO {table} ({col_DEPARTMENT_ID}, {col_NAME}) '
               + 'VALUES ("{DEPARTMENT_ID}", "{NAME}");')
        sql = sql.format(table=st.TABLE_DEPARTMENTS, col_DEPARTMENT_ID=st.TB_DEPARTMENTS_COL_DEPARTMENT_ID, col_NAME=st.TB_DEPARTMENTS_COL_NAME,
                         DEPARTMENT_ID=departmentID, NAME=department_name)
        return sql

    # Returns SQL statement for inserting a cleanser in cleansers table
    def insert_cleanser_values(self, cleanserID, name, description, header_list, datasets, cleanser_operation_types):
        sql = ('INSERT INTO {table} ({col_CLEANSER_ID}, {col_NAME}, {col_DESCRIPTION}, {col_HEADER_LIST}, {col_DATASETS}, {col_CLEANSER_OPERATION_TYPES}) '
               + 'VALUES ("{CLEANSER_ID}", "{NAME}", "{DESCRIPTION}", "{HEADER_LIST}", "{DATASETS}", "{CLEANSER_OPERATION_TYPES}");')
        sql = sql.format(table=st.TABLE_CLEANSER, col_CLEANSER_ID=st.TB_CLEANSER_COL_CLEANSER_ID, col_NAME=st.TB_CLEANSER_COL_NAME,
                         col_DESCRIPTION=st.TB_CLEANSER_COL_DESCRIPTION, col_HEADER_LIST=st.TB_CLEANSER_COL_HEADER_LIST,
                         col_DATASETS=st.TB_CLEANSER_COL_DATASETIDS, col_CLEANSER_OPERATION_TYPES=st.TB_CLEANSER_OPERATION_TYPES,
                         CLEANSER_ID=cleanserID, NAME=name, DESCRIPTION=description, HEADER_LIST=header_list, DATASETS=datasets,
                         CLEANSER_OPERATION_TYPES=cleanser_operation_types)
        return sql

    # Returns SQL statement for returning all datasets
    def select_all_from_table_by_access_rights(self, from_table, join_table, join_col_from_table,
                                               join_col_join_table, condition_table, condition,
                                               condition_value, condition_operator='='):
        if isinstance(condition_value, str):
            condition_value = '"{}"'.format(condition_value)
        sql = ('SELECT * '
               + 'FROM {from_table} '
               + 'LEFT JOIN {join_table} ON {from_table}.{join_col_from_table} = {join_table}.{join_col_join_table} '
               + 'WHERE {condition_table}.{condition} {condition_operator} {condition_value}')
        sql = sql.format(from_table=from_table, join_table=join_table,
                         join_col_from_table=join_col_from_table, join_col_join_table=join_col_join_table,
                         condition_table=condition_table, condition=condition, condition_operator=condition_operator,
                         condition_value=condition_value)
        return sql

    # Returns SQL statement for returning an object like dataset, user etc. based on condition like an id.
    # condition represents the column and condition value the specific value like the actual id of an object
    def select_object_by_condition(self, table, condition, condition_value, condition_operator='='):
        if isinstance(condition_value, str):
            condition_value = '"{}"'.format(condition_value)
        sql = ('SELECT * '
               + 'FROM {table} '
               + 'WHERE {column} {condition_operator} {condition_value}')
        sql = sql.format(table=table,
                         column=condition,
                         condition_operator=condition_operator,
                         condition_value=condition_value)
        return sql

    # Returns SQL statement for returning all data contained in a table
    def select_all_data_from_table(self, table):
        sql = ('SELECT * '
               + 'FROM {table}')
        sql = sql.format(table=table)
        return sql

    # Returns SQL statement for returning all entries of column matching the given condition
    def select_all_from_column(self, table, condition, condition_operator, condition_value):
        if isinstance(condition_value, str):
            condition_value = '"{}"'.format(condition_value)
        sql = ('SELECT * '
               + 'FROM {table} '
               + 'WHERE {condition} {condition_operator} {condition_value}')
        sql = sql.format(table=table,
                         condition=condition,
                         condition_operator=condition_operator,
                         condition_value=condition_value)
        return sql

    # Returns SQL statement for updating a column value in a table. Table, column and the value must be submitted.
    # Additionally a condition can be defined.
    def update_value(self, table, column, value, condition=None, condition_operator='=', condition_value=None):
        if isinstance(value, str):
            value = '"{}"'.format(value)
        elif isinstance(value, list):
            value_as_string = ''
            for val in value:
                value_as_string += val + ','
            value = '"{}"'.format(value_as_string[:-1])
        if isinstance(condition_value, str):
            condition_value = '"{}"'.format(condition_value)
        if condition:
            sql = ('UPDATE {table} '
                   + 'SET {column} = {value} '
                   + 'WHERE {condition} {condition_operator} {condition_value}')
            sql = sql.format(table=table, column=column, value=value, condition=condition,
                             condition_operator=condition_operator, condition_value=condition_value)
        else:
            sql = ('UPDATE {table} '
                   + 'SET {column} = {value} ')
            sql = sql.format(table=table, column=column, value=value)
        return sql

    # Returns SQL statement for deleting a rntry from an arbitrary table
    def delete_row_from_table(self, table, condition, condition_value, condition_operator='='):
        if isinstance(condition_value, str):
            condition_value = '"{}"'.format(condition_value)
        sql = ('DELETE FROM {table} '
               + 'WHERE {condition} {condition_operator} {condition_value}')
        sql = sql.format(table=table, condition=condition,
                         condition_operator=condition_operator, condition_value=condition_value)
        return sql

    # Returns SQL statement for deleting a rntry from an arbitrary table
    def drop_table(self, table):
        sql = ('DROP TABLE {table}')
        sql = sql.format(table=table)
        return sql

    # Returns sql with only the headers of a specified table
    def get_column_names_from_table(self, table):
        sql = ('SELECT COLUMN_NAME '
               + 'FROM INFORMATION_SCHEMA.COLUMNS '
               + 'WHERE TABLE_NAME = "{table}"')
        sql = sql.format(table=table)
        return sql
