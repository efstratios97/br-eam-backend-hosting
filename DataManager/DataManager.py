# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Implements main functionality of the DataManager
'''

import DataManager.DataSet as ds
import Utils.DataBaseUtils as db_utils
import Utils.DataBaseSQL as sql_stmt
import Utils.Settings as st
import DataCleanser.DataCleanser as dc
import UserManager.UserManager as um
import os
import pandas as pd


class DataManager:

    # DataSet Object is created
    def create_data_set(self, name, owner, data,
                        cleaned, access_user_list, access_business_unit_list, description,
                        storage_type, size=0):
        datasetID = "dataset_" + st.create_id()
        hash_of_dataset = st.hash_data(data)
        size = data.size
        dataset = ds.DataSet(datasetID=datasetID, name=name, owner=owner, size=size,
                             hash_of_dataset=hash_of_dataset, cleaned=cleaned, access_user_list=access_user_list,
                             access_business_unit_list=access_business_unit_list, description=description,
                             storage_type=storage_type, data=data)
        local = st.enum_storage_type_bool(
            storage_type=storage_type)
        self.insert_dataset_db(DataManager, dataset=dataset, local=local)
        self.insert_dataset_data_db(DataManager, dataset=dataset, local=local)
        self.insert_user_dataset_access_relation_db(
            DataManager, dataset=dataset)
        self.insert_department_dataset_access_relation_db(
            DataManager, dataset=dataset)
        return dataset

    # Creates Table (if not already exists) and then inserts data into the table

    def insert_dataset_db(self, dataset: ds.DataSet, local: bool):
        # Creates Table
        db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.
            create_DataSet_table_sql(sql_stmt.DataBaseSQL), local=local)
        # Inserts data
        db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                           sql_statement=sql_stmt.DataBaseSQL.
                                           insert_datasets_values(sql_stmt.DataBaseSQL,
                                                                  dataset_id=dataset.get_datasetID(), name=dataset.get_name(), owner=dataset.get_owner(),
                                                                  hash_of_dataset=dataset.get_hash_of_dataset(), size=dataset.get_size(),
                                                                  access_user_list=dataset.get_access_user_list(), cleaned=dataset.get_cleaned(),
                                                                  access_business_unit_list=dataset.get_access_business_unit_list(),
                                                                  description=dataset.get_description(), storage_type=dataset.get_storage_type()), local=local)

    # Creates a table for the data holded by a dataset object and insert it into the created table
    def insert_dataset_data_db(self, dataset: ds.DataSet, local: bool):
        db_engine = db_utils.DataBaseUtils.create_db_engine(
            db_utils.DataBaseUtils, local=local)
        dataset.get_data().to_sql(dataset.get_datasetID(),
                                  con=db_engine, if_exists='append', index=False)

    def insert_user_dataset_access_relation_db(self, dataset: ds.DataSet, local=False):
        # Creates Table
        db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.
            create_User_Dataset_access_relation_table_sql(sql_stmt.DataBaseSQL), local=local)
        # Inserts data
        for user in dataset.get_access_user_list().split(','):
            db_utils.DataBaseUtils.execute_sql(
                db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.
                insert_user_dataset_access_relation_values(
                    sql_stmt.DataBaseSQL, dataset_id=dataset.get_datasetID(), user_id=user),
                local=local)

    def insert_department_dataset_access_relation_db(self, dataset: ds.DataSet, local=False):
        # Creates Table
        db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.create_Department_Dataset_access_relation_table_sql(
                sql_stmt.DataBaseSQL), local=local)
        # Inserts data
        for department in dataset.get_access_business_unit_list().split(','):
            department = self.enum_depName_depID(
                DataManager, selected_department_name=department)
            db_utils.DataBaseUtils.execute_sql(
                    db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.
                    insert_department_dataset_access_relation_values(
                        sql_stmt.DataBaseSQL, dataset_id=dataset.get_datasetID(), department_id=department),
                    local=local)

    def enum_depName_depID(self, selected_department_name):
        departments = um.UserManager.get_departments(um.UserManager)
        for dep in departments:
            if dep['name'] == selected_department_name:
                return dep['department_id']

    # Updates all relevant data values in a dataset
    def update_dataset(self, dataset: ds.DataSet, local: bool):
        dataset_id = dataset.get_datasetID()
        self.update_dataset_cleaned(
            DataManager, cleaned=dataset.get_cleaned(), dataset_id=dataset_id, local=local)
        self.update_dataset_name(
            DataManager, name=dataset.get_name(), dataset_id=dataset_id, local=local)
        self.update_dataset_access_user_list(
            DataManager, access_user_list=dataset.get_access_user_list(), dataset_id=dataset_id, local=local)
        self.update_dataset_access_business_unit_list(
            DataManager, access_business_unit_list=dataset.get_access_business_unit_list(), dataset_id=dataset_id, local=local)
        self.update_dataset_description(
            DataManager, description=dataset.get_description(), dataset_id=dataset_id, local=local)

    # Updates the cleaned value in a dataset
    def update_dataset_cleaned(self, cleaned, dataset_id, local: bool):
        db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                           sql_statement=sql_stmt.DataBaseSQL.
                                           update_value(sql_stmt.DataBaseSQL, table=st.TABLE_DATASET,
                                                        column=st.TB_DATASET_COL_CLEANED, value=cleaned,
                                                        condition=st.TB_DATASET_COL_DATASET_ID,
                                                        condition_operator='=', condition_value=dataset_id),
                                           local=local)

    # Updates the name value in a dataset
    def update_dataset_name(self, name, dataset_id, local: bool):
        db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                           sql_statement=sql_stmt.DataBaseSQL.
                                           update_value(sql_stmt.DataBaseSQL, table=st.TABLE_DATASET,
                                                        column=st.TB_DATASET_COL_NAME, value=name,
                                                        condition=st.TB_DATASET_COL_DATASET_ID,
                                                        condition_operator='=', condition_value=dataset_id),
                                           local=local)

    # Updates the access_user_list value in a dataset
    def update_dataset_access_user_list(self, access_user_list, dataset_id, local: bool):
        db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                           sql_statement=sql_stmt.DataBaseSQL.
                                           update_value(sql_stmt.DataBaseSQL, table=st.TABLE_DATASET,
                                                        column=st.TB_DATASET_COL_ACCESS_USER_LIST, value=access_user_list,
                                                        condition=st.TB_DATASET_COL_DATASET_ID,
                                                        condition_operator='=', condition_value=dataset_id),
                                           local=local)

    # Updates the access_business_unit value in a dataset
    def update_dataset_access_business_unit_list(self, access_business_unit_list, dataset_id, local: bool):
        db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                           sql_statement=sql_stmt.DataBaseSQL.
                                           update_value(sql_stmt.DataBaseSQL, table=st.TABLE_DATASET,
                                                        column=st.TB_DATASET_COL_ACCESS_BUSINESS_UNIT_LIST, value=access_business_unit_list,
                                                        condition=st.TB_DATASET_COL_DATASET_ID,
                                                        condition_operator='=', condition_value=dataset_id),
                                           local=local)

    # Updates the description value in a dataset
    def update_dataset_description(self, description, dataset_id, local: bool):
        db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                           sql_statement=sql_stmt.DataBaseSQL.
                                           update_value(sql_stmt.DataBaseSQL, table=st.TABLE_DATASET,
                                                        column=st.TB_DATASET_COL_DESCRIPTION, value=description,
                                                        condition=st.TB_DATASET_COL_DATASET_ID,
                                                        condition_operator='=', condition_value=dataset_id),
                                           local=local)

    # Returns all dataset from the databases (local and cloud) and further create
    def get_all_datasets(self, user_id: str):
        data = []
        result_local = None
        result_cloud = None
        try:
            result_local = db_utils.DataBaseUtils.execute_sql(
                db_utils.DataBaseUtils,
                sql_statement=sql_stmt.DataBaseSQL.select_all_data_from_table(sql_stmt.DataBaseSQL,
                                                                              st.TABLE_DATASET),
                local=True, fetchall=True)
        except:
            print('No local db')
        try:
            result_cloud = db_utils.DataBaseUtils.execute_sql(
                db_utils.DataBaseUtils,
                sql_statement=sql_stmt.DataBaseSQL.
                select_all_from_table_by_access_rights(sql_stmt.DataBaseSQL, from_table=st.TABLE_DATASET,
                                                       join_table=st.TABLE_USER_DATASET_ACCESS_RELATION,
                                                       join_col_from_table=st.TB_DATASET_COL_DATASET_ID,
                                                       join_col_join_table=st.TB_DATASET_COL_DATASET_ID,
                                                       condition_table=st.TABLE_USER_DATASET_ACCESS_RELATION,
                                                       condition=st.TB_USER_COL_USER_ID,
                                                       condition_value=user_id, condition_operator='='),
                local=False, fetchall=True)
            user = um.UserManager.get_user_by_id(um.UserManager, user_id)
            user_dep_id = self.enum_depName_depID(
                DataManager, user.get_business_unit())
            result_cloud_by_dep = db_utils.DataBaseUtils.execute_sql(
                db_utils.DataBaseUtils,
                sql_statement=sql_stmt.DataBaseSQL.
                select_all_from_table_by_access_rights(sql_stmt.DataBaseSQL, from_table=st.TABLE_DATASET,
                                                       join_table=st.TABLE_DEPARTMENT_DATASET_ACCESS_RELATION,
                                                       join_col_from_table=st.TB_DATASET_COL_DATASET_ID,
                                                       join_col_join_table=st.TB_DATASET_COL_DATASET_ID,
                                                       condition_table=st.TABLE_DEPARTMENT_DATASET_ACCESS_RELATION,
                                                       condition=st.TB_DEPARTMENTS_COL_DEPARTMENT_ID,
                                                       condition_value=user_dep_id, condition_operator='='),
                local=False, fetchall=True)
            result_cloud = result_cloud + result_cloud_by_dep
        except:
            print('No cloud db')
        dataset_ids = []
        if result_local:
            for row in result_local:
                dataset = self.parse_dataset_obj(DataManager, row)
                data.append(dataset)
        if result_cloud:
            for row in result_cloud:
                if not row[0] in dataset_ids:
                    dataset = self.parse_dataset_obj(DataManager, row)
                    data.append(dataset)
                    dataset_ids.append(row[0])
        return data

    def get_all_datasets_only_id(self, user_id: str, local=False):
        data = []
        user = um.UserManager.get_user_by_id(um.UserManager, user_id)
        user_dep_id = self.enum_depName_depID(
            DataManager, user.get_business_unit())
        result = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                    sql_statement=sql_stmt.DataBaseSQL.
                                                    select_all_from_column(sql_stmt.DataBaseSQL, table=st.TABLE_USER_DATASET_ACCESS_RELATION,
                                                                           condition=st.TB_USER_COL_USER_ID,
                                                                           condition_operator='=', condition_value=user_id),
                                                    fetchall=True, local=local)
        result_2 = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                      sql_statement=sql_stmt.DataBaseSQL.
                                                      select_all_from_column(sql_stmt.DataBaseSQL, table=st.TABLE_DEPARTMENT_DATASET_ACCESS_RELATION,
                                                                             condition=st.TB_DEPARTMENTS_COL_DEPARTMENT_ID,
                                                                             condition_operator='=', condition_value=user_dep_id),
                                                      fetchall=True, local=local)
        if result_2:
            result = result + result_2
        dataset_ids = []
        for row in result:
            if not row[0] in dataset_ids:
                dataset_id = row[0]
                data.append(dataset_id)
                dataset_ids.append(row[0])
        return data

    def get_all_datasets_only_name(self, user_id: str, local=False):
        data = []
        user = um.UserManager.get_user_by_id(um.UserManager, user_id)
        user_dep_id = self.enum_depName_depID(
            DataManager, user.get_business_unit())
        result = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                    sql_statement=sql_stmt.DataBaseSQL.
                                                    select_all_from_column(sql_stmt.DataBaseSQL, table=st.TABLE_USER_DATASET_ACCESS_RELATION,
                                                                           condition=st.TB_USER_COL_USER_ID,
                                                                           condition_operator='=', condition_value=user_id),
                                                    fetchall=True, local=local)
        result_2 = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                      sql_statement=sql_stmt.DataBaseSQL.
                                                      select_all_from_column(sql_stmt.DataBaseSQL, table=st.TABLE_DEPARTMENT_DATASET_ACCESS_RELATION,
                                                                             condition=st.TB_DEPARTMENTS_COL_DEPARTMENT_ID,
                                                                             condition_operator='=', condition_value=user_dep_id),
                                                      fetchall=True, local=local)
        if result_2:
            result = result + result_2
        dataset_ids = []
        for row in result:
            if not row[0] in dataset_ids:
                dataset_ids.append(row[0])
        result_final = ()
        for dataset_id in dataset_ids:
            result_final = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                              sql_statement=sql_stmt.DataBaseSQL.
                                                              select_all_from_column(sql_stmt.DataBaseSQL, table=st.TABLE_DATASET,
                                                                                     condition=st.TB_DATASET_COL_DATASET_ID,
                                                                                     condition_operator='=', condition_value=dataset_id),
                                                              fetchone=True, local=local)
            dataset_name = result_final[1]
            dataset_id = result_final[0]
            data.append({"dataset_id": dataset_id, "name": dataset_name})
        return data

    def get_table_as_df(self, table, local=False):
        db_engine = db_utils.DataBaseUtils.create_db_engine(
            db_utils.DataBaseUtils, local=local)
        result = pd.read_sql(sql_stmt.DataBaseSQL.select_all_data_from_table(
            sql_stmt.DataBaseSQL, table=table), con=db_engine.connect())
        return result

    def parse_dataset_obj(self, db_row):
        if db_row:
            dataset_id = db_row[0]
            name = db_row[1]
            owner = db_row[2]
            hash_of_dataset = db_row[3]
            cleaned = ds.DataSet.enum_cleaned(ds.DataSet, cleaned=db_row[4])
            size = db_row[5]
            access_user_list = db_row[6]
            access_business_unit_list = db_row[7]
            storage_type = db_row[8]
            description = db_row[9]
            local = st.enum_storage_type_bool(
                storage_type=storage_type)
            data = self.get_table_as_df(
                DataManager, table=dataset_id, local=local)
            dataset = ds.DataSet(datasetID=dataset_id, name=name, owner=owner, size=size,
                                 hash_of_dataset=hash_of_dataset, cleaned=cleaned, access_user_list=access_user_list,
                                 access_business_unit_list=access_business_unit_list, description=description,
                                 storage_type=storage_type, data=data)
            return dataset
        pass

    # Returns a dataset from the databases (local or cloud) based on dataset_id.
    # Storage_type musrt be provided to select right database
    def get_dataset_by_id(self, dataset_id, local: bool):
        result = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                    sql_statement=sql_stmt.DataBaseSQL.select_object_by_condition(
                                                        sql_stmt.DataBaseSQL, table=st.TABLE_DATASET,
                                                        condition=st.TB_DATASET_COL_DATASET_ID, condition_value=dataset_id),
                                                    local=local, fetchone=True)
        dataset = self.parse_dataset_obj(DataManager, db_row=result)
        return dataset

    # Deletes a dataset from the database

    def delete_dataset(self, dataset_id, local: bool, condition_operator='='):
        db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils,
            sql_statement=sql_stmt.DataBaseSQL.delete_row_from_table(
                sql_stmt.DataBaseSQL, table=st.TABLE_DATASET, condition=st.TB_DATASET_COL_DATASET_ID,
                condition_value=dataset_id, condition_operator=condition_operator), local=local)
        cleansers = dc.DataCleanser.get_all_cleansers(
            dc.DataCleanser)
        for cleanser in cleansers:
            cleanser_datasets = cleanser.get_datasets().split(',')
            if dataset_id in cleanser_datasets:
                cleanser_datasets.remove(dataset_id)
                db_utils.DataBaseUtils.execute_sql(
                    db_utils.DataBaseUtils,
                    sql_statement=sql_stmt.DataBaseSQL.update_value(
                        sql_stmt.DataBaseSQL, table=st.TABLE_CLEANSER, column=st.TB_CLEANSER_COL_DATASETIDS, value=cleanser_datasets, condition=st.TB_CLEANSER_COL_CLEANSER_ID,
                        condition_value=cleanser.get_cleanserID(), condition_operator=condition_operator), local=local)

    # Drops the table holding the data of a dataset
    def drop_dataset_table(self, dataset_id, local: bool):
        db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils,
            sql_statement=sql_stmt.DataBaseSQL.drop_table(
                sql_stmt.DataBaseSQL, table=dataset_id), local=local)

    def get_dataset_table_headers(self, dataset_id, local: bool):
        header_list = []
        headers = db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils,
            sql_statement=sql_stmt.DataBaseSQL.get_column_names_from_table(
                sql_stmt.DataBaseSQL, table=dataset_id), local=local, fetchall=True)
        for header in headers:
            header_list.append(header[0])
        return header_list
