# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Implements main functionality of the DataCleanser
'''

import Utils.Settings as st
import DataCleanser.Cleanser as cl
import DataManager.DataManager as dm
import DataManager.DataSet as ds
import CustomCleanserAlgorithms.Clean_BR_EAM_Repository as cca
import Utils.DataBaseUtils as db_utils
import Utils.DataBaseSQL as sql_stmt
import pandas as pd


class DataCleanser:

    def create_cleanser(self, name, description, datasets, cleanser_operation_types, local=False):
        cleanser_id = 'cleanser_' + st.create_id()
        header_list = []
        for dataset_id in datasets.split(','):
            header_list = self.update_header_list(DataCleanser,
                                                  dataset_id=dataset_id, header_list=header_list)
        cleanser = cl.Cleanser(
            cleanser_id, name, description, header_list, datasets, cleanser_operation_types)
        check = self.insert_cleanser_db(DataCleanser, cleanser)
        for dataset_id in datasets.split(','):
            self.insert_cleanser_dataset_compatibility_relation_db(DataCleanser,
                                                                   cleanser, dataset_id)
        return cleanser, check

    def insert_cleanser_db(self, cleanser: cl.Cleanser, local=False):
        # Creates Table
        db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                           sql_stmt.DataBaseSQL.create_Cleanser_table_sql(sql_stmt.DataBaseSQL), local=local)
        # Inserts cleanser
        check = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                   sql_statement=sql_stmt.DataBaseSQL.
                                                   insert_cleanser_values(sql_stmt.DataBaseSQL,
                                                                          cleanserID=cleanser.get_cleanserID(), name=cleanser.get_name(), description=cleanser.get_description(),
                                                                          header_list=cleanser.get_header_list(), datasets=cleanser.get_datasets(),
                                                                          cleanser_operation_types=cleanser.get_cleanser_operation_types()), local=local)
        return check

    def insert_cleanser_dataset_compatibility_relation_db(self, cleanser: cl.Cleanser, dataset_id, local=False):
        # Creates Table
        db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.
            create_Cleanser_Dataset_compatibility_relation_table_sql(sql_stmt.DataBaseSQL), local=local)
        # Inserts data
        db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.
            insert_cleanser_dataset_compatability_relation_values(
                sql_stmt.DataBaseSQL, cleanser_id=cleanser.get_cleanserID(), dataset_id=dataset_id),
            local=local)

    def get_all_suitable_cleansers_by_dataset(self, dataset_id, local=False):
        data = []
        result = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                    sql_statement=sql_stmt.DataBaseSQL.
                                                    select_all_from_column(sql_stmt.DataBaseSQL, table=st.TABLE_CLEANSER_DATASET_COMPATIBILITY,
                                                                           condition=st.TB_DATASET_COL_DATASET_ID,
                                                                           condition_operator='=', condition_value=dataset_id),
                                                    fetchall=True, local=local)
        for row in result:
            cleanser = self.get_cleanser_by_id(DataCleanser, row[1])
            data.append(cleanser)
        return data

    def get_all_suitable_cleansers_by_header(self, user_id):
        suitable_cleaners = []
        cleansers = self.get_all_cleansers(DataCleanser)
        datasets = dm.DataManager.get_all_datasets_only_id(
            dm.DataManager, user_id)
        for cleanser in cleansers:
            for dataset_id in datasets:
                dataset_header = dm.DataManager.get_dataset_table_headers(
                    dm.DataManager, dataset_id=dataset_id, local=False)
                if cleanser.get_header_list() == st.make_list_to_str(dataset_header):
                    suitable_cleaners.append(cleanser)
                elif all(x in cleanser.get_header_list() for x in dataset_header):
                    suitable_cleaners.append(cleanser)
        return list(set(suitable_cleaners))

    def update_header_list(self, dataset_id, header_list):
        if isinstance(header_list, list):
            header_list = st.make_list_to_str(header_list)
        if isinstance(header_list, str):
            header_list = header_list.split(',')
        header_list_tmp = dm.DataManager.get_dataset_table_headers(
            dm.DataManager, dataset_id=dataset_id, local=False)
        if not st.check_list_identical(header_list, header_list_tmp):
            if header_list[0] == '' and len(header_list) == 1:
                header_list = header_list_tmp
            elif not all(x in header_list for x in header_list_tmp):
                [header_list.append(
                    val) for val in header_list_tmp if not val in header_list]
        header_list = st.make_list_to_str(header_list)
        return header_list

    def update_all_cleansers(self, user_id, local=False):
        cleansers = self.get_all_cleansers(DataCleanser)
        datasets = dm.DataManager.get_all_datasets_only_id(
            dm.DataManager, user_id)
        for cleanser in cleansers:
            datasets_suitable = []
            updated_header_list = []
            for dataset_id in datasets:
                if not datasets_suitable:
                    datasets_suitable = cleanser.get_datasets().split(',')
                if not dataset_id in datasets_suitable:
                    updated_header_list = self.update_header_list(DataCleanser,
                                                                  dataset_id, cleanser.get_header_list())
                    if not updated_header_list == cleanser.get_header_list():
                        db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                           sql_statement=sql_stmt.DataBaseSQL.
                                                           update_value(sql_stmt.DataBaseSQL, table=st.TABLE_CLEANSER,
                                                                        column=st.TB_CLEANSER_COL_HEADER_LIST, value=updated_header_list,
                                                                        condition=st.TB_CLEANSER_COL_CLEANSER_ID,
                                                                        condition_operator='=', condition_value=cleanser.get_cleanserID()),
                                                           local=local)
                    suitable_cleansers = [val.get_cleanserID()
                                          for val in self.get_all_suitable_cleansers_by_header(DataCleanser, user_id)]
                    for suitable_cleanser in suitable_cleansers:
                        if cleanser.get_cleanserID() in suitable_cleansers:
                            if not dataset_id in datasets_suitable:
                                datasets_suitable.append(
                                    dataset_id)
                            db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                               sql_statement=sql_stmt.DataBaseSQL.
                                                               update_value(sql_stmt.DataBaseSQL, table=st.TABLE_CLEANSER,
                                                                            column=st.TB_CLEANSER_COL_DATASETIDS, value=datasets_suitable,
                                                                            condition=st.TB_CLEANSER_COL_CLEANSER_ID,
                                                                            condition_operator='=', condition_value=cleanser.get_cleanserID()),
                                                               local=local)
                            self.insert_cleanser_dataset_compatibility_relation_db(DataCleanser,
                                                                                   cleanser, dataset_id)

    def define_operations(self):
        self.cleanser_operation_types = {
            "Remove Dummy Applications": cca.BRCleanserAlgorithms.remove_dummy_appications,
            "Identify & Remove Duplicate Applications": cca.BRCleanserAlgorithms.remove_duplicate_appications,
            "Remove Test Applications": cca.BRCleanserAlgorithms.remove_test_appications
        }

    def apply_cleanser(self, cleanser_id, dataset_id, cleanser_operation_types, local=False):
        self.define_operations(DataCleanser)
        df = dm.DataManager.get_table_as_df(
            dm.DataManager, table=dataset_id, local=local)
        df_out = pd.DataFrame(columns=df.columns.to_list())
        for cleanser_operation in cleanser_operation_types:
            df, df_out = self.cleanser_operation_types[cleanser_operation](
                cca.BRCleanserAlgorithms, df, df_out)
        return df, df_out

    def get_all_cleanser_operation_types(self):
        self.define_operations(DataCleanser)
        return list(self.cleanser_operation_types.keys())

    def get_all_cleansers(self):
        data = []
        result = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                    sql_stmt.DataBaseSQL.select_all_data_from_table(
                                                        sql_stmt.DataBaseSQL, table=st.TABLE_CLEANSER),
                                                    fetchall=True, local=False)
        if result:
            for row in result:
                cleanser = self.__parse_cleanser_obj(DataCleanser, row)
                data.append(cleanser)
        return data

    def get_cleanser_by_id(self, cleanser_id, local=False):
        result = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                    sql_statement=sql_stmt.DataBaseSQL.select_object_by_condition(
                                                        sql_stmt.DataBaseSQL, table=st.TABLE_CLEANSER,
                                                        condition=st.TB_CLEANSER_COL_CLEANSER_ID, condition_value=cleanser_id),
                                                    local=local, fetchone=True)
        cleanser = self.__parse_cleanser_obj(DataCleanser, db_row=result)
        return cleanser

    def delete_cleanser(self, cleanser_id, condition_operator='=', local=False):
        db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils,
            sql_statement=sql_stmt.DataBaseSQL.delete_row_from_table(
                sql_stmt.DataBaseSQL, table=st.TABLE_CLEANSER, condition=st.TB_CLEANSER_COL_CLEANSER_ID,
                condition_value=cleanser_id, condition_operator=condition_operator), local=local)

    def __parse_cleanser_obj(self, db_row):
        if db_row:
            cleanser_id = db_row[0]
            name = db_row[1]
            description = db_row[2]
            header_list = db_row[3]
            datasets = db_row[4]
            cleanser_operation_types = db_row[5]
            cleanser = cl.Cleanser(cleanser_id=cleanser_id, name=name, description=description,
                                   header_list=header_list, datasets=datasets, cleanser_operation_types=cleanser_operation_types)
            return cleanser
        pass
