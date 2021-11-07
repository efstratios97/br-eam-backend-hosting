# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Implements main functionality of the ArchitectureViewManager
'''

import Utils.DataBaseUtils as db_utils
import Utils.DataBaseSQL as sql_stmt
import Utils.Settings as st
import DataManager.DataManager as dm
import ArchitectureViewManager.ArchitectureView as av
import os
import pandas as pd


class ArchitectureViewManager:

    # DataSet Object is created
    def create_architecture_view(self, name,  description, components):
        architecture_viewID = "architecture_view_" + st.create_id()
        architecture_view = av.ArchitectureView(
            architecture_viewID, name, description, components)
        check_unique = self.insert_architecture_view_db(
            ArchitectureViewManager, architecture_view=architecture_view)
        return architecture_view, check_unique

    # Creates Table (if not already exists) and then inserts architecture_view into the table

    def insert_architecture_view_db(self, architecture_view: av.ArchitectureView, local=False):
        # Creates Table
        db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.
            create_architecture_views_table_sql(sql_stmt.DataBaseSQL), local=local)
        # Inserts data
        check_unique = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                          sql_statement=sql_stmt.DataBaseSQL.
                                                          insert_architecture_view_values(sql_stmt.DataBaseSQL,
                                                                                          architecture_viewID=architecture_view.get_architecture_viewID(),
                                                                                          architecture_view_name=architecture_view.get_name(),
                                                                                          architecture_view_description=architecture_view.get_description(),
                                                                                          architecture_view_components=architecture_view.get_components()), local=local)
        return check_unique

    def delete_architecture_view(self, architecture_view_id, condition_operator='=', local=False):
        db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils,
            sql_statement=sql_stmt.DataBaseSQL.delete_row_from_table(
                sql_stmt.DataBaseSQL, table=st.TABLE_ARCHITECTURE_VIEWS, condition=st.TB_ARCHITECTURE_VIEWS_COL_ARCHITECTURE_VIEW_ID,
                condition_value=architecture_view_id, condition_operator=condition_operator), local=local)

    def get_all_architecture_views(self):
        data = []
        result = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                    sql_stmt.DataBaseSQL.select_all_data_from_table(
                                                        sql_stmt.DataBaseSQL, table=st.TABLE_ARCHITECTURE_VIEWS),
                                                    fetchall=True, local=False)
        if result:
            for row in result:
                architecture_view = self.__parse_architecture_view_obj(
                    ArchitectureViewManager, row)
                data.append(architecture_view)
        return data

    def get_architecture_view_by_id(self, architecture_view_id, local=False):
        result = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                    sql_statement=sql_stmt.DataBaseSQL.select_object_by_condition(
                                                        sql_stmt.DataBaseSQL, table=st.TABLE_ARCHITECTURE_VIEWS,
                                                        condition=st.TB_ARCHITECTURE_VIEWS_COL_ARCHITECTURE_VIEW_ID,
                                                        condition_value=architecture_view_id),
                                                    local=local, fetchone=True)
        architecture_view = self.__parse_architecture_view_obj(
            ArchitectureViewManager, db_row=result)
        return architecture_view

    def get_components(self, dataset_id, local=False):
        components = dm.DataManager.get_dataset_table_headers(
            dm.DataManager, dataset_id, local)
        return components

    def get_departments(self, dataset_id, local=False):
        dataset = dm.DataManager.get_table_as_df(dm.DataManager, dataset_id)
        departments = list(
            set(dataset['Verantwortliche Organisationseinheit'].to_list()))
        return departments

    def __parse_architecture_view_obj(self, db_row):
        if db_row:
            architecture_view_id = db_row[0]
            name = db_row[1]
            description = db_row[2]
            components = db_row[3]
            architecture_view = av.ArchitectureView(
                architecture_view_id, name, description, components)
            return architecture_view
        pass

    def analyze_applicability(self, dataset_id, department, architecture_view: av.ArchitectureView):
        data = dm.DataManager.get_table_as_df(
            dm.DataManager, table=dataset_id)
        if not department == "All":
            data = data[data['Verantwortliche Organisationseinheit']
                        == department]
        architecture_view_components = architecture_view.get_components().split(",")
        series = []
        labels = []
        for component in architecture_view_components:
            series.append(
                {"data": [(1 - (data[component].isna().sum() / len(data[component])))*100],
                 "name": component})
            labels.append(component)
        return series, labels
