# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Implements main functionality of the UserManager
'''

import UserManager.User as us
import Utils.DataBaseUtils as db_utils
import Utils.DataBaseSQL as sql_stmt
import Utils.Settings as st
import DataManager.DataManager as dm
import DataManager.DataSet as ds
import os
import pandas as pd
import json


class UserManager:

    def create_user(self, first_name: str, last_name: str, email: str,
                    password: str, business_unit: str, access_rights_pillars, admin, role_manager, operation_issuer):
        if self.check_admin(UserManager, operation_issuer):
            user_id = "user_" + st.create_id()
            password = st.create_hash_password_sha512(
                password=password, complementary_input=user_id)
            access_rights_pillars_dict = self.__parse_access_rigths_pillars(
                UserManager, access_rights_pillars)
            user = us.User(userID=user_id, first_name=first_name, last_name=last_name, email=email,
                           password=password, business_unit=business_unit,
                           access_rights_pillars=access_rights_pillars_dict, admin=admin,
                           role_manager=role_manager)
            self.insert_user_db(UserManager, user=user)
            return user

    #
    def __parse_access_rigths_pillars(self, access_rights_pillars: str):
        access_rights_pillars = access_rights_pillars.replace("'", '"')
        return json.loads(access_rights_pillars)

    def insert_user_db(self, user: us.User):
        local = False
        access_rights_pillars = json.dumps(user.get_access_rights_pillars())
        access_rights_pillars = access_rights_pillars.replace('"', "'")
        # Creates Table
        db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.
            create_User_table_sql(sql_stmt.DataBaseSQL), local=local)
        # Inserts data
        db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                           sql_statement=sql_stmt.DataBaseSQL.
                                           insert_user_values(
                                               sql_stmt.DataBaseSQL, userID=user.get_userID(), first_name=user.get_first_name(),
                                               last_name=user.get_last_name(), email=user.get_email(), password=user.get_password(),
                                               business_unit=user.get_business_unit(), access_rights_pillars=access_rights_pillars,
                                               admin=user.get_admin(), role_manager=user.get_role_manager()), local=local)

    def update_password(self, new_password, user_id):
        local = False
        new_password = st.create_hash_password_sha512(
            password=new_password, complementary_input=user_id)
        db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.update_value(
                sql_stmt.DataBaseSQL, table=st.TABLE_USER, column=st.TB_USER_COL_PASSWORD, value=new_password,
                condition=st.TB_USER_COL_USER_ID, condition_value=user_id), local=local)

    def update_department(self, department, user_id, local=False):
        if self.check_admin(UserManager, user_id):
            db_utils.DataBaseUtils.execute_sql(
                db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.update_value(
                    sql_stmt.DataBaseSQL, table=st.TABLE_USER, column=st.TB_USER_COL_BUSINESS_UNIT, value=department,
                    condition=st.TB_USER_COL_USER_ID, condition_value=user_id), local=local)
            return True
        else:
            return False

    # Checks whether the given password matches the db entry

    def check_password(self, password_user, user_id):
        local = False
        result = db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.
            select_object_by_condition(sql_stmt.DataBaseSQL, table=st.TABLE_USER, condition=st.TB_USER_COL_USER_ID,
                                       condition_value=user_id), fetchone=True, local=local)
        user = self.__parse_user_obj(UserManager, result)
        password_db = user.get_password()
        password_user = st.create_hash_password_sha512(
            password=password_user, complementary_input=user_id)
        if password_db == password_user:
            return True
        else:
            return False

    def get_user_by_id(self, user_id):
        result = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                    sql_stmt.DataBaseSQL.
                                                    select_object_by_condition(
                                                        sql_stmt.DataBaseSQL, table=st.TABLE_USER,
                                                        condition=st.TB_USER_COL_USER_ID,
                                                        condition_value=user_id),
                                                    fetchone=True, local=False)
        user = self.__parse_user_obj(UserManager, result)
        return user

    def get_user_by_email(self, email):
        result = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                    sql_stmt.DataBaseSQL.
                                                    select_object_by_condition(
                                                        sql_stmt.DataBaseSQL, table=st.TABLE_USER,
                                                        condition=st.TB_USER_COL_EMAIL,
                                                        condition_value=email),
                                                    fetchone=True, local=False)
        user = self.__parse_user_obj(UserManager, result)
        return user

    def get_all_users(self):
        data = []
        result = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                    sql_stmt.DataBaseSQL.select_all_data_from_table(
                                                        sql_stmt.DataBaseSQL, table=st.TABLE_USER),
                                                    fetchall=True, local=False)
        for row in result:
            user = self.__parse_user_obj(UserManager, row)
            data.append(user)
        return data

    def delete_user(self, user_issuer, user_to_delete):
        if (self.check_admin(UserManager, user_issuer) or user_issuer == user_to_delete) and len(self.get_all_users(UserManager)) > 1:
            # Delete datasets which only deleted user has access
            try:
                datasets = dm.DataManager.get_all_datasets(
                    dm.DataManager, user_id=user_to_delete)
                for dataset in datasets:
                    access_user_list = dataset.get_access_user_list().split(',')
                    local = st.enum_storage_type_bool(
                        storage_type=dataset.get_storage_type())
                    if len(access_user_list) <= 1:
                        dm.DataManager.delete_dataset(
                            dm.DataManager, dataset.get_datasetID(),
                            local=local)
                        dm.DataManager.drop_dataset_table(
                            dm.DataManager, dataset_id=dataset.get_datasetID(), local=local)
                    # If user to delete is dataset owner and not the only one with access, he gets assigned an successor
                    elif dataset.get_owner() == user_to_delete:
                        new_owner = ''
                        access_user_list.remove(user_to_delete)
                        if not user_issuer == user_to_delete:
                            new_owner = user_issuer
                        else:
                            new_owner = [
                                val for val in access_user_list if not val == user_to_delete][0]
                        if len(access_user_list) <= 1:
                            access_user_list = access_user_list[0]
                        else:
                            access_user_list = ','.join(access_user_list)
                        for user in self.get_all_users(UserManager):
                            db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                               sql_stmt.DataBaseSQL.
                                                               update_value(
                                                                   sql_stmt.DataBaseSQL, table=st.TABLE_DATASET,
                                                                   column=st.TB_DATASET_COL_ACCESS_USER_LIST,
                                                                   value=access_user_list,
                                                                   condition=st.TB_DATASET_COL_OWNER,
                                                                   condition_value=user.get_userID()),
                                                               local=False)
                        db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                           sql_stmt.DataBaseSQL.
                                                           update_value(
                                                               sql_stmt.DataBaseSQL, table=st.TABLE_DATASET,
                                                               column=st.TB_DATASET_COL_OWNER,
                                                               value=new_owner,
                                                               condition=st.TB_DATASET_COL_OWNER,
                                                               condition_value=user_to_delete),
                                                           local=False)
                    else:
                        access_user_list.remove(user_to_delete)
                        db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                           sql_stmt.DataBaseSQL.
                                                           update_value(
                                                               sql_stmt.DataBaseSQL, table=st.TABLE_DATASET,
                                                               column=st.TB_DATASET_COL_ACCESS_USER_LIST,
                                                               value=access_user_list,
                                                               condition=st.TB_DATASET_COL_DATASET_ID,
                                                               condition_value=dataset.get_datasetID()),
                                                           local=False)
                # Delete User from users
                db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                   sql_stmt.DataBaseSQL.
                                                   delete_row_from_table(
                                                       sql_stmt.DataBaseSQL, table=st.TABLE_USER,
                                                       condition=st.TB_USER_COL_USER_ID,
                                                       condition_value=user_to_delete),
                                                   local=False)
                return True
            except:
                return True
                print("User Deleletion Unsuccessful")
        else:
            return False

    def check_admin(self, user_id):
        user = self.get_user_by_id(UserManager, user_id=user_id)
        if user.enum_admin_bool(user.get_admin()):
            return True
        else:
            return False

    def insert_department_db(self, department: str, user_id):
        if self.check_admin(UserManager, user_id):
            local = False
            dep_id = "dep_" + st.create_id()
            # Creates Table
            db_utils.DataBaseUtils.execute_sql(
                db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.create_department_table_sql(sql_stmt.DataBaseSQL), local=False)
            # Inserts data
            db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                               sql_statement=sql_stmt.DataBaseSQL.
                                               insert_department_values(
                                                   sql_stmt.DataBaseSQL, departmentID=dep_id, department_name=department), local=local)
            return True
        else:
            return False

    def get_departments_frontend(self):
        data = []
        db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.create_department_table_sql(sql_stmt.DataBaseSQL), local=False)
        result = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                    sql_stmt.DataBaseSQL.select_all_data_from_table(
                                                        sql_stmt.DataBaseSQL, table=st.TABLE_DEPARTMENTS),
                                                    fetchall=True, local=False)
        if not st.DEPARTMENT_GENESIS in [item for sublist in result for item in sublist]:
            db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                               sql_statement=sql_stmt.DataBaseSQL.
                                               insert_department_values(
                                                   sql_stmt.DataBaseSQL, departmentID=(
                                                       "dep_" + st.create_id()),
                                                   department_name=st.DEPARTMENT_GENESIS), local=False)
        for row in result:
            if row[1] != st.DEPARTMENT_GENESIS:
                department = {'department_id': row[0], 'name': row[1]}
                data.append(department)
        return data

    def get_departments(self):
        data = []
        db_utils.DataBaseUtils.execute_sql(
            db_utils.DataBaseUtils, sql_statement=sql_stmt.DataBaseSQL.create_department_table_sql(sql_stmt.DataBaseSQL), local=False)
        result = db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                                    sql_stmt.DataBaseSQL.select_all_data_from_table(
                                                        sql_stmt.DataBaseSQL, table=st.TABLE_DEPARTMENTS),
                                                    fetchall=True, local=False)
        if not st.DEPARTMENT_GENESIS in [item for sublist in result for item in sublist]:
            db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                               sql_statement=sql_stmt.DataBaseSQL.
                                               insert_department_values(
                                                   sql_stmt.DataBaseSQL, departmentID=(
                                                       "dep_" + st.create_id()),
                                                   department_name=st.DEPARTMENT_GENESIS), local=False)
        for row in result:
            department = {'department_id': row[0], 'name': row[1]}
            data.append(department)
        return data

    def delete_department(self, user_issuer, dep_to_delete):
        if self.check_admin(UserManager, user_issuer):
            # Delete dep from departments
            db_utils.DataBaseUtils.execute_sql(db_utils.DataBaseUtils,
                                               sql_stmt.DataBaseSQL.
                                               delete_row_from_table(
                                                   sql_stmt.DataBaseSQL, table=st.TABLE_DEPARTMENTS,
                                                   condition=st.TB_DEPARTMENTS_COL_DEPARTMENT_ID,
                                                   condition_value=dep_to_delete),
                                               local=False)
            return True
        else:
            return False

    def __parse_user_obj(self, db_row):
        if db_row:
            user_id = db_row[0]
            first_name = db_row[1]
            last_name = db_row[2]
            email = db_row[3]
            password = db_row[4]
            business_unit = db_row[5]
            access_rights_pillars_dict = self.__parse_access_rigths_pillars(
                UserManager, db_row[6])
            admin = db_row[7]
            role_manager = db_row[8]
            user = us.User(userID=user_id, first_name=first_name, last_name=last_name, email=email,
                           password=password, business_unit=business_unit,
                           access_rights_pillars=access_rights_pillars_dict, admin=admin,
                           role_manager=role_manager)
            return user
        pass
