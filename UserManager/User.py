# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Defines a User object
'''

import Utils.Settings as st
import datetime
import jwt


class User:

    def __init__(self, userID, first_name: str, last_name: str, email: str,
                 password: str, business_unit: str, access_rights_pillars: dict, admin, role_manager):
        self.__userID = userID
        self.__first_name = first_name
        self.__last_name = last_name
        self.__email = email
        self.__password = password
        self.__business_unit = business_unit
        self.__access_rights_pillars = access_rights_pillars
        self.__admin = self.enum_admin(admin)
        self.__role_manager = self.enum_role_manger(role_manager)

    # Enums
    def enum_admin(self, admin):
        enum_admin = {
            'admin': 1,
            'not_admin': 0,
            True: 1,
            False: 0,
            1: 1,
            0: 0,
            str(1): 1,
            str(0): 0
        }
        try:
            res = enum_admin[admin]
        except:
            res = 0
            print('Not Able to identify if User is admin\n' +
                  'Line: ' + str(st.get_linenumber_of_occured_error())
                  + 'File: ' + str(st.get_filename_of_occured_error()))
        return res

    def enum_admin_bool(self, admin):
        enum_admin = {
            'admin': True,
            'not_admin': False,
            'True': True,
            'False': False,
            1: True,
            0: False,
            '1': True,
            '0': False
        }
        try:
            res = enum_admin[admin]
        except:
            res = 0
            print('Not Able to identify if User is admin\n' +
                  'Line: ' + str(st.get_linenumber_of_occured_error())
                  + 'File: ' + str(st.get_filename_of_occured_error()))
        return res

    def enum_role_manger(self, role_manager):
        enum_role_manger = {
            'role_manager': 1,
            'not_role_manager': 0,
            True: 1,
            False: 0,
            1: 1,
            0: 0,
            str(1): 1,
            str(0): 0
        }
        try:
            res = enum_role_manger[role_manager]
        except:
            res = 0
            print('Not Able to identify if User is role manager\n' +
                  'Line: ' + str(st.get_linenumber_of_occured_error())
                  + 'File: ' + str(st.get_filename_of_occured_error()))
        return res

    def generate_token(self, user):
        payload = {
            'iid': user.get_userID(),
            'email': user.get_email(),
            'passwd': user.get_password(),
            'token_creation': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'duration': 6600
        }
        encoded_jwt = jwt.encode(payload, "lil'precious", algorithm='HS256')
        return encoded_jwt

    def decode_token(self, token):
        payload = jwt.decode(token, "lil'precious", algorithms=['HS256'])
        payload['expiration_date'] = datetime.datetime.strptime(payload['token_creation'], '%Y-%m-%d %H:%M:%S') + \
            datetime.timedelta(seconds=payload['duration'])
        return payload

    # Definition of get Methods for User Objects
    def get_userID(self):
        return self.__userID

    def get_first_name(self):
        return self.__first_name

    def get_last_name(self):
        return self.__last_name

    def get_full_name(self):
        return "{} {}".format(self.__first_name, self.__last_name)

    def get_email(self):
        return self.__email

    def get_password(self):
        return self.__password

    def get_access_rights_pillars(self):
        return self.__access_rights_pillars

    def get_business_unit(self):
        return self.__business_unit

    def get_admin(self):
        return self.__admin

    def get_role_manager(self):
        return self.__role_manager

    # Setter Methods for certain Attributes of User Objects
    def set_first_name(self, first_name: str):
        self.__first_name = first_name

    def set_last_name(self, last_name: str):
        self.__last_name = last_name

    def set_email(self, email: str):
        self.__email = email

    def set_password(self, password: str):
        self.__password = password

    def set_access_rights_pillars(self, access_rights_pillars: dict):
        self.__access_rights_pillars = access_rights_pillars

    def set_business_unit(self, business_unit: str):
        self.__business_unit = business_unit

    def set_admin(self, admin):
        self.__admin = self.enum_admin(admin)

    def set_role_manager(self, role_manager):
        self.__role_manager = self.enum_role_manger(role_manager)
