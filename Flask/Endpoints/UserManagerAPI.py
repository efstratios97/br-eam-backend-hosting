# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Endpoints for the UserManager
'''

import flask as fl
import UserManager.UserManager as um
import UserManager.User as us
import Utils.Settings as st
import json
import datetime


class UserManagerEndpoints:

    def endpoints_exception(self, code, msg):
        fl.abort(fl.make_response(fl.jsonify(message=msg), code))

    def user_to_dict(self, user: us.User):
        dict_formatted = {}
        if user:
            dict_formatted['user_id'] = user.get_userID()
            dict_formatted['first_name'] = user.get_first_name()
            dict_formatted['last_name'] = user.get_last_name()
            dict_formatted['admin'] = str(
                user.get_admin())
            dict_formatted['business_unit'] = user.get_business_unit()
            dict_formatted['email'] = user.get_email()
            dict_formatted['password'] = "TOP SECRET"
            dict_formatted['access_rights_pillars'] = json.dumps(
                user.get_access_rights_pillars())
            dict_formatted['role_manager'] = user.get_role_manager()
        return dict_formatted


blueprint = fl.Blueprint('UserManager', __name__)


@blueprint.route('/create_user/<user_id>', methods=['POST', 'OPTIONS'])
def create_user(user_id):
    result = {}
    body = fl.request.get_json(force=True)
    first_name = body['first_name']
    last_name = body['last_name']
    admin = body['admin']
    email = body['email']
    business_unit = body['business_unit']
    password = body['password']
    access_rights_pillars = body['access_rights_pillars']
    role_manager = body['role_manager']
    user = um.UserManager.create_user(um.UserManager, first_name=first_name, last_name=last_name, email=email,
                                      password=password, business_unit=business_unit, admin=admin, role_manager=role_manager,
                                      access_rights_pillars=access_rights_pillars, operation_issuer=user_id)
    if user:
        result = UserManagerEndpoints.user_to_dict(
            UserManagerEndpoints, user=user)
        return fl.jsonify(result), 200
    else:
        UserManagerEndpoints.endpoints_exception(UserManagerEndpoints,
                                                 403, 'Wrong Password: Access Denied')


@blueprint.route('/change_password/<user_id>', methods=['POST', 'OPTIONS'])
def change_user_password(user_id):
    result = {}
    body = fl.request.get_json(force=True)
    new_password = body['new_password']
    old_password = body['old_password']
    check = um.UserManager.check_password(
        um.UserManager, password_user=old_password, user_id=user_id)
    if check:
        um.UserManager.update_password(
            um.UserManager, new_password=new_password, user_id=user_id)
    else:
        UserManagerEndpoints.endpoints_exception(UserManagerEndpoints,
                                                 403, 'No admin rights')
    return fl.jsonify(result), 200


@blueprint.route('/users', methods=['GET', 'OPTIONS'])
def get_users():
    users = um.UserManager.get_all_users(um.UserManager)
    result = []
    for user in users:
        result.append(UserManagerEndpoints.user_to_dict(
            UserManagerEndpoints, user=user))
    return fl.jsonify(result), 200


@blueprint.route('/user/<user_id>', methods=['GET', 'OPTIONS'])
def get_user(user_id):
    result = {}
    user = um.UserManager.get_user_by_id(um.UserManager, user_id)
    if user:
        result = UserManagerEndpoints.user_to_dict(
            UserManagerEndpoints, user=user)
    return fl.jsonify(result), 200


@blueprint.route('/update_department/<department>/<user_id>', methods=['POST', 'OPTIONS'])
def update_department(department, user_id):
    result = {}
    ckeck_rights = um.UserManager.update_department(
        um.UserManager, department=department, user_id=user_id)
    if ckeck_rights:
        return fl.jsonify(result), 200
    else:
        UserManagerEndpoints.endpoints_exception(UserManagerEndpoints,
                                                 403, 'No rights')


@blueprint.route('/delete_user/<to_delete_user_id>/<user_issuer>', methods=['DELETE', 'OPTIONS'])
def delete_user(to_delete_user_id, user_issuer):
    result = {}
    ckeck_rights = False
    user = um.UserManager.get_user_by_id(
        um.UserManager, user_id=to_delete_user_id)
    try:
        ckeck_rights = um.UserManager.delete_user(
            um.UserManager, user_issuer=user_issuer,
            user_to_delete=to_delete_user_id)
    except:
        print('Deleting Dataset from Dataset table unsuccessful')
        ckeck_rights = True
    if ckeck_rights:
        return fl.jsonify(result), 200
    else:
        UserManagerEndpoints.endpoints_exception(UserManagerEndpoints,
                                                 403, 'No rights')


@blueprint.route('/departments', methods=['GET', 'OPTIONS'])
def get_departments():
    departments = um.UserManager.get_departments(um.UserManager)
    return fl.jsonify(departments), 200


@blueprint.route('/create_department/<user_id>', methods=['POST', 'OPTIONS'])
def post_department(user_id):
    body = fl.request.get_json(force=True)
    dep_name = body['dep_name']
    admin_check = um.UserManager.insert_department_db(
        um.UserManager, dep_name, user_id)
    result = body
    if admin_check:
        return fl.jsonify(result), 200
    else:
        UserManagerEndpoints.endpoints_exception(UserManagerEndpoints,
                                                 403, 'No admin rights')


@blueprint.route('/delete_department/<dep_to_delete>/<user_issuer>', methods=['DELETE', 'OPTIONS'])
def delete_department(dep_to_delete, user_issuer):
    result = {}
    try:
        ckeck_rights = um.UserManager.delete_department(
            um.UserManager, user_issuer=user_issuer,
            dep_to_delete=dep_to_delete)
    except:
        print('Deleting Department from Departments table unsuccessful')
    if ckeck_rights:
        return fl.jsonify(result), 200
    else:
        UserManagerEndpoints.endpoints_exception(UserManagerEndpoints,
                                                 403, 'No rights')


@blueprint.route('/user/auth', methods=['GET', 'OPTIONS'])
def auth_user():
    email = fl.request.args.get('email', None)
    passwd = fl.request.args.get('passwd', None)

    result = {}
    if email:
        user = um.UserManager.get_user_by_email(um.UserManager, email)
    else:
        UserManagerEndpoints.endpoints_exception(400, "EMAIL_PARAM_NOT_FOUND")

    if passwd and st.create_hash_password_sha512(passwd, user.get_userID()) == user.get_password():
        token = us.User.generate_token(us.User, user)
        result = {
            "token": token.decode('utf-8'),
            "duration": 6600
        }
    else:
        UserManagerEndpoints.endpoints_exception(UserManagerEndpoints,
                                                 400, "BAD_OR_MISSING_PASSWORD")

    return fl.jsonify(result), 200


@blueprint.route('/user/validatetoken', methods=['GET', 'OPTIONS'])
def validade_jwt():
    token = fl.request.args.get('token', None)

    result = {}
    decoded_payload = None
    if token:
        decoded_payload = us.User.decode_token(us.User, token)
    else:
        UserManagerEndpoints.endpoints_exception(400, "MISSING_TOKEN")

    if decoded_payload['expiration_date'] < datetime.datetime.now():
        UserManagerEndpoints.endpoints_exception(
            UserManagerEndpoints, 401, "TOKEN_EXPIRED")
    else:
        return '', 200
