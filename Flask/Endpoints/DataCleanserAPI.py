# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Endpoints for DataManager
'''

import flask as fl
import pandas as pd
import DataManager.DataManager as dm
import DataManager.DataSet as ds
import UserManager.UserManager as um
import DataCleanser.Cleanser as cl
import DataCleanser.DataCleanser as dc
import Utils.Settings as st
from collections import OrderedDict
import json


class DataCleanserEndpoints:

    def endpoints_exception(self, code, msg):
        fl.abort(fl.make_response(fl.jsonify(message=msg), code))

    def cleanser_to_dict(self, cleanser: cl.Cleanser, data_path=''):
        dict_formatted = {}
        if cleanser:
            dict_formatted['cleanser_id'] = cleanser.get_cleanserID()
            dict_formatted['name'] = cleanser.get_name()
            dict_formatted['description'] = cleanser.get_description()
            dict_formatted['header_list'] = cleanser.get_header_list()
            dict_formatted['datasets'] = cleanser.get_datasets()
            dict_formatted['cleanser_operation_types'] = cleanser.get_cleanser_operation_types(
            )
        return dict_formatted


blueprint = fl.Blueprint('DataCleanser', __name__)


@blueprint.route('/create_cleanser', methods=['POST', 'OPTIONS'])
def post_cleanser():
    result = {}
    name = fl.request.form['name']
    description = fl.request.form['description']
    datasets = fl.request.form['datasets']
    cleanser_operation_types = fl.request.form['cleanser_operation_types']
    try:
        cleanser, check = dc.DataCleanser.create_cleanser(
            dc.DataCleanser, name=name, description=description, datasets=datasets,
            cleanser_operation_types=cleanser_operation_types)
        result = DataCleanserEndpoints.cleanser_to_dict(
            DataCleanserEndpoints, cleanser=cleanser)
        if check:
            return fl.jsonify(result), 200
        else:
            DataCleanserEndpoints.endpoints_exception(DataCleanserEndpoints,
                                                      code=400, msg="Cleanser Creation Unsuccesful")
    except:
        DataCleanserEndpoints.endpoints_exception(DataCleanserEndpoints,
                                                  code=400, msg="Cleanser Creation Unsuccesful")


@blueprint.route('/update_cleansers/<user_id>', methods=['POST', 'OPTIONS'])
def update_cleanser(user_id):
    result = {}
    dc.DataCleanser.update_all_cleansers(dc.DataCleanser, user_id)
    return fl.jsonify(result), 200


@blueprint.route('/delete_cleanser/<cleanser_id>', methods=['DELETE', 'OPTIONS'])
def delete_cleanser(cleanser_id):
    result = {}
    cleanser = dc.DataCleanser.get_cleanser_by_id(
        dc.DataCleanser, cleanser_id=cleanser_id)
    try:
        dc.DataCleanser.delete_cleanser(
            dc.DataCleanser, cleanser_id=cleanser_id,)
    except:
        DataCleanserEndpoints.endpoints_exception(
            code=400, msg="Deleting Dataset from Cleanser table unsuccessful")
    return fl.jsonify(result), 200


@blueprint.route('/create_cleaned_dataset/<cleanser_id>/<dataset_id>/<uid>', methods=['POST', 'OPTIONS'])
def post_cleaned_dataset(cleanser_id, dataset_id, uid):
    result = {}
    cleanser_operation_types = fl.request.form['cleanser_operation_types']
    data, df_out = dc.DataCleanser.apply_cleanser(
        dc.DataCleanser, cleanser_id=cleanser_id, dataset_id=dataset_id,
        cleanser_operation_types=cleanser_operation_types.split(','))
    name = fl.request.form['name']
    cleaned = 1
    access_user_list = fl.request.form['access_user_list']
    access_business_unit_list = fl.request.form['access_business_unit_list']
    description = fl.request.form['description']
    storage_type = fl.request.form['storage_type']
    owner = uid
    # Adds to the user_access_list the owner/creator of the dataset by default
    access_user_list_ids = ""
    if access_user_list == "":
        access_user_list = owner
    else:
        for user_mail in access_user_list.split(','):
            user = um.UserManager.get_user_by_email(um.UserManager, user_mail)
            access_user_list_ids += user.get_userID() + ","
        access_user_list = access_user_list_ids[:-1]
    if not owner in access_user_list.split(','):
        access_user_list += "," + owner
    dataset = dm.DataManager.create_data_set(dm.DataManager, name=name, owner=owner, data=data, cleaned=cleaned,
                                             access_user_list=access_user_list, access_business_unit_list=access_business_unit_list,
                                             description=description, storage_type=storage_type)

    df_out = df_out.astype(str)
    df_out_dict = df_out.to_dict(orient='records')
    result = json.dumps(df_out_dict, sort_keys=False)
    if result == '[]':
        result = json.dumps([{"Identified Rows": "NONE"}])
    return fl.jsonify(result), 200


@blueprint.route('/get_cleansers', methods=['GET', 'OPTIONS'])
def get_cleaners():
    result = {}
    result['data'] = []
    cleansers = dc.DataCleanser.get_all_cleansers(
        dc.DataCleanser)
    for cleanser in cleansers:
        result['data'].append(DataCleanserEndpoints.cleanser_to_dict(
            DataCleanserEndpoints, cleanser=cleanser))
    return fl.jsonify(result), 200


@blueprint.route('/get_suitable_cleansers/<dataset_id>', methods=['GET', 'OPTIONS'])
def get_suitable_cleaners(dataset_id):
    result = {}
    result['data'] = []
    cleansers = dc.DataCleanser.get_all_suitable_cleansers_by_dataset(
        dc.DataCleanser, dataset_id)
    for cleanser in cleansers:
        result['data'].append(DataCleanserEndpoints.cleanser_to_dict(
            DataCleanserEndpoints, cleanser=cleanser))
    return fl.jsonify(result), 200


@blueprint.route('/get_cleanser_by_id/<cleanser_id>', methods=['GET', 'OPTIONS'])
def get_cleanser_by_id(cleanser_id):
    result = {}
    result['data'] = []
    cleanser = dc.DataCleanser.get_cleanser_by_id(
        dc.DataCleanser, cleanser_id=cleanser_id)
    result['data'].append(DataCleanserEndpoints.cleanser_to_dict(
        DataCleanserEndpoints, cleanser=cleanser))
    return fl.jsonify(result), 200


@blueprint.route('/get_cleanser_operation_types', methods=['GET', 'OPTIONS'])
def get_operations():
    result = {}
    result['data'] = []
    cleanser_operation_types = dc.DataCleanser.get_all_cleanser_operation_types(
        dc.DataCleanser)
    result['data'].append(cleanser_operation_types)
    return fl.jsonify(result), 200
