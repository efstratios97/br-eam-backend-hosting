import flask as fl
import pandas as pd
import DataManager.DataManager as dm
import ArchitectureViewManager.ArchitectureViewManager as avm
import ArchitectureViewManager.ArchitectureView as av
import Utils.Settings as st


class ArchitectureViewManagerEndpoints:

    def endpoints_exception(self, code, msg):
        fl.abort(fl.make_response(fl.jsonify(message=msg), code))

    def architecture_view_to_dict(self, architecture_view: av.ArchitectureView, data_path=''):
        dict_formatted = {}
        if architecture_view:
            dict_formatted['architecture_view_id'] = architecture_view.get_architecture_viewID(
            )
            dict_formatted['name'] = architecture_view.get_name()
            dict_formatted['description'] = architecture_view.get_description()
            dict_formatted['components'] = architecture_view.get_components()
        return dict_formatted


blueprint = fl.Blueprint('ArchitectureViewManager', __name__)


@blueprint.route('/create_architecture_view', methods=['POST', 'OPTIONS'])
def create_architecture_view():
    result = {}
    name = fl.request.form['name']
    description = fl.request.form['description']
    components = fl.request.form['components']
    architecture_view, check_worked = avm.ArchitectureViewManager.create_architecture_view(avm.ArchitectureViewManager, name=name,
                                                                                           description=description, components=components)
    result = ArchitectureViewManagerEndpoints.architecture_view_to_dict(
        ArchitectureViewManagerEndpoints, architecture_view=architecture_view)
    if not check_worked:
        ArchitectureViewManagerEndpoints.endpoints_exception(ArchitectureViewManagerEndpoints,
                                                             400, "ARCHITECTURE_VIEW_COULD_NOT_BE_CREATED")
    return fl.jsonify(result), 200


@blueprint.route('/get_architecture_views', methods=['GET', 'OPTIONS'])
def get_architecture_views():
    result = {}
    result['data'] = []
    architecture_views = avm.ArchitectureViewManager.get_all_architecture_views(
        avm.ArchitectureViewManager)
    for architecture_view in architecture_views:
        result['data'].append(ArchitectureViewManagerEndpoints.architecture_view_to_dict(
            ArchitectureViewManagerEndpoints, architecture_view=architecture_view))
    return fl.jsonify(result), 200
    if not result:
        ArchitectureViewManagerEndpoints.endpoints_exception(ArchitectureViewManagerEndpoints,
                                                             404, "ARCHITECTURE_VIEW_NOT_FOUND")
    return fl.jsonify(result), 200


@blueprint.route('/get_departments_from_dataset/<dataset_id>', methods=['GET', 'OPTIONS'])
def get_departments_from_dataset(dataset_id):
    result = {}
    result['data'] = []
    departments = avm.ArchitectureViewManager.get_departments(
        avm.ArchitectureViewManager, dataset_id)
    for department in departments:
        result['data'].append(department)
    return fl.jsonify(result), 200
    if not result:
        ArchitectureViewManagerEndpoints.endpoints_exception(ArchitectureViewManagerEndpoints,
                                                             404, "DEPARTMENTS_NOT_FOUND")
    return fl.jsonify(result), 200


@blueprint.route('/get_components/<dataset_id>', methods=['GET', 'OPTIONS'])
def get_components(dataset_id):
    result = {}
    result['data'] = []
    departments = avm.ArchitectureViewManager.get_components(
        avm.ArchitectureViewManager, dataset_id)
    for department in departments:
        result['data'].append(department)
    return fl.jsonify(result), 200
    if not result:
        ArchitectureViewManagerEndpoints.endpoints_exception(ArchitectureViewManagerEndpoints,
                                                             404, "DEPARTMENTS_NOT_FOUND")
    return fl.jsonify(result), 200


@blueprint.route('/delete_architecture_view/<architecture_view_id>', methods=['DELETE', 'OPTIONS'])
def delete_architecture_view(architecture_view_id):
    result = {}
    avm.ArchitectureViewManager.delete_architecture_view(
        avm.ArchitectureViewManager, architecture_view_id)
    return fl.jsonify(result), 200


@blueprint.route('/analyze_applicability/<dataset_id>/<architecture_view_id>/<department>', methods=['POST', 'OPTIONS'])
def analyze_applicability(dataset_id, architecture_view_id, department):
    result = {}
    architecture_view = avm.ArchitectureViewManager.get_architecture_view_by_id(avm.ArchitectureViewManager,
                                                                                architecture_view_id)
    result = avm.ArchitectureViewManager.analyze_applicability(
        avm.ArchitectureViewManager, dataset_id=dataset_id, department=department, architecture_view=architecture_view)
    if not result:
        ArchitectureViewManagerEndpoints.endpoints_exception(ArchitectureViewManagerEndpoints,
                                                             400, "ARCHITECTURE_VIEW_COULD_NOT_BE_ANALYZED")
    return fl.jsonify(result), 200
