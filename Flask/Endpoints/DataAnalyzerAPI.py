# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Endpoints for DataAnalyzer
'''

import flask as fl
import pandas as pd
import DataAnalyzer.Plotter as plt
import DataAnalyzer.DataAnalyzer as da
import DataAnalyzer.Analyzer as bra
import DataManager.DataManager as dm
import Utils.Settings as st


class DataAnalyzerEndpoints:

    def endpoints_exception(self, code, msg):
        fl.abort(fl.make_response(fl.jsonify(message=msg), code))


blueprint = fl.Blueprint('DataAnalyzer', __name__)


@blueprint.route('/show_plotter/<dataset_id>/<plotter_type>', methods=['GET', 'OPTIONS'])
def show_graph(dataset_id, plotter_type):
    result = {}
    data = dm.DataManager.get_table_as_df(dm.DataManager, table=dataset_id)
    result = da.DataAnalyzer.execute_plot(
        da.DataAnalyzer, type=plotter_type, data=data)
    return fl.jsonify(result), 200


@blueprint.route('/post_inputs/<dataset_id>/<plotter_type>', methods=['POST', 'OPTIONS'])
def post_inputs(dataset_id, plotter_type):
    data = dm.DataManager.get_table_as_df(dm.DataManager, table=dataset_id)
    input_values = {}
    body = fl.request.get_json(force=True)
    plt.Plotter.init_params(plt.Plotter)
    input_values.update(
        {plt.Plotter.resposible_unit: body[plt.Plotter.resposible_unit]})
    input_values.update(
        {plt.Plotter.supported_bc: body[plt.Plotter.supported_bc]})
    print(plotter_type)
    if plotter_type == "bubble_plotter":
        input_values.update(
            {plt.Plotter.bubble_size_axis: body[plt.Plotter.bubble_size_axis]})
    input_values.update(
        {plt.Plotter.bubble_y_axis: body[plt.Plotter.bubble_y_axis]})
    input_values.update({plt.Plotter.sorting: body[plt.Plotter.sorting]})
    input_values.update({plt.Plotter.top: body[plt.Plotter.top]})
    result = da.DataAnalyzer.execute_plot(
        da.DataAnalyzer, data=data, input_values=input_values, type=plotter_type)
    return fl.jsonify(result), 200


@blueprint.route('/get_params/<dataset_id>', methods=['GET', 'OPTIONS'])
def get_params(dataset_id):
    result = {}
    data = dm.DataManager.get_table_as_df(dm.DataManager, table=dataset_id)
    result = bra.Analyzer.define_params(bra.Analyzer, data)
    return fl.jsonify(result), 200
