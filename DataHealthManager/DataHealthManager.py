# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Implements main functionality of the DataManager
'''

import Utils.DataBaseUtils as db_utils
import Utils.DataBaseSQL as sql_stmt
import Utils.Settings as st
import DataManager.DataManager as dm
import pandas as pd


class DataHealthManager:

    def treemap_ranking_by_applications(self, dataset_id):
        data_included = []
        data = []
        df = dm.DataManager.get_table_as_df(dm.DataManager, table=dataset_id)
        # df['Verantwortliche Organisationseinheit'] = df['Verantwortliche Organisationseinheit'].apply(
        #     lambda x: 'NICHT EINGEPFLEGT' if not isinstance(x, str) else x)
        df['Verantwortliche Organisationseinheit'] = df['Verantwortliche Organisationseinheit'].apply(
            lambda x: 'NICHT EINGEPFLEGT' if x == '' else x)  # Because change of how read datasets --> None replaced with ""
        df['Verantwortliche Organisationseinheit'] = df['Verantwortliche Organisationseinheit'].apply(
            lambda x: x.replace(" (Organisationseinheit)", ""))

        def get_dep_count_pairs(x):
            if not x['Verantwortliche Organisationseinheit'] in data_included:
                data.append(
                    {"x": x['Verantwortliche Organisationseinheit'], "y": 1})
                data_included.append(x['Verantwortliche Organisationseinheit'])
            else:
                for pair in data:
                    if pair['x'] == x['Verantwortliche Organisationseinheit']:
                        pair['y'] = pair['y'] + 1
        df.apply(lambda x: get_dep_count_pairs(x), axis=1)
        data = sorted(data, key=lambda i: i['y'], reverse=True)
        return data

    def get_applications(self, dataset_id):
        data = []
        df = dm.DataManager.get_table_as_df(dm.DataManager, table=dataset_id)
        data = df['Name'].to_list()
        return data

    def get_data_for_radar_description(self, dataset_id, app_name):
        data = []
        df = dm.DataManager.get_table_as_df(dm.DataManager, table=dataset_id)
        df = df[df['Name'] == app_name]

        def calculate_providing_interfaces(x):
            if not pd.isnull(x['Bereitgestellte Schnittstellen']):
                giving_interfaces = x['Bereitgestellte Schnittstellen'].count(
                    ',') + 1
            else:
                giving_interfaces = 0
            return giving_interfaces

        def calculate_taking_interfaces(x):
            if not pd.isnull(x['Genutzte Schnittstellen']):
                taking_interfaces = x['Genutzte Schnittstellen'].count(
                    ',') + 1
            else:
                taking_interfaces = 0
            return taking_interfaces

        def calculate_count_bc(x):
            count_bc = 0
            if not pd.isnull(x['Unterstützte Geschäftsfähigkeiten']):
                count_bc = x['Unterstützte Geschäftsfähigkeiten'].count(
                    ',') + 1
            return count_bc

        def calculate_business_obj(x):
            business_obj = 0
            if not pd.isnull(x['Referenzierte Geschäftsobjekte']):
                business_obj = x['Referenzierte Geschäftsobjekte'].count(
                    ',') + 1
            return business_obj

        def calculate_complexity(x):
            if not pd.isnull(x['Bereitgestellte Schnittstellen']):
                complexity = x['Bereitgestellte Schnittstellen'].count(',') + 1
            else:
                complexity = 0
            if not pd.isnull(x['Genutzte Schnittstellen']):
                complexity += x['Genutzte Schnittstellen'].count(',') + 1
            return complexity

        df['providing_interfaces'] = df.apply(
            lambda x: calculate_providing_interfaces(x), axis=1)
        df['taking_interfaces'] = df.apply(
            lambda x: calculate_taking_interfaces(x), axis=1)
        df['count_bc'] = df.apply(
            lambda x: calculate_count_bc(x), axis=1)
        df['business_obj'] = df.apply(
            lambda x: calculate_business_obj(x), axis=1)
        df['complexity'] = df.apply(
            lambda x: calculate_complexity(x), axis=1)
        data = [df['providing_interfaces'].iloc[0], df['taking_interfaces'].iloc[0],
                df['complexity'].iloc[0], df['count_bc'].iloc[0], df['business_obj'].iloc[0]]
        labels = ['Genutzte Schnittstellen', 'Bereitgestellte Schnittstellen',
                  'Komplexität', 'Geschäftsfähigkeiten', 'Geschäftsobjekte']
        return data, labels
