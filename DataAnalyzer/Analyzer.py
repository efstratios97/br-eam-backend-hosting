import pandas as pd
import math


class Analyzer:

    def __analyzer_selection_switch(self):
        self.__analyzer_selection_switch_ = {}
        self.__analyzer_selection_switch_ = {
            'bubble_plotter': self.__run_analyzer_bubble_plotter,
            'simple_statistics': self.__run_analyzer_simple_statistics
        }

    def run_analyzer(self, data, type='bubble_plotter'):
        self.__analyzer_selection_switch(Analyzer)
        data, params = self.__analyzer_selection_switch_[type](
            Analyzer, data)
        return data, params

    def __run_analyzer_bubble_plotter(self, data):
        data = self.__normalize_bc(Analyzer, data)
        params = self.define_params(Analyzer, data)
        data = self.__calculate_complexity(Analyzer, data)
        return data, params

    def __run_analyzer_simple_statistics(self, data):
        data = self.__normalize_bc(Analyzer, data)
        params = self.define_params(Analyzer, data)
        data = self.__calculate_complexity(Analyzer, data)
        return data, params

    def define_params(self, df):
        self.list_bc = []
        self.__get_list_bc(Analyzer, df)
        self.list_bc.insert(0, 'All')
        self.list_bu = self.__get_list_bu(Analyzer, df)
        self.sorting = ['Ascending', 'Descending']
        self.range = ['All', 3, 5, 10, 20]
        self.bubble_size = ['Anzahl Nutzer', 'Komplexität',
                            'Anzahl unterstützter Geschäftsfähigkeiten']
        self.bubble_y_axis = ['Anzahl Nutzer', 'Komplexität',
                              'Anzahl unterstützter Geschäftsfähigkeiten']
        params = {'list_bc': self.list_bc, 'list_bu': self.list_bu, 'sorting': self.sorting,
                  'range': self.range, 'bubble_size': self.bubble_size, 'bubble_y_axis': self.bubble_y_axis}
        return params

    def __get_list_bu(self, df):
        df['Verantwortliche Organisationseinheit'] = df['Verantwortliche Organisationseinheit'].apply(
            lambda x: 'NICHT EINGEPFLEGT' if not isinstance(x, str) else x)
        list_bu = df['Verantwortliche Organisationseinheit'].to_list()
        # list_bu = [val.replace(" (Organisationseinheit)", "")
        #            for val in list_bu]
        list_bu = list(set(list_bu))
        list_bu.insert(0, 'All')
        return list_bu

    def __get_list_bc(self, df):
        def split_bc(x):
            list_x = x.replace('\n', ' ').replace(
                ' (Geschäftsfähigkeit)', '').split(', ')
            list_x = [val.strip() for val in list_x]
            [self.list_bc.append(val)
             for val in list_x if not val in self.list_bc]
            return x.replace('\n', ' ').replace(' (Geschäftsfähigkeit)', '')
        df['Unterstützte Geschäftsfähigkeiten'] = df['Unterstützte Geschäftsfähigkeiten'].apply(lambda x: split_bc(x)
                                                                                                if isinstance(x, str) else None)

    def __normalize_bc(self, df):
        def split_bc(x):
            list_x = x.replace('\n', ' ').replace(
                ' (Geschäftsfähigkeit)', '').split(', ')
            list_x = [val.strip() for val in list_x]
            return x.replace('\n', ' ').replace(' (Geschäftsfähigkeit)', '')
        df['Unterstützte Geschäftsfähigkeiten'] = df['Unterstützte Geschäftsfähigkeiten'].apply(lambda x: split_bc(x)
                                                                                                if isinstance(x, str) else None)
        return df

    def __calculate_complexity(self, df):
        def calculate_complexity(x):
            if not pd.isnull(x['Bereitgestellte Schnittstellen']):
                complexity = x['Bereitgestellte Schnittstellen'].count(',') + 1
            else:
                complexity = 0
            if not pd.isnull(x['Genutzte Schnittstellen']):
                complexity += x['Genutzte Schnittstellen'].count(',') + 1
            return complexity

        def calculate_count_bc(x):
            count_bc = 0
            if not pd.isnull(x['Unterstützte Geschäftsfähigkeiten']):
                count_bc = x['Unterstützte Geschäftsfähigkeiten'].count(
                    ',') + 1
            return count_bc

        def find_domain(x):
            col_bc = ''
            for column in self.domain_df.columns:
                if column.startswith('Eingehende Beziehung'):
                    col_bc = column
                    break
            for bc in self.domain_df:
                if not pd.isnull(x['Unterstützte Geschäftsfähigkeiten']):
                    for x in x['Unterstützte Geschäftsfähigkeiten'].split(','):
                        if x in bc:
                            return self.domain_df.loc[self.domain_df['col_bc'] == bc, 'Name']
        df['Komplexität'] = df.apply(
            lambda x: calculate_complexity(x), axis=1)
        df['Anzahl unterstützter Geschäftsfähigkeiten'] = df.apply(
            lambda x: calculate_count_bc(x), axis=1)
        return df
