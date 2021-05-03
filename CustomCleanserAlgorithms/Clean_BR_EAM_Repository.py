# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Implementation of the BR EAM Repository
'''

import pandas as pd


class BRCleanserAlgorithms:

    def remove_dummy_appications(self, df, df_out):
        TO_REMOVE_LABEL = 'TO_REMOVE'
        self.df_out = df_out

        def label_dummy_applications(x):
            self.df_out = self.df_out.append(x)
            x['Name'] = TO_REMOVE_LABEL
            return x
        init_df = df.apply(lambda x: label_dummy_applications(x)
                           if x['Name'].lower().strip().startswith('anwend')
                           else x, axis=1)
        init_df = init_df.drop(
            init_df[init_df['Name'] == TO_REMOVE_LABEL].index)
        return init_df, self.df_out

    def remove_duplicate_appications(self, df, df_out):
        TO_REMOVE_LABEL = 'TO_REMOVE'
        self.x_duplicates = []
        self.df_out = df_out

        def label_duplicate_applications(x, df):
            dup_candidate = x['Name'].replace(" ", "").lower()
            if dup_candidate in self.x_duplicates:
                app_current_string = ""
                app_current = df.iloc[self.x_duplicates.index(dup_candidate)]
                for val in app_current.values:
                    if not val == TO_REMOVE_LABEL or val == x['Name']:
                        app_current_string += str(val)

                app_x_string = ""
                for val in x.values:
                    if not val == TO_REMOVE_LABEL or val == x['Name']:
                        app_x_string += str(val)
                if len(app_x_string) < len(app_current_string):
                    self.df_out = self.df_out.append(df.iloc[self.x_duplicates.index(
                        dup_candidate)])
                else:
                    self.df_out = self.df_out.append(x)
            else:
                self.x_duplicates.append(dup_candidate)
            return x
        init_df = df.apply(
            lambda x: label_duplicate_applications(x, df), axis=1)
        common = init_df.merge(self.df_out, on=['Name'])
        init_df = init_df[(~init_df.Name.isin(common.Name))]
        return init_df, self.df_out

    def remove_test_appications(self, df, df_out):
        TO_REMOVE_LABEL = 'TO_REMOVE'
        self.df_out = df_out

        def label_test_applications(x):
            if "test" in x['Name']:
                self.df_out = self.df_out.append(x)
                x['Name'] = TO_REMOVE_LABEL
            return x
        init_df = df.apply(lambda x: label_test_applications(x), axis=1)
        init_df = init_df.drop(
            init_df[init_df['Name'] == TO_REMOVE_LABEL].index)
        return init_df, self.df_out
