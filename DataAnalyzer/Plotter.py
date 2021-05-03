from bokeh.plotting import figure, output_file, show, ColumnDataSource, curdoc
from bokeh.embed import components
from bokeh.io import curdoc
from bokeh.models import Div, RangeSlider, Spinner
from bokeh.models import CustomJS, MultiChoice
from bokeh.layouts import column, widgetbox
import bokeh
import DataAnalyzer.Analyzer as bra
import pandas as pd
import numpy as np


class Plotter:

    def __init__(self, plotter_id, name, description, params, operation=''):
        self.__plotter_id = plotter_id
        self.__name = name
        self.__description = description
        self.__params = params
        self.__operation = operation

    # Definition of get Methods for Plotter Objects

    def get_plotterID(self):
        return self.__plotter_id

    def get_name(self):
        return self.__name

    def get_description(self):
        return self.__description

    def get_params(self):
        return self.__params

    def get_operation(self):
        return self.__operation

    # Setter Methods for certain Attributes of Plotter Objects
    def set_plotter_id(self, plotter_id: str):
        self.__plotter_id = plotter_id

    def set_name(self, name: str):
        self.__name = name

    def set_description(self, description: str):
        self.__description = description

    def set_params(self, params: dict):
        self.__params = params

    def set_operation(self, operation: dict):
        self.__operation = operation

        # Bubble Plot

    def init_params(self):
        self.resposible_unit = 'responsible_unit'
        self.supported_bc = 'supported_bc'
        self.bubble_y_axis = 'bubble_y_axis'
        self.bubble_size_axis = 'bubble_size_axis'
        self.sorting = 'sorting'
        self.top = 'top'

    def run_bubble_plotter(self, data, input_values='', type='bubble_plotter'):
        self.init_params(Plotter)
        data, params = bra.Analyzer.run_analyzer(
            bra.Analyzer, data=data, type=type)
        result = self.__plot_bubble_plot(Plotter, data, params, input_values)
        return result

    def run_statistical_analysis_plotter(self, data, input_values='', type='simple_statistics'):
        self.init_params(Plotter)
        data, params = bra.Analyzer.run_analyzer(
            bra.Analyzer, data=data, type=type)
        result = self.__plot_simple_statistics(
            Plotter, data, params, input_values)
        return result

    def __plot_bubble_plot(self, data, params, input_values):
        output_file("bubble_plot.html")
        curdoc().theme = 'dark_minimal'
        TOOLS = "hover,crosshair,pan,wheel_zoom,zoom_in,zoom_out,box_zoom,undo,tap,box_select,poly_select,reset,save"
        if input_values:
            df = data
        else:
            df = data[0:0]
            p = figure(x_range=df['Name'],  plot_height=450, title="Heatmap",
                       tools=TOOLS, toolbar_location="below")
        if input_values:
            if not "All" in input_values[self.resposible_unit]:
                if len(input_values[self.resposible_unit]) <= 1:
                    df = df[df['Verantwortliche Organisationseinheit']
                            == input_values[self.resposible_unit][0]]
                else:
                    def remove_bu(x):
                        for input_ in input_values[self.resposible_unit]:
                            if not input_ in x and not x in input_values[self.resposible_unit]:
                                return 'REMOVE'
                            else:
                                return x
                    df['Verantwortliche Organisationseinheit'] = df['Verantwortliche Organisationseinheit'].apply(
                        lambda x: remove_bu(x))
                    df = df[df['Verantwortliche Organisationseinheit'] != 'REMOVE']
            if not "All" in input_values[self.supported_bc]:

                def remove_bc(x):
                    for input_ in input_values[self.supported_bc]:
                        if not input_ in x and not x in input_values[self.supported_bc]:
                            return 'REMOVE'
                        else:
                            return x
                df = df[df['Unterstützte Geschäftsfähigkeiten'].notna()]
                df['Unterstützte Geschäftsfähigkeiten'] = df['Unterstützte Geschäftsfähigkeiten'].apply(
                    lambda x: remove_bc(x))
                df = df[df['Unterstützte Geschäftsfähigkeiten'] != 'REMOVE']
            # Sort after bubble size
            if input_values[self.sorting] == 'Ascending':
                df.sort_values(
                    by=input_values[self.bubble_size_axis], inplace=True)
            else:
                df.sort_values(
                    by=input_values[self.bubble_size_axis], inplace=True, ascending=False)
            if not input_values[self.top] == 'All':
                df = df.head(int(input_values[self.top]))
            colors = np.array([[r, g, 150]
                               for r, g in zip(50 + 2 * df[input_values[self.bubble_size_axis]], 30 + 2*df[input_values[self.bubble_y_axis]])], dtype="uint8")
            p = figure(x_range=df['Name'],  plot_height=500, title="Heatmap Analysis",
                       tools=TOOLS, toolbar_location="below", y_axis_label=input_values[self.bubble_y_axis],
                       x_axis_label=input_values[self.bubble_size_axis], x_minor_ticks=-10)
            if input_values[self.bubble_size_axis] == 'Anzahl Nutzer':
                df[input_values[self.bubble_size_axis]] = df[input_values[self.bubble_size_axis]].apply(
                    lambda x: x+500 if x <= 500 and x > 0 else x)
                df_size = df[input_values[self.bubble_size_axis]]/50
            elif input_values[self.bubble_size_axis] == 'Anzahl unterstützter Geschäftsfähigkeiten':
                df_size = df[input_values[self.bubble_size_axis]]*4
            else:
                df_size = df[input_values[self.bubble_size_axis]]*4
            p.scatter(x=df['Name'], y=df[input_values[self.bubble_y_axis]], fill_alpha=0.6, size=df_size, fill_color=colors,
                      line_color=None)
            layout = column(p, sizing_mode="stretch_both")
            script, div = components(
                layout, wrap_script=False, theme='dark_minimal')
            return {'div': div, 'script': script}
        layout = column(p, sizing_mode="stretch_both")
        script, div = components(p, wrap_script=False, theme='dark_minimal')
        return {'div': div, 'script': script}

    def __plot_simple_statistics(self, data, params, input_values):
        output_file("simple_statistics_plot.html")
        curdoc().theme = 'dark_minimal'
        TOOLS = "hover,crosshair,pan,wheel_zoom,zoom_in,zoom_out,box_zoom,undo,tap,box_select,poly_select,reset,save"
        if input_values:
            df = data
        else:
            df = data[0:0]
            p = figure(x_range=df['Name'],  plot_height=450, title="Statistics",
                       tools=TOOLS, toolbar_location="below")
        if input_values:
            if not "All" in input_values[self.resposible_unit]:
                if len(input_values[self.resposible_unit]) <= 1:
                    df = df[df['Verantwortliche Organisationseinheit']
                            == input_values[self.resposible_unit][0]]
                else:
                    def remove_bu(x):
                        for input_ in input_values[self.resposible_unit]:
                            if not input_ in x and not x in input_values[self.resposible_unit]:
                                return 'REMOVE'
                            else:
                                return x
                    df['Verantwortliche Organisationseinheit'] = df['Verantwortliche Organisationseinheit'].apply(
                        lambda x: remove_bu(x))
                    df = df[df['Verantwortliche Organisationseinheit'] != 'REMOVE']
            if not "All" in input_values[self.supported_bc]:

                def remove_bc(x):
                    for input_ in input_values[self.supported_bc]:
                        if not input_ in x and not x in input_values[self.supported_bc]:
                            return 'REMOVE'
                        else:
                            return x
                df = df[df['Unterstützte Geschäftsfähigkeiten'].notna()]
                df['Unterstützte Geschäftsfähigkeiten'] = df['Unterstützte Geschäftsfähigkeiten'].apply(
                    lambda x: remove_bc(x))
                df = df[df['Unterstützte Geschäftsfähigkeiten'] != 'REMOVE']
            # Sort after bubble size
            if input_values[self.sorting] == 'Ascending':
                df.sort_values(
                    by=input_values[self.bubble_y_axis], inplace=True)
            else:
                df.sort_values(
                    by=input_values[self.bubble_y_axis], inplace=True, ascending=False)
            if not input_values[self.top] == 'All':
                df = df.head(int(input_values[self.top]))
            colors = np.array([[r, g, 150]
                               for r, g in zip(50 + 2 * df[input_values[self.bubble_y_axis]], 30 + 2*df[input_values[self.bubble_y_axis]])], dtype="uint8")
            p = figure(x_range=df['Name'],  plot_height=500, title="Heatmap Analysis",
                       tools=TOOLS, toolbar_location="below", y_axis_label=input_values[self.bubble_y_axis],
                       x_minor_ticks=-10)
            p.circle(x=df['Name'], y=df[input_values[self.bubble_y_axis]], fill_alpha=0.6, size=15,
                     line_color=None)
            layout = column(p, sizing_mode="stretch_width")
            script, div = components(
                layout, wrap_script=False, theme='dark_minimal')
            return {'div': div, 'script': script}
        layout = column(p, sizing_mode="stretch_width")
        script, div = components(p, wrap_script=False, theme='dark_minimal')
        return {'div': div, 'script': script}
