import DataAnalyzer.Plotter as plt


class DataAnalyzer:

    def __create_execution_switch(self):
        execution_switch = {
            'bubble_plotter': plt.Plotter.run_bubble_plotter,
            'simple_statistics': plt.Plotter.run_statistical_analysis_plotter
        }
        return execution_switch

    def execute_plot(self, data, operation='', input_values='', type='bubble_plotter', plotter_id=''):
        execution_switch = self.__create_execution_switch(DataAnalyzer)
        if operation:
            result = operation[plotter_id]()
        else:
            result = execution_switch[type](
                plt.Plotter, data, input_values)
        return result
