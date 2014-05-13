import ast
import os
from pylab import average, median, std
import pandas
import matplotlib.pyplot as plt
import time


class ScalingPlotter:

    def __init__(self, benchmark_group_id, **kwargs):
        self._legendTitle = kwargs['legendTitle'] if kwargs.has_key('legendTitle') else "rows"
        self._group_id = benchmark_group_id
        self._data = self._collect()
        self._df = pandas.DataFrame(self._data)
        self._df["rows"] = self._df['run'].map(lambda x: int(x.split("_")[1]))
        self._df["instances"] = self._df['run'].map(lambda x: int(x.split("_")[3]))

    def plot_total_response_time(self):
        selection_lambda = lambda x: x['op_name'] == "ResponseTask"
        y_label = "Median Respone Time in ms"
        field = 'end'
        file_infix = 'response'
        self._plot_row_functions(selection_lambda, field, y_label, file_infix)

    # selection_lambda takes a row in the dataframe and returns true for the
    # tasks of which the mean task size should be plotted.
    def plot_mean_task_size(self, selection_lambda):
        y_label = "Median Task Duration in ms"
        field = 'duration'
        file_infix = 'meantasksize'
        self._plot_row_functions(selection_lambda, field, y_label, file_infix)


    # Plots the dataframe with a data series per row size
    # x-axis is number of instances.
    # y-axis is determined by the aggregated field
    # selection_lambda: praedicate to filter rows of dataframe
    # field: the field to aggregate and plot
    # y_label: describe the aggregate of the field for the y-axis
    # file_infix: string used to generate the middle portion of the filename
    def _plot_row_functions(self, selection_lambda, field, y_label, file_infix):
        criterion = self._df.apply(selection_lambda, axis=1)
        tasks = self._df[criterion]
        group = tasks.groupby(["rows", "instances"]).median()

        group[field].unstack('rows').plot()
        plt.xlabel("Number of Instances")
        plt.ylabel(y_label)
        plt.yscale('log')
        plt.legend(title=self._legendTitle)
        fname = "%s_%s_%d.pdf" % (self._group_id, file_infix, int(time.time()))
        plt.savefig(fname)
        print ">>>%s" % fname

    # returns a list of dictionaries with the following keys
    # run, build, user, query_name, op_id, op_name, start, duration
    def _collect(self):
        data = list()
        dir_results = os.path.join(os.getcwd(), "results", self._group_id)
        if not os.path.isdir(dir_results):
            raise Exception(
                "Group result directory '%s' not found!" % dir_results)

        for str_run in os.listdir(dir_results):
            dir_run = os.path.join(dir_results, str_run)

            if not os.path.isdir(dir_run):
                continue

            for str_build in os.listdir(dir_run):
                dir_build = os.path.join(dir_run, str_build)
                if not os.path.isdir(dir_build):
                    continue

                for str_user in os.listdir(dir_build):
                    dir_user = os.path.join(dir_build, str_user)
                    if not os.path.isdir(dir_user):
                        continue

                    log_file_name = os.path.join(dir_user, "queries.log")
                    if not os.path.isfile(log_file_name):
                        print "WARNING: no queries log found in %s!" % dir_user
                        continue

                    for rawline in open(log_file_name):
                        linedata = rawline.split(";")
                        query_name = linedata[0]
                        perf_data = ast.literal_eval(linedata[1])

                        # add new item to main data list structure for every op
                        for op_data in perf_data:
                            dur  = op_data["endTime"] - op_data["startTime"]
                            data.append({
                                "run": str_run,
                                "build": str_build,
                                "user": str_user,
                                "query_name": query_name,
                                "op_id": op_data["id"],
                                "op_name": op_data["name"],
                                "start": op_data["startTime"],
                                "end": op_data["endTime"],
                                "duration": dur})

        return data
