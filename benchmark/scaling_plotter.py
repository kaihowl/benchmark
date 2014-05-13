import ast
import os
from pylab import average, median, std
import pandas
import matplotlib.pyplot as plt
import time
from scipy.optimize import curve_fit
import numpy as np


class ScalingPlotter:

    """
    Collection of routines to generate scaling-curve related diagrams.

    These are specifically the Figures 2, 3, and 6 (each (a) and (b)).
    The data is generated with benchmarks located in exp_mixed.py
    """

    def __init__(self, benchmark_group_id, **kwargs):
        self._legend_title = kwargs['legendTitle']  \
            if kwargs.has_key('legendTitle') else "rows"
        self._group_id = benchmark_group_id
        self._data = self._collect()
        self._df = pandas.DataFrame(self._data)
        # run are strings like "rows_<numberofrows>_instances_<numinstances>"
        # expand column run in separate columns
        self._df["rows"], self._df["instances"] = zip(* \
                self._df['run'].map( \
                    lambda x: [int(s) for s in x.split("_")[1:4:2] ]))

    def plot_fitting_for(self, eval_selection_lambda, rows=None):
        if rows==None:
            print "No rows specified for fitting. Assuming biggest size."
            rows=self._df['rows'].max()

        def fit_func(x, a, b):
            return np.divide(a, x) + b

        def filter(x):
            return x["rows"] == rows and eval_selection_lambda(x)

        criterion = self._df.apply(filter, axis=1)
        cur_data = self._df[criterion]
        group = cur_data.groupby("instances").median()

        x = np.array(group.index)
        y = np.array(group['duration'])
        # Get the distance of a value to it predecessor
        # This will smooth out greater distances between higher number of
        # instances. A bigger weight will be applied to measurements that have
        # a wide distance to a previous measurement.
        # This assumes a higher resolution of measurements in the lower
        # instances range and a lower resolution in the higher instances range.
        # This will also explictly drop the first value
        # Avoid weights of zero! These are not defined with the used algos.
        weights = np.concatenate([[0.1], np.diff(x)])
        weights = np.subtract(weights.max()+1, weights)
        fit_params, fit_covariances, infodict, errmsg, ier = \
                curve_fit(fit_func, x, y, sigma=weights, full_output=True)
        # TODO calculate the goodness of fit

        plt.figure()
        measurement_label = 'Measured task execution time on %d rows' % rows
        group['duration'].plot(label=measurement_label)
        plt.plot(x, fit_func(x, *fit_params), label='Fitting')
        plt.yscale('log')
        plt.legend(loc='best')

        fname = 'fitting.pdf'
        print ">>>%s" % fname
        plt.savefig(fname)

    def plot_total_response_time(self):
        """ Plot the total response time vs the number of instances """
        selection_lambda = lambda x: x['op_name'] == "ResponseTask"
        y_label = "Median Respone Time in ms"
        field = 'end'
        file_infix = 'response'
        self._plot_row_functions(selection_lambda, field, y_label, file_infix)

    def plot_mean_task_size(self, selection_lambda):
        """ Plot the mean task size of a task vs the number of instances
        selection_lambda -- filter rows that should be plotted
        """
        y_label = "Median Task Duration in ms"
        field = 'duration'
        file_infix = 'meantasksize'
        self._plot_row_functions(selection_lambda, field, y_label, file_infix)

    def _plot_row_functions(self, selection_lambda, field, y_label, file_infix):
        """ Plot an aggregated field vs the instances per row size

        selection_lambda -- filter rows that should be plotted
        field -- the field that should be aggregated
        y_label -- describe the aggregate of the field for y-axis label
        file_infix -- string used for the middle portion of the filename
        """
        criterion = self._df.apply(selection_lambda, axis=1)
        tasks = self._df[criterion]
        group = tasks.groupby(["rows", "instances"]).median()

        group[field].unstack('rows').plot()
        plt.xlabel("Number of Instances")
        plt.ylabel(y_label)
        plt.yscale('log')
        plt.legend(title=self._legend_title)
        fname = "%s_%s_%d.pdf" % (self._group_id, file_infix, int(time.time()))
        plt.savefig(fname)
        print ">>>%s" % fname

    def _collect(self):
        """ Return a list of dictionaries with the following keys

        run
        build
        user
        query_name
        op_id, op_name, start, end, duration
        """
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
