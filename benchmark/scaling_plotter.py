import ast
import os
import pandas
import matplotlib.pyplot as plt
import time
from scipy.optimize import curve_fit
import scipy
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

    def plot_fitting_for(self, eval_selection_lambda, task_name, multiply_table_sizes=False):
        """ Plot overall single-step fitting.

            eval_selection_lambda -- Select the task whose duration shall be fit
            task_name -- Describe the task selected by the selection lambda
            multiply_table_sizes -- Split the tablesizes in VBAP and VBAK and
                                    multiply. Used for NestedLoopEquiJoin.
        """
        tasks = self._filter_by_lambda(eval_selection_lambda)

        meantimes = tasks.groupby(["rows", "instances"])["duration"].mean()
        def add_weights(data):
            data.name = 'meanDuration'
            new_data = data.reset_index(level='rows', drop=True).reset_index()
            x = np.array(new_data['instances'])
            weights = np.concatenate([[0.1], np.diff(x)])
            weights = np.subtract(weights.max()+1, weights)
            new_data['weights'] = weights
            return new_data

        # Rows, instances, meanDuration, weights
        fitdata = meantimes.groupby(level=0).apply(lambda x: add_weights(x)).reset_index()
        # Curve fits struggle with too little values
        fitdata["rows_100k"] = fitdata["rows"] / 100000

        # TODO needs to use multiply_table_sizes
        def fit_func(x_tuples, a, b):
            instances = [x[0] for x in x_tuples]
            tablesizes = [x[1] for x in x_tuples]
            return np.multiply(a, tablesizes) / instances + b

        x = np.array(fitdata[["instances", "rows_100k"]])
        y = np.array(fitdata["meanDuration"])
        weights = fitdata["weights"]
        params, cov = curve_fit(fit_func, x, y, sigma=weights)

        # Plot each row size with measured and fitted curve
        plt.figure()
        plt.yscale("log")
        plt.ylabel("Mean %s duration" % task_name)
        for rows_100k, group in fitdata.groupby("rows_100k"):
            min_instances = group["instances"].min()
            max_instances = group["instances"].max()
            x = np.arange(min_instances, max_instances, step=1)
            x_pred = [(i, rows_100k) for i in x]
            fit_label = "Fitted %d" % (rows_100k * 100000)
            plt.plot(x, fit_func(x_pred, *params), "--", label=fit_label)
            measured_label = "Measured %d" % (rows_100k * 100000)
            group.plot(x="instances", y="meanDuration", label=measured_label)
        plt.legend(loc="best")

        filename = "%s_singlefitting_%s_%d.pdf" % \
            (self._group_id, task_name, int(time.time()))
        print ">>>%s" % filename
        plt.savefig(filename)

    def plot_total_response_time(self, dump_to_csv=False):
        """ Plot the total response time vs the number of instances """
        selection_lambda = lambda x: x['op_name'] == "ResponseTask"
        y_label = "Mean Respone Time in ms"
        field = 'end'
        file_infix = 'response'
        self._plot_row_functions(self._filter_by_lambda(selection_lambda), field, y_label, file_infix, dump_to_csv=dump_to_csv)

    def plot_true_response_time(self, dump_to_csv=False):
        """ Plot the true response time vs the number of instances

            The true response time is the difference between the startTime of
            the ResponseTask and the endTime of the RequestParseTask. This
            accounts for longer json transformation for bigger number of
            instances.
        """
        group = self._df.groupby(['rows', 'instances', 'query_id'])

        def true_response_time(query_df):
            resp_tasks = query_df[query_df['op_name'] == "ResponseTask"]
            assert len(resp_tasks) == 1, "There should be one ResponseTask"

            req_p_tasks = query_df[query_df['op_name'] == "RequestParseTask"]
            assert len(req_p_tasks) == 1, "There should be one RequestParseTask"

            end_time = resp_tasks['start'].iloc[0]
            start_time = req_p_tasks['end'].iloc[0]
            return end_time - start_time

        resp_times_series = group.apply(true_response_time)
        resp_times_series.name = 'duration'
        resp_times_df = resp_times_series.reset_index()
        y_label = "True Mean Respone Time in ms"
        field = 'duration'
        file_infix = 'trueresponse'
        self._plot_row_functions(resp_times_df, field, y_label, file_infix, dump_to_csv=dump_to_csv)

    def plot_mean_task_size(self, selection_lambda, task_name="mean", dump_to_csv=False):
        """ Plot the mean task size of a task vs the number of instances
        selection_lambda -- filter rows that should be plotted
        task_name -- the name is used for y label construction and the file name
        """
        y_label = "Mean %s Duration in ms" % task_name
        field = 'duration'
        file_infix = 'meantasksize_%s' % task_name
        self._plot_row_functions(self._filter_by_lambda(selection_lambda), field, y_label, file_infix, dump_to_csv=dump_to_csv)

    def _filter_by_lambda(self, selection_lambda):
        """ Filter the current dataframe by applying the selection lambda

            selection_lambda -- boolean lambda over panda dataframe rows
        """
        return self._df[self._df.apply(selection_lambda, axis=1)]

    def _plot_row_functions(self, data, field, y_label, file_infix, dump_to_csv=False):
        """ Plot an aggregated field vs the instances per row size

        data -- pandas dataframe
        field -- the field that should be aggregated
        y_label -- describe the aggregate of the field for y-axis label
        file_infix -- string used for the middle portion of the filename
        dump_to_csv -- Also dump the used data points to a csv file (default: False)
        """
        group = data.groupby(["rows", "instances"])

        group[field].mean().unstack('rows').plot()
        plt.xlabel("Number of Instances")
        plt.ylabel(y_label)
        plt.yscale('log')
        plt.legend(title=self._legend_title)
        fprefix = "%s_%s_%d" % (self._group_id, file_infix, int(time.time()))
        fname = fprefix + ".pdf"
        plt.savefig(fname)
        print ">>>%s" % fname

        if dump_to_csv:
            with open(fprefix+".csv", "w") as csv_file:
                group[field].agg([np.median, np.mean]).reset_index().to_csv(csv_file)
                print ">>>%s" % csv_file.name

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

                    query_id = 0
                    for rawline in open(log_file_name):
                        query_id += 1
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
                                "query_id": query_id,
                                "query_name": query_name,
                                "op_id": op_data["id"],
                                "op_name": op_data["name"],
                                "start": op_data["startTime"],
                                "end": op_data["endTime"],
                                "duration": dur})

        return data
