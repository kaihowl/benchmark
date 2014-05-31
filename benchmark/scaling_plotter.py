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

        def task_size_fit_func(x, a, b):
            return np.divide(a, x) + b

        def linear_func(x, a, b):
            return np.multiply(a, x) + b

        def quadratic_func(x, a, b):
            return np.multiply(a, np.square(x)) + b

        self.task_size_fit_func = task_size_fit_func
        self.linear_func = linear_func
        self.quadratic_func = quadratic_func

    def plot_fitting_for(self, eval_selection_lambda, task_name, rows=None):
        """ Plot curve(s) and fitting of a task's size/duration

            eval_selection_lambda -- Select the task whose duration shall be fit
            task_name -- Describe the task selected by the selection lambda
            rows -- Only plot and fit for the run(s) with this table size.
                    Optional argument. Takes scalar and sequence-like values.
                    Default is to print fitting for all table sizes
        """
        if not rows:
            rows = self._df['rows'].unique()

        try:
            for cur_rows in rows:
                self._plot_single_fitting_for(eval_selection_lambda, task_name, cur_rows)
        except TypeError:  # no list but single element for rows
            self._plot_single_fitting_for(eval_selection_lambda, task_name, rows)

    def plot_tablesize_fitting_for(self, eval_selection_lambda, task_name, fit_func_str):
        """ Plot the fitting for a task with table size as the independent var

            eval_selection_lambda -- Select the task whose duration shall be fit
            task_name -- Describe the task selected by the lambda
            fit_func_str -- "linear"/"quadratic" describes which function to use
                            for the overall fitting
        """
        fit_funcs = {
                "linear": self.linear_func,
                "quadratic": self.quadratic_func
                }
        criterion = self._df.apply(eval_selection_lambda, axis=1)
        data = self._df[criterion]
        rows = data["rows"].unique()
        rows.sort()

        def fit_for_rows(rows):
            group = data[data['rows'] == rows].groupby("instances").mean()
            return self._fit_single_data(group['duration'], self.task_size_fit_func)

        fittings = pandas.DataFrame([x/100000] + list(fit_for_rows(x)["fit_params"]) for x in rows)
        fittings.columns = ["rows_100k", "a", "b"]
        fittings.set_index("rows_100k", inplace=True)

        a_fitting = self._fit_single_data(fittings["a"], fit_funcs[fit_func_str])
        b_fitting = self._fit_single_data(fittings["b"], fit_funcs[fit_func_str])

        plt.subplot(1, 2, 1)
        plt.title("Parameter a fitting")
        fittings["a"].plot(style="o", label="Single fitted parameters")
        plt.xlabel("Table size in 100k")
        plt.plot(a_fitting["plottable_x"], a_fitting["plottable_y"], label='Overall fitting')
        plt.legend(loc='best')

        plt.subplot(1, 2, 2)
        plt.title("Parameter b fitting")
        fittings["b"].plot(style="o", label="Single fitted parameters")
        plt.xlabel("Table size in 100k")
        plt.plot(b_fitting["plottable_x"], b_fitting["plottable_y"], label='Overall fitting')
        plt.legend(loc='best')

        fname = "%s_parfitting_%s_%d.pdf" % (self._group_id, task_name, int(time.time()))
        plt.savefig(fname)
        print ">>>%s" % fname

    def _fit_single_data(self, data, fit_func):
        """ Fit data for selected task's size for a given number of rows.
            Returns the parameters for the fitting function, the expected
            y-values, a range of plottable x values between min and max rows in
            the data set, the corresponding plottable y values, the chi-square
            value, the probability of the cdf, and the reduced chi square
            value.

            data -- Pandas dataframe with x-values as index and single column as
                    y values
            fit_func -- The function to be fit to the data
        """
        x = np.array(data.index)
        y = np.array(data)
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
        fit_params, fit_covariances = curve_fit(fit_func, x, y, sigma=weights)

        # Calculate goodness of fit with reduced Chi-Square
        # Main ideas:
        # http://www.physics.utoronto.ca/~phy326/python/curve_fit_to_data.py
        y_exp = fit_func(x, *fit_params)
        chisq = (np.square(y[1:]-y_exp[1:])/y_exp[1:]).sum()
        dof = len(x[1:]) - len(fit_params)
        cdf = scipy.special.chdtrc(dof,chisq)
        reduced_chi_square = chisq / dof
        plottable_x = np.arange(x.min(), x.max()+1)
        plottable_y = fit_func(plottable_x, *fit_params)
        return {
                "fit_params": fit_params,
                "expected_y": y_exp,
                "plottable_x": plottable_x,
                "plottable_y": plottable_y,
                "chisq": chisq,
                "cdf": cdf,
                "reduced_chi_square": reduced_chi_square}

    def _plot_single_fitting_for(self, eval_selection_lambda, task_name, rows):
        """ Plot curve and fitting for selected task's size/duration

            eval_selection_lambda -- Select the task whose duration shall be fit
            rows -- Only plot and fir for the run with this table size
        """

        def filter(x):
            return x["rows"] == rows and eval_selection_lambda(x)

        # TODO consider passing in the data. Might be faster.
        criterion = self._df.apply(filter, axis=1)
        cur_data = self._df[criterion]
        group = cur_data.groupby("instances").mean()

        fitting = self._fit_single_data(group['duration'], self.task_size_fit_func)

        # Plot
        plt.figure()
        measurement_label = 'Measured task execution time on %d rows' % rows
        group['duration'].plot(label=measurement_label)
        plt.plot(fitting["plottable_x"], fitting['plottable_y'], label='Fitting')
        plt.yscale('log')
        plt.ylabel("Mean %s Duration in ms" % task_name)
        plt.legend(loc='best')

        fname = '%s_fitting_%s_%d_rows_%d.pdf' % (self._group_id, task_name, rows, int(time.time()))
        print ">>>%s" % fname
        plt.savefig(fname)

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
