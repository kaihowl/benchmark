import ast
import os
from pylab import average, median, std
import pandas
import matplotlib.pyplot as plt
import time


class ScalingPlotter:

    def __init__(self, benchmark_group_id):
        self._group_id = benchmark_group_id
        self._data = self._collect()
        self._df = pandas.DataFrame(self._data)
        self._df["rows"] = self._df['run'].map(lambda x: int(x.split("_")[1]))
        self._df["instances"] = self._df['run'].map(lambda x: int(x.split("_")[3]))

    def plot_total_response_time(self):
        response_tasks = self._df[self._df['op_name'] == "ResponseTask"]
        group = response_tasks.groupby(["rows", "instances"]).median()

        group['end'].unstack('rows').plot()
        plt.xlabel("Number of Instances")
        plt.ylabel("Total Respone Time in ms")
        plt.yscale('log')
        filename = "%s_response_%d.pdf" % (self._group_id, int(time.time()))
        plt.savefig(filename)
        print ">>>%s" % filename

    # selection_lambda takes a row in the dataframe and returns true for the
    # tasks of which the mean task size should be plotted.
    def plot_mean_task_size(self, selection_lambda):
        criterion = self._df.apply(selection_lambda, axis=1)
        tasks = self._df[criterion]
        group = tasks.groupby(["rows", "instances"]).median()

        group['duration'].unstack('rows').plot()
        plt.xlabel("Number of Instances")
        plt.ylabel("Mean Task Duration in ms")
        plt.yscale('log')
        filename = "%s_meantasksize_%d.pdf" % (self._group_id, int(time.time()))
        plt.savefig(filename)
        print ">>>%s" % filename

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
