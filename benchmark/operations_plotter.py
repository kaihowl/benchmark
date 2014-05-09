import ast
import os
from pylab import average, median, std
import pandas
import matplotlib.pyplot as plt


class OperationsPlotter:

    def __init__(self, benchmark_group_id):
        self._group_id = benchmark_group_id
        self._data = self._collect()
        self._df = pandas.DataFrame(self._data)

    def plot_histograms(self):
        from matplotlib.backends.backend_pdf import PdfPages
        filename = 'histograms.pdf'
        pp = PdfPages(filename)
        # Tell notify wrapper to attach file to email
        print "\n>>>%s\n" % filename
        for run in self._df['run'].unique():
            print "Plotting run %s" % run
            plt.figure()
            escaped_title = run.replace("_", "\_")
            title = "Histogram of probe durations for run %s" % escaped_title
            plt.title(title)
            xlabel = "Probe Instance Duration"
            plt.xlabel(xlabel)
            ylabel = "Occurences in Entire Experiment"
            plt.ylabel(ylabel)
            data = self._df[self._df['run'] == run]
            criterion = data['op_name'].map(lambda x: x=="HashJoinProbe")
            data[criterion]['duration'].hist()
            pp.savefig()
            plt.close()

            # Plot single NUMA nodes
            for node in data['node'].unique():
                plt.figure()
                subtitle = title + " for node %d" % node
                plt.title(subtitle)
                plt.xlabel(xlabel)
                plt.ylabel(ylabel)
                subdata = data[data['node'] == node]
                subcrit = subdata['op_name'].map(lambda x: x=="HashJoinProbe")
                subdata[subcrit]['duration'].hist()
                pp.savefig()
                plt.close()
        pp.close()

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
                                "core": op_data["lastCore"],
                                "node": op_data["lastNode"],
                                "duration": dur})

        return data
