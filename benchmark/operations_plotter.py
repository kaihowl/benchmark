import ast
import os
from pylab import average, median, std
import pandas
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np


class OperationsPlotter:

    def __init__(self, benchmark_group_id):
        self._group_id = benchmark_group_id
        self._data = self._collect()
        self._df = pandas.DataFrame(self._data)

    def plot_histograms(self, op_name):
        """ Plot one histogram of durations for op_name per run

            op_name -- op_name to be plot
        """
        filename = '%s_histograms.pdf' % op_name
        pp = PdfPages(filename)

        # Tell notify wrapper to attach file to email
        print "\n>>>%s\n" % filename

        for run in self._df['run'].unique():
            escaped_run = run.replace("_", "\_")
            data = self._df[self._df['run'] == run]

            criterion = data['op_name'].map(lambda x: x==op_name)
            ops = data[criterion]
            title = "Histogram of %s' duration for run %s" % (op_name, escaped_run)
            self._new_hist(ops['duration'], title, pp)

        pp.close()

    def plot_numa_histograms(self, op_name):
        """ Plot histograms of durations for op_name per run and NUMA node

            op_name -- name of the operation to be plot
        """
        filename = "%s_histograms_numa.pdf" % op_name
        pp = PdfPages(filename)
        print "\n>>>%s\n" % filename

        for run in self._df['run'].unique():
            escaped_run = run.replace("_", "\_")
            data = self._df[self._df['run'] == run]

            criterion = data['op_name'].map(lambda x: x == op_name)
            ops = data[criterion]

            for node in ops['node'].unique():
                cur_ops = ops[ops['node'] == node]
                title = "Histogram of %s' duration for run %s on node %s" % (op_name, escaped_run, node)
                self._new_hist(
                        cur_ops['duration'],
                        title,
                        pp)
        pp.close()

    def print_modal_statistics(self, op_name):
        """ Print the characteristics of all modes in multi-modal histograms.

            op_name -- Name of operation whose duration is used in histograms
        """

        for run in self._df['run'].unique():
            escaped_run = run.replace("_", "\_")
            data = self._df[self._df['run'] == run]
            criterion = data['op_name'].map(lambda x: x == op_name)
            ops = data[criterion]

            # Detect runs of non-zero counts and print stats if several of
            # those runs exist -> meaning, we have a multi-modal distribution
            count, division = np.histogram(ops['duration'])
            bounded_counts = np.hstack(([0], count, [0]))
            run_sequence = np.diff((bounded_counts>0)*1)
            sequences = zip(np.where(run_sequence==1)[0], np.where(run_sequence==-1)[0])
            print count
            print sequences
            if len(sequences) > 1:
                print "Found %d-modal distribution for run %s" % (len(sequences), run)

                def print_summary(data):
                    print " duration: %.3f to %.3f" % (data['duration'].min(), data['duration'].max())
                    print " start: %.3f to %.3f" % (data['start'].min(), data['start'].max())
                    print " end: %.3f to %.3f" % (data['end'].min(), data['end'].max())
                    print " nodes: %s" % (data['node'].unique())
                    print " cores: %s" % (data['core'].unique())
                    print " data: %d to %d" % (data['data'].min(), data['data'].max())
                    print " input data: %d to %d" % (data['inRows'].min(), data['inRows'].max())
                    print " output data: %d to %d" % (data['outRows'].min(), data['outRows'].max())
                    print " line data: %d to %d" % (data['line'].min(), data['line'].max())

                for seq_start, seq_end in sequences:
                    print "New sequence:"
                    upper_ops = ops['duration'] >= division[seq_start]
                    lower_ops = ops['duration'] <= division[seq_end]
                    data = ops[upper_ops & lower_ops]
                    print_summary(data)


    def plot_mean_query_durations(self):
        """ Plot bar chart for mean query durations of all different queries """

        plt.figure()
        plt.title("Mean query durations by query")

        criterion = self._df["op_name"] == "ResponseTask"
        response_tasks = self._df[criterion]
        response_tasks.groupby("query_name").mean()["duration"].plot(kind='bar')

        filename = 'mean_query_durations.pdf'
        plt.savefig(filename)
        print ">>>%s" % filename
        plt.close()


    def _new_hist(self, series, title, pp):
        """Plot a new histogram in a multi-page pdf

            series -- Pandas series
            title -- The title of the histogram
            pp -- the PdfPages to plot to
        """

        plt.figure()
        plt.title(title)
        xlabel = "Operation Instance %s" % series.name
        plt.xlabel(xlabel)
        ylabel = "Num Occurences in Entire Experiment"
        plt.ylabel(ylabel)
        series.hist()
        pp.savefig()
        plt.close()


    # returns a list of dictionaries with the following keys
    # run, build, user, query_name, op_id, op_name, start, duration, line
    # line identifies a run within a user as a line in the transaction log
    # corresponds to a run within a user
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

                    line = 0
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
                                "core": op_data["lastCore"],
                                "node": op_data["lastNode"],
                                "data": op_data["data"] if op_data["name"]!="ResponseTask" else 0,
                                "inRows": op_data["inRows"],
                                "outRows": op_data["outRows"],
                                "line": line,
                                "duration": dur})
                        line += 1

        return data
