import ujson
import os
import matplotlib.pyplot as plt
import time
import sys
import pandas


class VarUserPlotter:

    def __init__(self, benchmarkGroupId):
        self._groupId = benchmarkGroupId
        self._df = pandas.DataFrame(self._collect())

    def plot_throughput_per_run(self):
        result = self._df.groupby(['instances', 'users', 'run']).count()[
            'txId'].mean(level=['instances', 'users']).unstack(level=0)

        self._plot_and_csv_result(result, "throughput", "Throughput")

    def plot_meansize_per_run(self):
        """ Plot the mean size of dependent tasks per run """
        flat_dict_list = list()

        def expand_op_data(row):
            orig_row = dict(row)
            for op in orig_row.pop('opData'):
                flat_dict_list.append(dict(orig_row.items() + op.items()))

        self._df.apply(expand_op_data, axis=1)
        ops_df = pandas.DataFrame(flat_dict_list)

        filter_op_names = [
            "TableScan",
            "PrefixSum",
            "RadixCluster",
            "Histogram",
            "NestedLoopEquiJoin"]
        criterion = ops_df["name"].apply(lambda x: x in filter_op_names)
        filtered_ops = ops_df[criterion]
        filtered_ops['opDuration'] = filtered_ops[
            'endTime'] - filtered_ops['startTime']
        result = filtered_ops.groupby(
            ['instances', 'users']).mean()['opDuration'].unstack(level=0)

        self._plot_and_csv_result(result, "meansize", "Mean Task Size")

    def _plot_and_csv_result(self, df, fileinfix, y_label):
        """ Plot and csv the dataframe

            df -- the dataframe to be plot
            fileinfix -- used to distinguish files
            y_label -- the y label to be used
        """

        plt.figure()
        df.plot()
        fbasename = "%s_%s_%d" % (self._groupId, fileinfix, int(time.time()))
        pdfname = "%s.pdf" % fbasename
        plt.ylabel(y_label)
        plt.savefig(pdfname)
        print ">>>%s" % pdfname

        csvname = "%s.csv" % fbasename
        df.to_csv(csvname)
        print ">>>%s" % csvname

    def _collect(self):
        data = []
        dirResults = os.path.join(os.getcwd(), "results", self._groupId)
        if not os.path.isdir(dirResults):
            raise Exception(
                "Group result directory '%s' not found!" % dirResults)

        for str_instances_users_run in os.listdir(dirResults):
            dirRun = os.path.join(dirResults, str_instances_users_run)

            if not os.path.isdir(dirRun):
                continue

            str_instances, str_users, str_run = str_instances_users_run.split(
                "_")
            instances = int(str_instances)
            users = int(str_users)
            run = int(str_run)

            # We expect only one build to exist
            if len(os.listdir(dirRun)) > 1:
                sys.exit("Expected exactly one build but found several!")

            dirBuild = os.path.join(dirRun, os.listdir(dirRun)[0])
            if not os.path.isdir(dirBuild):
                sys.exit("Expected a build dir, but only found a file!")

            for user in os.listdir(dirBuild):
                dirUser = os.path.join(dirBuild, user)
                if not os.path.isdir(dirUser):
                    continue

                logFileName = os.path.join(dirUser, "transactions.log")
                if not os.path.isfile(logFileName):
                    print "WARNING: no transaction log found in %s!" % dirUser
                    continue

                for rawline in open(logFileName):
                    linedata = rawline.split(";")
                    if len(linedata) < 2:
                        continue
                    txId = linedata[0]
                    runTime = float(linedata[1])
                    startTime = float(linedata[2])
                    # convert python literal to json
                    # in order to use the faster ujson instead of ast
                    jsonData = linedata[3].replace("u'", '"').replace("'", '"')
                    opData = ujson.loads(jsonData)

                    # add new item to main data list structure
                    data.append({
                        "instances": instances,
                        "users": users,
                        "run": run,
                        "user": user,
                        "txId": txId,
                        "runTime": runTime,
                        "startTime": startTime,
                        "opData": opData})

        return data
