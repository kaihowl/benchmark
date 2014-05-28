import ast
import os
from pylab import average, median, std
import matplotlib.pyplot as plt
import time
import sys
import pandas

class DynamicPlotter:

    def __init__(self, benchmarkGroupId):
        self._groupId = benchmarkGroupId
        self._dirOutput = os.path.join(os.getcwd(), "plots", str(self._groupId))
        self._runs = self._collect()
        self._varying_users_dataframe = self._make_varying_users_dataframe()

        if not os.path.isdir(self._dirOutput):
            os.makedirs(self._dirOutput)

    # queryToGroupMapping is a dictionary that has query names as keys and the responding
    # group they fall into as the value. This will then print statistics for the groups instead of
    # individual queries.
    def printGroupFormatted(self, queryToGroupMapping):
      runStats = self._aggregateToGroups(queryToGroupMapping)
      numRuns = len(runStats[runStats.keys()[0]])
      logStr = "Average of %i runs:\n" % numRuns

      for groupName in set(queryToGroupMapping.values()):
        logStr += "mts_%s\tavgThroughput\tavgAvgServertime\tavgMedServertime\tavgStdServertime\n" % groupName
        for mts in sorted(runStats.keys()):
          statsList = runStats[mts]

          def getValueOrDefault(x, key, defValue):
            if groupName in x:
              return x[groupName][key]
            else:
              return defValue
          avgThroughput = average([getValueOrDefault(x, "throughput", 0) for x in statsList])
          avgAvgServertime = average([getValueOrDefault(x, "avgservertime", 0) for x in statsList])
          avgMedServertime = average([getValueOrDefault(x, "medservertime", 0) for x in statsList])
          avgStdServertime = average([getValueOrDefault(x, "stdservertime", 0) for x in statsList])
          logStr += "%s\t%s\t%s\t%s\t%s\n" % (mts, avgThroughput, avgAvgServertime,
              avgMedServertime, avgStdServertime)
        logStr += "\n\n"

      return logStr

    # expects a queryToGroupMapping containing all querynames to be mapped to either the string
    # "OLAP", "OLTP", or "TOLAP". It will then plot a diagram with the throughput for OLAP, and#
    # the responsetimes for TOLAP and OLTP.
    def plotGroups(self, queryToGroupMapping, title='Varying MTS'):
      runStats = self._aggregateToGroups(queryToGroupMapping)

      fig = plt.figure(1, figsize=(10,10))
      ax = fig.add_subplot(1,1,1)
      ax.set_title(title)
      ax.set_xlabel("MTS")
      ax.set_ylabel("Throughput")
      ax2 = ax.twinx()
      ax2.set_ylabel("Response Time in s")
      ax2.set_yscale("log")
      #MTS values
      x_values = sorted([x for x in runStats.keys()])

      line_colors = {
          "OLAP": "b",
          "OLTP": "g",
          "TOLAP": "r"
      }

      for name in ("OLAP",):
        y_values = [average([y[name]["throughput"] for y in runStats[x]]) for x in x_values]
        ax.plot(x_values, y_values, line_colors[name], label=name)

      for name in ("OLTP", "TOLAP"):
        y_values = [average([y[name]["avgservertime"] for y in runStats[x]]) for x in x_values]
        ax2.plot(x_values, y_values, line_colors[name], label=name)

      lines, labels = ax.get_legend_handles_labels()
      lines2, labels2 = ax2.get_legend_handles_labels()
      ax2.legend(lines + lines2, labels + labels2, loc=4)

      fname = "varying_mts_%s.pdf" % str(int(time.time()))
      plt.savefig(fname)
      # Output filename for notify to attach to email
      print "\n>>>%s\n" % fname
      plt.close()

    # returns a dictionary
    # key: mts
    # value: list of dictionaries with key: groupName and value: throughput,
    # avgservertime, medianservertime
    def _aggregateToGroups(self, queryToGroupMapping):
      mtsStats = {}
      for run, mtsDict in self._runs.iteritems():
        for mts, queries in mtsDict.iteritems():
          stats = {}
          for query in queries:
            groupName = queryToGroupMapping[query["txId"]]
            stats.setdefault(groupName, list())
            for op in query["opData"]:
              if op["id"] == "respond":
                servertime = op["endTime"]
                break
            stats[groupName].append(servertime)

          finalStats = {}
          for groupName, servertimes in stats.iteritems():
            finalStats[groupName] = {
                "throughput": len(servertimes),
                "avgservertime": average(servertimes),
                "medservertime": median(servertimes),
                "stdservertime": std(servertimes)
                }

          mtsStats.setdefault(int(mts), list()).append(finalStats)

      return mtsStats

    # prints a per query view of the degree of parallelism
    # for a query and the average of the median runtimes
    # of all operations in a query
    def printQueryOpStatistics(self):
        logStr = "mts\trun\tquery_opname\tcount\tavgavgruntime\tavgstdruntime\n"
        result_lines = []
        for run, mtsDict in self._runs.iteritems():
            for mts, queryList in mtsDict.iteritems():
                tmpOpStats = {}
                for query in queryList:
                    # collect runtimes of same operators in this query
                    tmpQueryStats = {}
                    for op in query["opData"]:
                        runtime = op["endTime"] - op["startTime"]
                        name = query["txId"] + "_" + op['name']
                        tmpQueryStats.setdefault(name, list()).append(runtime)
                    # insert parallelization degree and runtime for this query
                    for opname, runtimes in tmpQueryStats.iteritems():
                        tmpOpStats.setdefault(opname, list()).append({
                                "count": len(runtimes),
                                "avgruntime": average(runtimes),
                                "stdruntime": std(runtimes)})
                for opname, statDictList in tmpOpStats.iteritems():
                    count = average([x["count"] for x in statDictList])
                    avgavgruntime = average([x["avgruntime"] for x in statDictList])
                    avgstdruntime = average([x["stdruntime"] for x in statDictList])
                    result_lines.append((mts, run, opname, count, avgavgruntime, avgstdruntime))

        for line in sorted(result_lines):
            line_template = "%s\t" * len(line) + "\n"
            curLineStr = line_template % line
            logStr += curLineStr

        return logStr

    def plot_throughput_per_run(self):
        df = self._varying_users_dataframe

        result = df.groupby(['instances', 'users']).count()['txId'].unstack(level=0)

        self._plot_and_csv_result(result, "throughput", "Throughput")

    def plot_meansize_per_run(self):
        """ Plot the mean size of dependent tasks per run """
        df = self._varying_users_dataframe

        flat_dict_list = list()
        def expand_op_data(row):
            orig_row = dict(row)
            for op in orig_row.pop('opData'):
                flat_dict_list.append(dict(orig_row.items() + op.items()))

        df.apply(expand_op_data, axis=1)
        ops_df = pandas.DataFrame(flat_dict_list)

        filter_op_names = ["TableScan", "PrefixSum", "RadixCluster", "Histogram", "NestedLoopEquiJoin" ]
        criterion = ops_df["name"].apply(lambda x: x in filter_op_names)
        filtered_ops = ops_df[criterion]
        filtered_ops['opDuration'] = filtered_ops['endTime'] - filtered_ops['startTime']
        result = filtered_ops.groupby(['instances', 'users']).mean()['opDuration'].unstack(level=0)

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

    def _flatruns(self):
        """ Returns the result from _collect as a flat list of dictionaries.
            This output can be consumed by pandas.
        """
        flat = list()
        for run, run_data in self._runs.iteritems():
            for mts, mts_list in run_data.iteritems():
                for user_dict in mts_list:
                    additions = {
                            "run": run,
                            "mts": mts}
                    flat.append(dict(additions.items() + user_dict.items()))

        return flat

    def _make_varying_users_dataframe(self):
        df = pandas.DataFrame(self._flatruns())

        new_columns = list(df.columns)
        new_columns[new_columns.index("mts")] = "instances"
        new_columns[new_columns.index("run")] = "users"
        df.columns = new_columns

        return df

    def _collect(self):
        data = {}
        dirResults = os.path.join(os.getcwd(), "results", self._groupId)
        if not os.path.isdir(dirResults):
            raise Exception("Group result directory '%s' not found!" % dirResults)

        for str_mts_run in os.listdir(dirResults):
          dirRun = os.path.join(dirResults, str_mts_run)

          if not os.path.isdir(dirRun):
            continue

          str_mts, str_run = str_mts_run.split("_")
          mts = int(str_mts)
          run = int(str_run)

          data.setdefault(run, {})

          # We expect only one build to exist
          if len(os.listdir(dirRun)) > 1:
              sys.exit("Expected exactly one build but found several!")

          dirBuild = os.path.join(dirRun, os.listdir(dirRun)[0])
          if not os.path.isdir(dirBuild):
              sys.exit("Expected a build dir, but only found a file!")

          data[run][mts] = list()
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
              opData = ast.literal_eval(linedata[3])

              # add new item to main data list structure
              data[run][mts].append({
                "user": user,
                "txId": txId,
                "runTime": runTime,
                "startTime": startTime,
                "opData": opData})

        return data
