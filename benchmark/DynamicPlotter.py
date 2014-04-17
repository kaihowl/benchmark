import ast
import os
from pylab import average, median, std
import matplotlib.pyplot as plt
import time

class DynamicPlotter:

    def __init__(self, benchmarkGroupId):
        self._groupId = benchmarkGroupId
        self._dirOutput = os.path.join(os.getcwd(), "plots", str(self._groupId))
        self._runs = self._collect()

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
    def plotGroups(self, queryToGroupMapping):
      runStats = self._aggregateToGroups(queryToGroupMapping)

      fig = plt.figure(1, figsize=(10,10))
      ax = fig.add_subplot(1,1,1)
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

    def _collect(self):
        data = {}
        dirResults = os.path.join(os.getcwd(), "results", self._groupId)
        if not os.path.isdir(dirResults):
            raise Exception("Group result directory '%s' not found!" % dirResults)

        for str_run in os.listdir(dirResults):
          dirRun = os.path.join(dirResults, str_run)

          if not os.path.isdir(dirRun):
            continue

          run = int(str_run)

          data[run] = {}

          for str_mts in os.listdir(dirRun):
            dirMts = os.path.join(dirRun, str_mts)
            if not os.path.isdir(dirMts):
              continue

            mts = int(str_mts)

            data[run][mts] = list()
            for user in os.listdir(dirMts):
              dirUser = os.path.join(dirMts, user)
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
