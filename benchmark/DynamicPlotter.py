import ast
import os
import sys
from pylab import *
import matplotlib.pyplot as plt
import time

# tries to convert a number to int. 
# returns the successful conversion result or original string
def _try_int(s):
    try: 
        return int(s)
    except ValueError:
        return s

class DynamicPlotter:

    def __init__(self, benchmarkGroupId):
        self._groupId = benchmarkGroupId
        self._dirOutput = os.path.join(os.getcwd(), "plots", str(self._groupId))
        self._runs = self._collect()
        self._buildIds = self._runs[self._runs.keys()[0]].keys()

        if not os.path.isdir(self._dirOutput):
            os.makedirs(self._dirOutput)

    # queryToGroupMapping is a dictionary that has query names as keys and the responding
    # group they fall into as the value. This will then print statistics for the groups instead of 
    # individual queries.
    def printGroupFormatted(self, queryToGroupMapping):
      logStr = ""
      runStats = self._aggregateToGroups(queryToGroupMapping)

      for groupName in set(queryToGroupMapping.values()):
        logStr += "mts_%s\tthroughput\tavgSRT\tmedSRT\n" % groupName
        for intRunId, runId in sorted([(_try_int(x), x) for x in runStats.keys()]):
          stats = runStats[runId]
          statDict = stats[groupName]
          logStr += "%s\t%s\t%s\t%s\n" % (runId, statDict["throughput"],
              statDict["avgservertime"], statDict["medservertime"])
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
      x_values = sorted([int(x) for x in runStats.keys()])

      line_colors = { 
          "OLAP": "b",
          "OLTP": "g",
          "TOLAP": "r"
      }

      for name in ("OLAP",):
        y_values = [runStats[str(x)][name]["throughput"] for x in x_values]
        ax.plot(x_values, y_values, line_colors[name], label=name)

      for name in ("OLTP", "TOLAP"):
        y_values = [runStats[str(x)][name]["avgservertime"] for x in x_values]
        ax2.plot(x_values, y_values, line_colors[name], label=name)

      lines, labels = ax.get_legend_handles_labels()
      lines2, labels2 = ax2.get_legend_handles_labels()
      ax2.legend(lines + lines2, labels + labels2, loc=4)

      fname = "varying_mts_%s.pdf" % str(int(time.time()))
      plt.savefig(fname)
      plt.close()

    def _aggregateToGroups(self, queryToGroupMapping):
      runStats = {}
      for runId, runData in self._runs.iteritems():
        # key: group name
        # value: list of throughput/averageservertime for all queries belonging to the group
        tmpStats = {}
        txStats = runData[runData.keys()[0]]["txStats"]
        for txId, singleTxStat in txStats.iteritems():
          curGroupName = queryToGroupMapping[txId]
          tmpStats.setdefault(curGroupName, list())
          tmpStats[curGroupName].append(singleTxStat)

        finalStats = {}
        for groupName, statList in tmpStats.iteritems():
          totalThroughput = sum([x["throughput"] for x in statList])
          totalAverage = sum([x["throughput"] * x["avgservertime"] for x in statList]) / totalThroughput
          totalMedian = sum([x["throughput"] * x["medservertime"] for x in
            statList]) / totalThroughput
          finalStats[groupName] = {
              "throughput": totalThroughput,
              "avgservertime": totalAverage,
              "medservertime": totalMedian}

        runStats[runId] = finalStats

      return runStats

    # prints a per query view of the degree of parallelism 
    # for a query and the average of the median runtimes 
    # of all operations in a query
    # filterBySubStrings is a list of strings used as an output filter
    # only lines matching at least one substring in that list will show
    def printQueryOpStatistics(self, filterBySubStrings=list()):
      logStr = "runId\topStatName\tminPar\tavgPar\tmaxPar\tavgMedSRT\n"
      for intRunId, runId in sorted([(_try_int(x), x) for x in self._runs.keys()]):
        runData = self._runs[runId]
        opStats = runData[runData.keys()[0]]["opStats"]
        for opStatName in sorted(opStats.keys()):
          statDict = opStats[opStatName]
          curLineStr = "%s\t%s\t%s\t%s\t%s\t%s\n" % (runId, opStatName, statDict["minCount"], statDict["avgCount"], statDict["maxCount"], statDict["avgMedSrt"])
          if (len(filterBySubStrings)==0 or reduce(lambda x, el: x or el in curLineStr, filterBySubStrings, False)):
            logStr += curLineStr

      return logStr

    def _collect(self):
        runs = {}
        dirResults = os.path.join(os.getcwd(), "results", self._groupId)
        if not os.path.isdir(dirResults):
            raise Exception("Group result directory '%s' not found!" % dirResults)

        # --- Runs --- #
        for run in os.listdir(dirResults):

            dirRun = os.path.join(dirResults, run)
            if os.path.isdir(dirRun):
                runs[run] = {}

                # --- Builds --- #
                for build in os.listdir(dirRun):
                    dirBuild = os.path.join(dirRun, build)
                    if os.path.isdir(dirBuild):

                        # -- Count Users --- #
                        numUsers = 0
                        for user in os.listdir(dirBuild):
                            dirUser = os.path.join(dirBuild, user)
                            if os.path.isdir(dirUser):
                                numUsers += 1

                        # key: queryname + "_" + operator name + "_" + operator id till underscore
                        # value: list of count and median runtimes of the operator group
                        tmpOpStats = {}

                        # key: txId
                        # value: server runtime
                        tmpTxStats = {}

                        # -- Users --- #
                        for user in os.listdir(dirBuild):

                            dirUser = os.path.join(dirBuild, user)
                            if os.path.isdir(dirUser):
                                if not os.path.isfile(os.path.join(dirUser, "transactions.log")):
                                    print "WARNING: no transaction log found in %s!" % dirUser
                                    continue
                                for rawline in open(os.path.join(dirUser, "transactions.log")):

                                    linedata = rawline.split(";")
                                    if len(linedata) < 2:
                                        continue

                                    txId        = linedata[0]
                                    runtime     = float(linedata[1])
                                    starttime   = float(linedata[2])
                                    
                                    tmpTxStats.setdefault(txId, list())

                                    if len(linedata) > 3:
                                        opData = ast.literal_eval(linedata[3])
                                        curOpStats = {}
                                        for op in opData:
                                            if op["name"].encode('utf8') == "ResponseTask":
                                              queryEndTime = float(op["endTime"])
                                            # uniquely identify all instances of an operator
                                            # in a query
                                            opGroupName = op["name"] + op["id"][:op["id"].rfind("_")]
                                            curOpStats.setdefault(opGroupName, list())
                                            opTime = float(op["endTime"]) - float(op["startTime"])
                                            curOpStats[opGroupName].append(opTime)

                                        for opGroupName, rtList in curOpStats.iteritems():
                                          opStatName = txId + "_" + opGroupName
                                          tmpOpStats.setdefault(opStatName, list())
                                          tmpOpStats[opStatName].append({
                                            "count": len(rtList),
                                            "medianDur": median(rtList)
                                          })

                                    tmpTxStats[txId].append(queryEndTime)



                        # key: queryname + "_" + operator name + "_" + operator id till underscore
                        # value: count and avg-median server runtime
                        opStats = {}
                        for opStatName, tmpRuns in tmpOpStats.iteritems():
                          countList = [x["count"] for x in tmpRuns]
                          avgCount = average(countList)
                          minCount = amin(countList)
                          maxCount = amax(countList)

                          opStats[opStatName] = {
                              "avgCount": avgCount,
                              "minCount": minCount,
                              "maxCount": maxCount,
                              "avgMedSrt": average([x["medianDur"] for x in tmpRuns])
                          }

                        # key: queryname
                        # value: throughput,  medservertime, and avgservertime
                        txStats = {}
                        for txId, serverRunTimes in tmpTxStats.iteritems():
                          avgTime = average(serverRunTimes)
                          medTime = median(serverRunTimes)
                          counts = len(serverRunTimes)
                          txStats[txId] = {
                              "throughput": counts,
                              "avgservertime": avgTime, 
                              "medservertime": medTime}

                        runs[run][build] = {
                            "opStats": opStats,
                            "txStats": txStats}

        return runs
