import ast
import os
import sys
from pylab import *

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
          finalStats[groupName] = {
              "throughput": totalThroughput,
              "avgservertime": totalAverage}

        runStats[runId] = finalStats

      for runId, stats in runStats.iteritems():
        for groupName, statDict in stats.iteritems():
          logStr += "%s\t%s\t%s\t%s\n" % (runId, groupName, statDict["throughput"], statDict["avgservertime"])

      return logStr


    # prints a per query view of the degree of parallelism 
    # for a query and the average of the median runtimes 
    # of all operations in a query
    def printQueryOpStatistics(self):
      logStr = ""
      for runId, runData in self._runs.iteritems():
        opStats = runData[runData.keys()[0]]["opStats"]
        for opStatName in sorted(opStats.keys()):
          statDict = opStats[opStatName]
          logStr += "%s\t%s\t%s\t%s\n" % (runId, opStatName, statDict["count"], statDict["avgMedSrt"])

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
                                            opGroupName = op["name"] + op["id"][:op["id"].find("_")]
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
                          if not avgCount == minCount:
                            print "Expected min to equal average parallelism count within a group"
                            sys.exit(1)

                          opStats[opStatName] = {
                              "count": avgCount,
                              "avgMedSrt": average([x["medianDur"] for x in tmpRuns])
                          }

                        # key: queryname
                        # value: throughput and avgservertime
                        txStats = {}
                        for txId, serverRunTimes in tmpTxStats.iteritems():
                          avgTime = average(serverRunTimes)
                          counts = len(serverRunTimes)
                          txStats[txId] = {
                              "throughput": counts,
                              "avgservertime": avgTime}

                        runs[run][build] = {
                            "opStats": opStats,
                            "txStats": txStats}

        return runs
