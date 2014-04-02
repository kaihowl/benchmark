import os
import shutil
import matplotlib as mpl
#mpl.use('Agg')
from pylab import *

import ast

class MixedWLPlotter:

    def __init__(self, benchmarkGroupId):
        self._groupId = benchmarkGroupId
        self._dirOutput = os.path.join(os.getcwd(), "plots", str(self._groupId))
        self._varyingParameter = None
        self._runs = self._collect()
        self._buildIds = self._runs[self._runs.keys()[0]].keys()

        if not os.path.isdir(self._dirOutput):
            os.makedirs(self._dirOutput)

    def printStatistics(self, queries):
        logStr = ""
        output = {}
        #print self._runs
        for runId, runData in self._runs.iteritems():
            output[runId] = ""
            for query in queries:
                if query in runData[runData.keys()[0]]["txStats"]:
                    stats = runData[runData.keys()[0]]["txStats"][query]
                    output[runId] += str(self._groupId) + "\t\t" + query + " " + str(stats["totalRuns"]) + " " + str(stats["srtMin"]) + " " + str(stats["srtMax"]) + " " +str(stats["srtAvg"])  + " " + str(stats["srtStd"]) + " " + str(stats["opAvg"]) + " " +str(stats["opStd"]) + " " + str(stats["schedAvg"]) + " " +str(stats["schedStd"]) + "\n"
            #stats = runData[runData.keys()[0]]["txStats"]["q7idx_vbak"]
            #output[runId] += "  q7idx_vbak " + str(stats["totalRuns"]) + " " + str(stats["srtMin"]) + " " + str(stats["srtMax"]) + " " +str(stats["srtAvg"])  + " " + str(stats["srtStd"]) + " " + str(stats["opAvg"]) + " " +str(stats["opStd"]) + " " + str(stats["schedAvg"]) + " " +str(stats["schedStd"]) + "\n"
            #stats = runData[runData.keys()[0]]["txStats"]["xselling"]
            #output[runId] += "  xselling " + str(stats["totalRuns"]) + " " + str(stats["srtMin"]) + " " + str(stats["srtMax"]) + " " +str(stats["srtAvg"])  + " " + str(stats["srtStd"]) + " " + str(stats["opAvg"]) + " " +str(stats["opStd"]) + " " + str(stats["schedAvg"]) + " " +str(stats["schedStd"]) + "\n"

        for run in sorted(output.iterkeys()):
            logStr += "%s %s" % (str(run), output[run])
        #    numUsers = runData[runData.keys()[0]]["numUsers"]
        #    print "Run ID: %s [%s users]" % (runId, numUsers)
        #    print "=============================="
        #    for buildId, buildData in runData.iteritems():
        #        if buildData == {'numUsers': 0, 'txStats': {}}:
        #            continue
        #        print "|\n+-- Build ID: %s" % buildId
        #        print "|"
        #        print "|     Transaction       tps      min(ms)    max(ms)   avg(ms)    median(ms)"
        #        totalRuns = 0.0
        #        totalTime = 0.0
        #        for txId, txData in buildData["txStats"].iteritems():
        #            totalRuns += txData["totalRuns"]
        #            totalTime += txData["userTime"] 
        #        print str(totalRuns) + " " + str(totalTime)
        #        for txId, txData in buildData["txStats"].iteritems():
        #            print "|     -------------------------------------------------------------------------------------------"
        #            print "|     TX: {:14s} tps: {:05.2f}, min: {:05.2f}, max: {:05.2f}, avg: {:05.2f}, med: {:05.2f} (all in ms)".format(txId, float(txData["totalRuns"]) / totalTime, txData["rtMin"]*1000, txData["rtMax"]*1000, txData["rtAvg"]*1000, txData["rtMed"]*1000)
        #            print "|     -------------------------------------------------------------------------------------------"
        #            if txData["operators"] and len(txData["operators"].keys()) > 0:
        #                print "|       Operator                   #perTX     min(ms)    max(ms)   avg(ms)    median(ms)"
        #                print "|       ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        #                for opName, opData in txData["operators"].iteritems():
        #                    print "|       {:25s}  {:05.2f}      {:05.2f}      {:05.2f}      {:05.2f}      {:05.2f}".format(opName, opData["avgRuns"], opData["rtMin"], opData["rtMax"], opData["rtAvg"], opData["rtMed"])
        #        print "|     -------------------------------------------------------------------------------------------"
        #        print "|     total:            %1.2f tps\n" % (totalRuns / totalTime)
        return logStr

    def printFormattedStatistics(self, queries):
        logStr = ""
        output = {}
        #print self._runs
        for runId, runData in self._runs.iteritems():
            output[runId] = ""
            total_runs = 0;
            avg_runtime = 0;
            total_runtime = 0;
            avg_op_runtime = 0;
            total_op_runtime = 0;

            for query in queries:
                if query in runData[runData.keys()[0]]["txStats"]:
                    stats = runData[runData.keys()[0]]["txStats"][query]
                    total_runs += stats["totalRuns"]
                    total_runtime += stats["srtAvg"] * stats["totalRuns"]
                    total_op_runtime += stats["opAvg"] * stats["totalRuns"]
                    output[runId] += str(self._groupId) + " " + str(runId) + " " + query +  " " + str(stats["totalRuns"]) +  " " +str(stats["srtAvg"])  + " " + str(stats["srtStd"]) + " " + str(stats["opAvg"]) + " " +str(stats["opStd"]) + "\n"
            if(total_runs>0):
                avg_runtime = total_runtime / total_runs
                avg_op_runtime = total_op_runtime / total_runs
                output[runId] += str(self._groupId) + " " + str(runId) + " avg " + str(total_runs) + " " + str(avg_runtime) + " " + str(avg_op_runtime) + "\n"

        logStr = "GroupId\trunId\tlogKey\ttotalRuns\tsrtAvg\tsrtStd\topAvg\topStd\n"
        for run in sorted(output.iterkeys()):
            logStr += "%s" % (output[run])    
        return logStr
    
    def printFormattedStatisticsAverage(self, queries):
        logStr = ""
        output = {}
        #print self._runs
        for runId, runData in self._runs.iteritems():
            output[runId] = ""
            total_runs = 0;
            avg_runtime = 0;
            total_runtime = 0;
            avg_op_runtime = 0;
            total_op_runtime = 0;

            for query in queries:
                if query in runData[runData.keys()[0]]["txStats"]:
                    stats = runData[runData.keys()[0]]["txStats"][query]
                    total_runs += stats["totalRuns"]
                    total_runtime += stats["srtAvg"] * stats["totalRuns"]
                    total_op_runtime += stats["opAvg"] * stats["totalRuns"]
            if(total_runs>0):
                avg_runtime = total_runtime / total_runs
                avg_op_runtime = total_op_runtime / total_runs
            
            output[runId] += str(self._groupId) + " " + str(runId) + " " + str(total_runs) + " " + str(avg_runtime) + " " + str(avg_op_runtime) + "\n"

            #stats = runData[runData.keys()[0]]["txStats"]["q7idx_vbak"]
            #output[runId] += "  q7idx_vbak " + str(stats["totalRuns"]) + " " + str(stats["srtMin"]) + " " + str(stats["srtMax"]) + " " +str(stats["srtAvg"])  + " " + str(stats["srtStd"]) + " " + str(stats["opAvg"]) + " " +str(stats["opStd"]) + " " + str(stats["schedAvg"]) + " " +str(stats["schedStd"]) + "\n"
            #stats = runData[runDaa.keys()[0]]["txStats"]["xselling"]
            #output[runId] += "  xselling " + str(stats["totalRuns"]) + " " + str(stats["srtMin"]) + " " + str(stats["srtMax"]) + " " +str(stats["srtAvg"])  + " " + str(stats["srtStd"]) + " " + str(stats["opAvg"]) + " " +str(stats["opStd"]) + " " + str(stats["schedAvg"]) + " " +str(stats["schedStd"]) + "\n"

        for run in sorted(output.iterkeys()):
            logStr += "%s" % (output[run])
        #    numUsers = runData[runData.keys()[0]]["numUsers"]
        #    print "Run ID: %s [%s users]" % (runId, numUsers)
        #    print "=============================="
        #    for buildId, buildData in runData.iteritems():
        #        if buildData == {'numUsers': 0, 'txStats': {}}:
        #            continue
        #        print "|\n+-- Build ID: %s" % buildId
        #        print "|"
        #        print "|     Transaction       tps      min(ms)    max(ms)   avg(ms)    median(ms)"
        #        totalRuns = 0.0
        #        totalTime = 0.0
        #        for txId, txData in buildData["txStats"].iteritems():
        #            totalRuns += txData["totalRuns"]
        #            totalTime += txData["userTime"] 
        #        print str(totalRuns) + " " + str(totalTime)
        #        for txId, txData in buildData["txStats"].iteritems():
        #            print "|     -------------------------------------------------------------------------------------------"
        #            print "|     TX: {:14s} tps: {:05.2f}, min: {:05.2f}, max: {:05.2f}, avg: {:05.2f}, med: {:05.2f} (all in ms)".format(txId, float(txData["totalRuns"]) / totalTime, txData["rtMin"]*1000, txData["rtMax"]*1000, txData["rtAvg"]*1000, txData["rtMed"]*1000)
        #            print "|     -------------------------------------------------------------------------------------------"
        #            if txData["operators"] and len(txData["operators"].keys()) > 0:
        #                print "|       Operator                   #perTX     min(ms)    max(ms)   avg(ms)    median(ms)"
        #                print "|       ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        #                for opName, opData in txData["operators"].iteritems():
        #                    print "|       {:25s}  {:05.2f}      {:05.2f}      {:05.2f}      {:05.2f}      {:05.2f}".format(opName, opData["avgRuns"], opData["rtMin"], opData["rtMax"], opData["rtAvg"], opData["rtMed"])
        #        print "|     -------------------------------------------------------------------------------------------"
        #        print "|     total:            %1.2f tps\n" % (totalRuns / totalTime)
        return logStr

    def printOpStatistics(self):
        logStr = "" 
        output = {}
        for runId, runData in self._runs.iteritems():
            opStats = runData[runData.keys()[0]]["txStats"]["0"]["operators"]
            runstr = ""
            for opname, opData in opStats.iteritems():
                runstr +=  "\t" + str(opname) + " " + str(opData["totRuns"]) + " " + str(opData["rtAvg"]) + " " +    str(opData["rtStd"]) + "\n"
            output[runData[runData.keys()[0]]["numUsers"]] = runstr
        for users in sorted(output.iterkeys()):
            logStr += "%s" % (output[users])
        return logStr
    

    def plotTotalThroughput(self):
        plt.title("Total Transaction Throughput")
        plt.ylabel("Transactions per Second")
        plt.xlabel("Number of Parallel Users")
        for buildId in self._buildIds:
            plotX = []
            plotY = []
            for runId, runData in self._runs.iteritems():
                plotX.append(runData[buildId]["numUsers"])
                totalRuns = 0.0
                totalTime = 0.0
                for txId, txData in runData[buildId]["txStats"].iteritems():
                    totalRuns += txData["totalRuns"]
                    totalTime += txData["userTime"]
                plotY.append(totalRuns / totalTime)
            plotX, plotY = (list(t) for t in zip(*sorted(zip(plotX, plotY))))
            plt.plot(plotX, plotY, label=buildId)
        plt.xticks(plotX)
        plt.legend(loc='upper left', prop={'size':10})
        fname = os.path.join(self._dirOutput, "total_throughput.pdf")
        plt.savefig(fname)
        plt.close()

    def plotTransactionResponseTimes(self):
        for txId in ["NEW_ORDER","PAYMENT","STOCK_LEVEL","DELIVERY","ORDER_STATUS"]:
            plt.title("%s Response Times" % txId)
            plt.ylabel("Avg. Response Time in ms")
            plt.xlabel("Number of Parallel Users")
            for buildId in self._buildIds:
                plotX = []
                plotY = []
                for runId, runData in self._runs.iteritems():
                    plotX.append(runData[buildId]["numUsers"])
                    plotY.append(runData[buildId]["txStats"][txId]["rtAvg"] / 1000)
                plotX, plotY = (list(t) for t in zip(*sorted(zip(plotX, plotY))))
                plt.plot(plotX, plotY, label=buildId)
            plt.legend(loc='upper left', prop={'size':10})
            fname = os.path.join(self._dirOutput, "%s_avg_rt.pdf" % txId)
            plt.savefig(fname)
            plt.close()

    def plotResponseTimesVaryingUsers(self):
        plt.figure(1, figsize=(10, 20))
        curPlt = 0
        for buildId in self._buildIds:
            curPlt += 1
            plt.subplot(len(self._buildIds), 1, curPlt)
            plt.tight_layout()
            plt.yscale('log')
            plt.ylabel("Response Time in s")
            pltData = []
            xtickNames = []
            for runId, runData in self._runs.iteritems():
                numUsers = len(runData[runData.keys()[0]])
                aggData  = self._aggregateUsers(runId, buildId)
                for txId, txData in aggData.iteritems():
                    pltData.append(txData["raw"])
                    xtickNames.append("%s, %s users" % (txId, numUsers))
            bp = plt.boxplot(pltData, notch=0, sym='+', vert=1, whis=1.5)
            plt.title("Transaction response times for varying users in build '%s" % buildId)
            plt.setp(bp['boxes'], color='black')
            plt.setp(bp['whiskers'], color='black')
            plt.setp(bp['fliers'], color='red', marker='+')
            plt.xticks(arange(len(xtickNames)), xtickNames, rotation=45, fontsize=5)
        fname = os.path.join(self._dirOutput, "varying_users.pdf")
        plt.savefig(fname)
        plt.close()

    def plotResponseTimeFrequencies(self):
        for buildId in self._buildIds:
            for runId, runData in self._runs.iteritems():
                aggData = self._aggregateUsers(runId, buildId)
                maxPlt = len(aggData.keys())
                curPlt = 0
                plt.figure(1, figsize=(10, 4*maxPlt))
                for txId, txData in aggData.iteritems():
                    curPlt += 1
                    plt.subplot(maxPlt, 1, curPlt)
                    plt.tight_layout()
                    plt.title("RT Frequency in %s (build '%s', run '%s')" % (txId, buildId, runId))
                    plt.xlabel("Response Time in s")
                    plt.ylabel("Number of Transactions")
                    plt.xlim(txData["min"], txData["max"])
                    plt.xticks([txData["min"], txData["average"], percentile(txData["raw"], 90), txData["max"]],
                               ["" % txData["min"], "avg\n(%s)" % txData["average"], "90th percentile\n(%s)" % percentile(txData["raw"], 90), "max\n(%s)" % txData["max"]],
                               rotation=45, fontsize=5)
                    plt.grid(axis='x')
                    y, binEdges = np.histogram(txData["raw"], bins=10)
                    binCenters = 0.5*(binEdges[1:]+binEdges[:-1])
                    plt.plot(binCenters, y, '-')
                fname = os.path.join(self._dirOutput, "rt_freq_%s_%s.pdf" % (buildId, runId))
                plt.savefig(fname)
                plt.close()

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
                        #runs[run][build] = []

                        # -- Count Users --- #
                        numUsers = 0
                        for user in os.listdir(dirBuild):
                            dirUser = os.path.join(dirBuild, user)
                            if os.path.isdir(dirUser):
                                numUsers += 1

                        txStats = {}
                        opStats = {}
                        hasOpData = False

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
                                    

                                    opStats.setdefault(txId, {})
                                    txStats.setdefault(txId,{
                                        "totalTime": 0.0,
                                        "userTime":  0.0,
                                        "totalRuns": 0,
                                        "rtTuples":  [],
                                        "srtTuples": [],
                                        "opTime": [],
                                        "schedTime": [],
                                        "rtMin":     0.0,
                                        "rtMax":     0.0,
                                        "rtAvg":     0.0,
                                        "rtMed":     0.0,
                                        "rtStd":     0.0,
                                        "srtMin":     0.0,
                                        "srtMax":     0.0,
                                        "srtAvg":     0.0,
                                        "srtMed":     0.0,
                                        "srtStd":     0.0,
                                        "opAvg":       0.0,
                                        "opStd":       0.0,
                                        "schedAvg":     0.0,
                                        "schedStd":     0.0
                                    })
                                    txStats[txId]["totalTime"] += runtime
                                    txStats[txId]["userTime"]  += runtime / float(numUsers)
                                    txStats[txId]["totalRuns"] += 1
                                    txStats[txId]["rtTuples"].append((starttime, runtime))



                                    if len(linedata) > 3:
                                        totOpTime = 0.0
                                        opEndTime = 0.0
                                        opData = ast.literal_eval(linedata[3])
                                        for op in opData:
                                            if op["name"].encode('utf8') == "ResponseTask":
                                                opEndTime = float(op["endTime"])
                                            
                                            opStats[txId].setdefault(op["name"], {
                                                "rtTuples":  [],
                                                "totRuns":   0.0,
                                                "rtMin":     0.0,
                                                "rtMax":     0.0,
                                                "rtAvg":     0.0,
                                                "rtMed":     0.0,
                                                "rtStd":     0.0
                                            })
                                            opTime = float(op["endTime"]) - float(op["startTime"])
                                            opStats[txId][op["name"]]["rtTuples"].append(opTime)
                                            totOpTime += opTime
                                        txStats[txId]["opTime"].append(totOpTime)
                                        txStats[txId]["srtTuples"].append(opEndTime)
                                        txStats[txId]["schedTime"].append(opEndTime - totOpTime)


                        for txId, txData in txStats.iteritems():
                            allRuntimes = [a[1] for a in txData["rtTuples"]]
                            txStats[txId]["rtTuples"].sort(key=lambda a: a[0])
                            txStats[txId]["rtMin"] = amin(allRuntimes)
                            txStats[txId]["rtMax"] = amax(allRuntimes)
                            txStats[txId]["rtAvg"] = average(allRuntimes)
                            txStats[txId]["rtMed"] = median(allRuntimes)
                            txStats[txId]["rtStd"] = std(allRuntimes)

                            allSRuntimes = txData["srtTuples"]
                            
                            if len(allSRuntimes):
                                txStats[txId]["srtMin"] = amin(allSRuntimes)
                                txStats[txId]["srtMax"] = amax(allSRuntimes)
                                txStats[txId]["srtAvg"] = average(allSRuntimes)
                                txStats[txId]["srtMed"] = median(allSRuntimes)
                                txStats[txId]["srtStd"] = std(allSRuntimes)

                            opTimes = txData["opTime"]
                            if len(opTimes):
                                txStats[txId]["opAvg"] = average(opTimes)
                                txStats[txId]["opStd"] = std(opTimes)

                            schedTimes = txData["schedTime"]
                            if len(opTimes):
                                txStats[txId]["schedAvg"] = average(schedTimes)
                                txStats[txId]["schedStd"] = std(schedTimes)


                            for opId, opData in opStats[txId].iteritems():

                                opStats[txId][opId]["totRuns"] = len(opData["rtTuples"])
                                opStats[txId][opId]["rtMin"] = amin(opData["rtTuples"])
                                opStats[txId][opId]["rtMax"] = amax(opData["rtTuples"])
                                opStats[txId][opId]["rtAvg"] = average(opData["rtTuples"])
                                opStats[txId][opId]["rtMed"] = median(opData["rtTuples"])
                                opStats[txId][opId]["rtStd"] = std(opData["rtTuples"])
                            txStats[txId]["operators"] = opStats[txId]
                        runs[run][build] = {"txStats": txStats, "numUsers": numUsers}
        return runs
