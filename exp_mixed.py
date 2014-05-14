import argparse
import benchmark
import os
import pprint
import time

from benchmark.bench_mixed import MixedWLBenchmark
from benchmark.mixedWLPlotter import MixedWLPlotter
from benchmark.DynamicPlotter import DynamicPlotter
from benchmark.benchmark import Benchmark
from benchmark.repeating_user import RepeatingUser
from benchmark.scaling_plotter import ScalingPlotter

def runbenchmarks(groupId, s1, **kwargs):
    output = ""
    users = [1, 11]#, 48, 64, 96, 128]
    #users = [512]
    for i in users:
        runId = str(i)
        kwargs["numUsers"] = i
        b1 = MixedWLBenchmark(groupId, runId, s1, **kwargs)
        b1.run()
        time.sleep(1)
    plotter = MixedWLPlotter(groupId)
    output += plotter.printFormattedStatisticsAverage(kwargs["benchmarkQueries"])
    return output

# Creates Figure 2 of DASFAA paper
def runBenchmark_scaling_curve_Scan(groupId, s1, numRuns=5, **kwargs):
    selection_lambda = lambda x: x["op_name"] == "TableScan"
    kwargs["legendTitle"] = "Rows"
    return runBenchmark_scaling_curve("queries/scaling-curves/scan.json", selection_lambda, groupId, s1, numRuns, **kwargs)

# Creates Figure 3 of DASFAA paper
def runBenchmark_scaling_curve_Join(groupId, s1, numRuns=5, **kwargs):
    selection_lambda = lambda x: x["op_name"] == "NestedLoopEquiJoin"
    kwargs["legendTitle"] = "Probe Rows"
    return runBenchmark_scaling_curve("queries/scaling-curves/join.json", selection_lambda, groupId, s1, numRuns, **kwargs)

def runBenchmark_scaling_curve(mainQueryFile, eval_selection_lambda, groupId, s1, numRuns=5, **kwargs):
    output = ""

    # TODO support full and narrow schema

    # TODO which scheduler should we use?
    kwargs["scheduler"] = "DynamicPriorityScheduler"
    kwargs["hyriseDBPath"] = "/home/Kai.Hoewelmeyer/vldb-tables/scaler/output-range"

    # This benchmark's users terminate themselves after a set number of runs
    kwargs["runtime"] = 0

    rows = [100*10**3, 1*10**6, 10*10**6, 100*10**6]
    # len(instances) == 20
    instances = [1] + range(2, 16, 2) + range(16, 256, 32) + range(256, 1025, 256)

    if not kwargs["evaluationOnly"]:
        for cur_rows in rows:
            for cur_instances in instances:
                kwargs["userClass"]        = RepeatingUser
                kwargs["numUsers"]         = 1
                kwargs["userArgs"]         = {
                        "repetitions"      : numRuns,
                        "instances"        : cur_instances,
                        # Capped instances are used in join.json
                        "capped_instances" : min(cur_instances, 100),
                        "rows"             : cur_rows }

                kwargs["tableLoadQueries"] = ("preload", )
                kwargs["tableLoadArgs"]    = {"rows": cur_rows}
                kwargs["prepareQueries"]   = list()
                kwargs["benchmarkQueries"] = ("mainQueryFile", )

                b = Benchmark(groupId, "rows_%d_instances_%d" % (cur_rows, cur_instances), s1, **kwargs)
                b.addQueryFile("preload", "queries/scaling-curves/preload.json")
                b.addQueryFile("mainQueryFile",  mainQueryFile)
                b.run()

    plotter = ScalingPlotter(groupId, **kwargs)
    plotter.plot_total_response_time(dump_to_csv=True)
    plotter.plot_mean_task_size(eval_selection_lambda, dump_to_csv=True)
    plotter.plot_fitting_for(eval_selection_lambda, rows=max(rows))
    return output

# NOTE: Changed the queries to the name_spaced versions since no standard versions exist.
def runBenchmark_varying_users_OLTP(groupId, s1, **kwargs):
    output = ""
    kwargs["oltpQueries"] = ("vldb_q6a", "vldb_q6b", "vldb_q7", "vldb_q8", "vldb_q9")

    users = [64]#[1, 4, 8, 16, 24, 32, 64]
    for j in users:
        print "starting OLTP benchmark with " + str(j) + " users"
        runId = str(j)
        kwargs["oltpUser"] = j
        kwargs["numUsers"] = j
        b1 = MixedWLBenchmark(groupId, runId, s1, **kwargs)
        b1.run()
        time.sleep(3)
    plotter = MixedWLPlotter(groupId)
    output += plotter.printFormattedStatistics(kwargs["oltpQueries"])
    return output



def runBenchmark_varying_users_OLAP(groupId, s1, **kwargs):
    output = ""
    #users = [1, 2, 4, 8, 16]#, 24, 32]#, 48, 64, 96, 128]

    kwargs["olapQueries"] = ("q10i","q11i","q12i")

    instances = [32]#[16, 32]
    users = [16]#, 32, 64, 128]#[1, 4, 8, 16, 24, 32, 64]
    for i in instances:
        for j in users:
           print "starting benchmark with " + str(i) + " instances and " + str(j) + " users"
           runId = str(i) + "_" + str(j)
           kwargs["olapInstances"] = i
           kwargs["olapUser"] = j
           kwargs["numUsers"] = kwargs["olapUser"]
           b1 = MixedWLBenchmark(groupId, runId, s1, **kwargs)
           b1.run()
           time.sleep(3)
    plotter = MixedWLPlotter(groupId)
    output += plotter.printFormattedStatistics(kwargs["olapQueries"])
    return output

def runBenchmark_varying_users(groupId, s1, **kwargs):
    output = ""
    #users = [1, 2, 4, 8, 16]#, 24, 32]#, 48, 64, 96, 128]

    kwargs["olapQueries"] = ("q10i","q11i","q12i")

    # numbers chosen to match the original paper's datapoints
    instances = [1, 8, 32, 128]
    users = [1, 2, 4, 5, 8, 10, 16, 20, 24, 30, 32, 40, 50, 60, 64]
    for i in instances:
        for j in users:
           print "starting benchmark with " + str(i) + " instances and " + str(j) + " users"
           runId = str(i) + "_" + str(j)
           kwargs["olapInstances"] = i
           kwargs["olapUser"] = j
           kwargs["numUsers"] = kwargs["olapUser"] + kwargs["oltpUser"] + kwargs["tolapUser"]
           b1 = MixedWLBenchmark(groupId, runId, s1, **kwargs)
           b1.run()
           time.sleep(3)
    plotter = MixedWLPlotter(groupId)
    output += plotter.printFormattedStatistics(kwargs["oltpQueries"]+kwargs["tolapQueries"] +kwargs["olapQueries"])
   # output += plotter.printOpStatistics ()
    return output

def runBenchmark_prio(groupId, s1, **kwargs):
    output = ""
    #users = [1, 2, 4, 8, 16]#, 24, 32]#, 48, 64, 96, 128]

    kwargs["olapQueries"] = ("q6_ch","q10","q12")
    kwargs["olapInstances"] = 10
    kwargs["tolapUser"] = 1
    kwargs["tolapThinkTime"] = 1
    kwargs["tolapQueries"] = ("xselling","q11")
    kwargs["oltpUser"] = 1
    kwargs["oltpQueries"] = ("q7idx_vbak","q8idx_vbap")

    #users = [1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64]
    users = [1]#, 2, 3, 4]#, 4, 8, 10, 12, 14, 20, 30, 60]
    kwargs["bench_users_a"] = [12]# 3,4 ,5, 6]#, 4, 8, 16, 24, 32, 40, 48, 56, 64, 96, 128]
    kwargs["bench_users_t"] = [12]
    for j in users:
        print "starting benchmark with " + str(j) + " users"
        runId = str(j)
        kwargs["olapUser"] = j
        kwargs["numUsers"] = kwargs["olapUser"] + kwargs["oltpUser"] + kwargs["tolapUser"]
        b1 = MixedWLBenchmark(groupId, runId, s1, **kwargs)
        b1.run()
        time.sleep(2)
    plotter = MixedWLPlotter(groupId)
    #output += groupId + "\n"
    output += plotter.printFormattedStatisticsAverage(kwargs["oltpQueries"]+kwargs["tolapQueries"] +kwargs["olapQueries"])
   # output += plotter.printOpStatistics ()
    return output

def runBenchmark_task_sizes(groupId, s1, **kwargs):
    output = ""
    #users = [1, 2, 4, 8, 16]#, 24, 32]#, 48, 64, 96, 128]

    kwargs["olapQueries"] = ("q6_ch","q10","q12")
    kwargs["olapUser"] = 0
    kwargs["tolapUser"] = 11
    kwargs["tolapThinkTime"] = 0
    kwargs["tolapQueries"] = ("xselling",)
    kwargs["oltpUser"] = 0
    kwargs["oltpQueries"] = ("q7idx_vbak",)

    #instances = [1, 2, 4, 8, 11, 12, 16, 20, 22, 28, 33]
    instances = [11]
    for j in instances:
        print "starting benchmark with " + str(j) + " instances"
        runId = str(j)
        kwargs["olapInstances"] = j
        kwargs["numUsers"] = kwargs["olapUser"] + kwargs["oltpUser"] + kwargs["tolapUser"]
        b1 = MixedWLBenchmark(groupId, runId, s1, **kwargs)
        b1.run()
        time.sleep(5)
    plotter = MixedWLPlotter(groupId)
    output += groupId + "\n"
    output += plotter.printStatistics(kwargs["oltpQueries"]+kwargs["tolapQueries"] +kwargs["olapQueries"])
   # output += plotter.printOpStatistics ()
    return output


def runBenchmark_varying_mts(groupId, settings, numRuns=1, separateOLAPTables=True, **kwargs):
    num_olap_users = 32
    output = ""

    kwargs["scheduler"] = "DynamicPriorityScheduler"
    kwargs["oltpQueries"] = ("vldb_q1", "vldb_q2", "vldb_q3", "vldb_q4", "vldb_q5", "vldb_q6a", "vldb_q6b", "vldb_q7", "vldb_q8", "vldb_q9")
    kwargs["oltpUser"] = 1
    kwargs["tolapQueries"] = ("vldb_xselling",)
    # TODO why was that 0?
    kwargs["tolapUser"] = 1
    # TODO is this in seconds?
    kwargs["tolapThinkTime"] = 1
    kwargs["olapQueries"] = ("vldb_q10", "vldb_q11", "vldb_q12")
    kwargs["olapUser"] = num_olap_users
    kwargs["separateOLAPTables"] = separateOLAPTables
    output += "separateOLAPTables = %s\n" % str(kwargs["separateOLAPTables"])
    # Rebuild only the first time
    # TODO this seems like non-sense to me
    kwargs["rebuild"] = True

    distincts = None

    mts_list = [10, 30, 50, 70, 150, 200, 250, 350, 400, 450, 500, 750, 1000]

    if not kwargs["evaluationOnly"]:
        for run in range(0, numRuns):
          print "Run %d of %d" % (run+1, numRuns)
          for mts in mts_list:
            print "starting benchmark with mts=%s and separateTables=%s" % (str(mts), str(separateOLAPTables))
            if not distincts is None:
              print "Reusing distincts from now on."
              kwargs["distincts"] = distincts
            kwargs["mts"] = mts
            kwargs["numUsers"] = kwargs["olapUser"] + kwargs["oltpUser"] + kwargs["tolapUser"]
            b1 = MixedWLBenchmark(groupId, "%d_%d" % (mts, run), settings, **kwargs)
            b1.run()
            time.sleep(5)
            # save distincts for next run
            distincts = b1.getDistinctValues()
            # do not rebuild on next run
            kwargs["rebuild"] = False

    groupMapping = {}
    identityMapping = {}
    for query in kwargs["oltpQueries"]:
     groupMapping[query] = "OLTP"
     identityMapping[query] = query
    for query in kwargs["olapQueries"]:
      groupMapping[query] = "OLAP"
      identityMapping[query] = query
    for query in kwargs["tolapQueries"]:
      groupMapping[query] = "TOLAP"
      identityMapping[query] = query
    plotter = DynamicPlotter(groupId)
    output += groupId + "\n"
    output += plotter.printGroupFormatted(groupMapping)
    output += "\n"
    output += plotter.printGroupFormatted(identityMapping)
    output += "\n"
    output += plotter.printQueryOpStatistics()
    plotter.plotGroups(groupMapping)
    return output

aparser = argparse.ArgumentParser(description='Python implementation of the TPC-C Benchmark for HYRISE')
aparser.add_argument('--duration', default=20, type=int, metavar='D',
                     help='How long to run the benchmark in seconds')
aparser.add_argument('--clients', default=-1, type=int, metavar='N',
                     help='The number of blocking clients to fork (note: this overrides --clients-min/--clients-max')
aparser.add_argument('--clients-min', default=1, type=int, metavar='N',
                     help='The minimum number of blocking clients to fork')
aparser.add_argument('--clients-max', default=1, type=int, metavar='N',
                     help='The maximum number of blocking clients to fork')
aparser.add_argument('--no-load', action='store_true',
                     help='Disable loading the data')
aparser.add_argument('--no-execute', action='store_true',
                     help='Disable executing the workload')
aparser.add_argument('--port', default=5001, type=int, metavar="P",
                     help='Port on which HYRISE should be run')
aparser.add_argument('--host', default="127.0.0.1", type=str, metavar="H",
                     help='IP on which HYRISE should be run remotely')
aparser.add_argument('--remoteUser', default="hyrise", type=str, metavar="R",
                     help='remote User for remote host on which HYRISE should be run remotely')
aparser.add_argument('--remote', action='store_true',
                     help='run hyrise server on a remote machine')
aparser.add_argument('--threads', default=0, type=int, metavar="T",
                     help='Number of server threads to use')
aparser.add_argument('--warmup', default=5, type=int,
                     help='Warmuptime before logging is activated')
aparser.add_argument('--manual', action='store_true',
                     help='Do not build and start a HYRISE instance (note: a HYRISE server must be running on the specified port)')
aparser.add_argument('--stdout', action='store_true',
                     help='Print HYRISE server\'s stdout to console')
aparser.add_argument('--stderr', action='store_true',
                     help='Print HYRISE server\'s stderr to console')
aparser.add_argument('--rebuild', action='store_true',
                     help='Force `make clean` before each build')
aparser.add_argument('--regenerate', action='store_true',
                     help='Force regeneration of TPC-C table files')
aparser.add_argument('--perfdata', default=True, action='store_true',
                     help='Collect additional performance data. Slows down benchmark.')
aparser.add_argument('--json', default=False, action='store_true',
                     help='Use JSON queries instead of stored procedures.')
aparser.add_argument('--evaluation-only', default=False, action='store_true',
                     help='Do not run the benchmark, only evaluate last results.')
args = vars(aparser.parse_args())

s1 = benchmark.Settings("Standard",
        PERSISTENCY="NONE",
        COMPILER="autog++",
        WITH_PAPI=0,
        USE_JE_MALLOC=True
        )


#gaza remote
#kwargs = {
#    "port"              : args["port"],
#    "manual"            : True,
#    "warmuptime"        : 5,
#    "runtime"           : 10,
#    "showStdout"        : False,
#    "showStderr"        : args["stderr"],
#    "rebuild"           : args["rebuild"],
#    "regenerate"        : args["regenerate"],
#    "noLoad"            : args["no_load"],
#    "serverThreads"     : args["threads"],
#    "collectPerfData"   : args["perfdata"],
#    "useJson"           : args["json"],
#    "dirBinary"         : "/home/Johannes.Wust/hyrise/build/",
#    "hyriseDBPath"      : "/home/Johannes.Wust/vldb-tables/scaler/output",#/home/Kai.Hoewelmeyer/vldb-tables/scaler/output",
#    "scheduler"         : "CentralScheduler",
#    "serverThreads"     : 30,
#    "remote"            : True,
#    "remoteUser"        : "Johannes.Wust",
#    "host"              : "gaza"
#}

#gaza local
#kwargs = {
#    "port"              : args["port"],
#    "warmuptime"        : 20,
#    "runtime"           : 120,
#    "prepareQueries"    : ("preload",),
#    "showStdout"        : False,
#    "showStderr"        : args["stderr"],
#    "rebuild"           : args["rebuild"],
#    "regenerate"        : args["regenerate"],
#    "noLoad"            : args["no_load"],
#    "serverThreads"     : args["threads"],
#    "collectPerfData"   : args["perfdata"],
#    "useJson"           : args["json"],
#    "dirBinary"         : "/home/Kai.Hoewelmeyer/hyrise/build/",
#    "hyriseDBPath"      : "/home/Kai.Hoewelmeyer/vldb-tables/scaler/output",
#    "scheduler"         : "DynamicPriorityScheduler",
#    "serverThreads"     : 31,
#    "remote"            : False,
#    "host"              : "127.0.0.1"
#}

##begram local
kwargs = {
    "port"              : args["port"],
    "warmuptime"        : 20,
    "runtime"           : 120,
    "showStdout"        : True,
    "showStderr"        : args["stderr"],
    "rebuild"           : args["rebuild"],
    "regenerate"        : args["regenerate"],
    "noLoad"            : args["no_load"],
    "collectPerfData"   : args["perfdata"],
    "useJson"           : args["json"],
    "evaluationOnly"    : args["evaluation_only"],

    "hyriseDBPath"      : "/home/Kai.Hoewelmeyer/vldb-tables/scaler/output",
    # Set this value according to the data in hyriseDBPath: full/narrow
    "schema"            : "full",

    "scheduler"         : "DynamicPriorityScheduler",
    "serverThreads"     : 31,
    "remote"            : False,
    "manual"            : False,
    "host"              : "127.0.0.1"
}


output = ""
output += "kwargs\n"
output += str(kwargs)
output += "\n"
output += "\n"
output += "Varying MTS 31 OLTP users on 31 threads\n"
output += "\n"

#schedulers = [
    #    "WSThreadLevelQueuesScheduler",
    #    "ThreadLevelQueuesScheduler",
    #    "CoreBoundQueuesScheduler",
    #    "WSCoreBoundQueuesScheduler",
    #    "WSThreadLevelPriorityQueuesScheduler",
    #    "ThreadLevelPriorityQueuesScheduler",
    #    "CoreBoundPriorityQueuesScheduler",
    #    "WSCoreBoundPriorityQueuesScheduler",
    #    "CentralScheduler",
    #    "CentralPriorityScheduler",
    #    "ThreadPerTaskScheduler",
    #    "DynamicPriorityScheduler",
    #   "DynamicScheduler",
    #    "NodeBoundQueuesScheduler",
    #    "WSNodeBoundQueuesScheduler",
    #    "NodeBoundPriorityQueuesScheduler",
    #    "WSNodeBoundPriorityQueuesScheduler"
#]

#output += "\n"
#output += "\n"
#kwargs["runtime"] = 120
#for scheduler in schedulers:
#    print "OLAP benchmark with " + scheduler
#    kwargs["scheduler"] = scheduler
#    output += runBenchmark_varying_users("Var_q3" + kwargs["scheduler"] + "_OLAP_" + str(kwargs["serverThreads"]), s1, **kwargs)
#
#runBenchmark_varying_users(groupId, numRuns, ...)
output += runBenchmark_scaling_curve_Scan("scalingcurve-scan", s1, **kwargs)
output += runBenchmark_scaling_curve_Join("scalingcurve-join", s1, **kwargs)
filename = "results_" + str(int(time.time()))
f = open(filename,'w')
f.write(output) # python will convert \n to os.linesep
f.close() # you can omit in most cases as the destructor will call if

print output
