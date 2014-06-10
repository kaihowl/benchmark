import argparse
import benchmark
import time
import datetime
import sys

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
def runBenchmark_scaling_curve_scan(groupId, s1, numRuns=5, **kwargs):
    scan = (lambda x: x["op_name"] == "TableScan", "TableScan")
    kwargs["legendTitle"] = "Rows"
    return _scaling_curve(
            "queries/scaling-curves/scan.json",
            groupId,
            s1,
            numRuns,
            mean_tasks=[scan],
            fit_tasks=[scan + ("linear", )],
            **kwargs)

# Creates Figure 3 of DASFAA paper
def runBenchmark_scaling_curve_join(groupId, s1, numRuns=5, **kwargs):
    join = (lambda x: x["op_name"] == "NestedLoopEquiJoin", "NestedLoopEquiJoin")
    # TODO what to select? hash or probe? hash first or second?
    prefix = (lambda x: x["op_name"] == "PrefixSum", "PrefixSum")
    # TODO what to select? hash or probe?
    histogram = (lambda x: x["op_name"] == "Histogram", "Histogram")
    # TODO what to selet? hash or probe?
    cluster = (lambda x: x["op_name"] == "RadixCluster", "RadixCluster")
    kwargs["legendTitle"] = "Probe Rows"
    return _scaling_curve("queries/scaling-curves/join.json",
            groupId,
            s1,
            numRuns,
            mean_tasks=[join, prefix, histogram, cluster],
            fit_tasks=[join + ("quadratic", ), cluster + ("linear", )],
            **kwargs)

def _scaling_curve(mainQueryFile, groupId, s1, numRuns=5, mean_tasks=[], fit_tasks=[],**kwargs):
    """ Run a scaling curve benchmark.

        This is just a helper for scan and join scaling curves.

        Will output the mean response time for the provided query. For supplied
        lambdas in mean_tasks, plot the mean task size. For the supplied lambdas
        in fit_tasks, fit the a/x+b model to the mean task size.

        mainQueryFile -- path to the main query json file
        mean_tasks -- Sequence of pairs of lambdas and strings.
                      The lambdas select the tasks, the strings are used for
                      y axis labels
        fit_tasks --  Sequence of three-tuples of lambdas, strings, and either
                      "linear"/"quadratic" for the overall fitting.
                      The group of tasks selected by the lambda are fit with our
                      model. The strings are used for the y axis labels in the
                      single plots.
    """
    output = ""

    # TODO support full and narrow schema

    # This benchmark's users terminate themselves after a set number of runs
    kwargs["runtime"] = 0

    rows = [100*10**3, 1*10**6, 10*10**6, 100*10**6]
    # len(instances) == 35
    instances = [1] + range(2,32, 2)+ range(32, 64, 8) + range(64, 256, 32) + range(256,512,64) + range(512, 1025, 128)

    if not kwargs["evaluationOnly"]:
        for cur_rows in rows:
            for cur_instances in instances:
                print "scaling curve %s with %d rows and %d instances" % (groupId, cur_rows, cur_instances)
                kwargs["userClass"]        = RepeatingUser
                kwargs["numUsers"]         = 1
                kwargs["userArgs"]         = {
                        "repetitions"      : numRuns,
                        "instances"        : cur_instances,
                        # Capped instances are used in join.json
                        "capped_instances" : min(400, cur_instances),
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
    plotter.plot_true_response_time(dump_to_csv=True)
    plotter.plot_total_response_time(dump_to_csv=True)
    for (sel_lambda, name) in mean_tasks:
        plotter.plot_mean_task_size(sel_lambda, task_name=name, dump_to_csv=True)
    for (sel_lambda, name, fit_func_str) in fit_tasks:
        plotter.plot_fitting_for(sel_lambda, task_name=name)
    for (sel_lambda, name, fit_func_str) in fit_tasks:
        # Call add_tablesize_fitting_for() without other plots inbetween
        # Finalize plot with save_tablesize_fittings()
        plotter.add_tablesize_fitting_for(sel_lambda, name, fit_func_str)
    plotter.save_tablesize_fittings()
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

def runBenchmark_varying_users(groupId, s1, separateOLAPTables=False, **kwargs):
    output = ""

    kwargs["olapQueries"] = ("vldb_q10_instances", "vldb_q11_instances", "vldb_q12_instances")

    kwargs["separateOLAPTables"] = separateOLAPTables
    print "Using separateOLAPTables=%s" % str(kwargs["separateOLAPTables"])

    distincts = None

    # numbers chosen to match the original paper's datapoints
    if not kwargs["evaluationOnly"]:
        instances = [1, 8, 16, 31, 62, 124]
        users = [1, 2, 4, 5, 8, 10, 16, 20, 24, 30, 32, 40, 50, 60, 64]
        for i in instances:
            for j in users:
                if not distincts is None:
                    print "Reusing distincts from now on."
                    kwargs["distincts"] = distincts
                print "starting benchmark with " + str(i) + " instances and " + str(j) + " users"
                runId = str(i) + "_" + str(j)
                kwargs["olapInstances"] = i
                kwargs["olapUser"] = j
                kwargs["numUsers"] = kwargs["olapUser"]
                b1 = MixedWLBenchmark(groupId, runId, s1, **kwargs)
                b1.run()
                time.sleep(3)
                distincts = b1.getDistinctValues()

    plotter = DynamicPlotter(groupId)
    plotter.plot_throughput_per_run()
    plotter.plot_meansize_per_run()
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
    if kwargs["scheduler"] != "DynamicPriorityScheduler":
        print "\x1b[31;1mWARNING: Varying benchmark without DynamicPriorityScheduler?\x1b[0m"

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
    title = "Varying MTS with separateOLAPTables=%s" % str(separateOLAPTables)
    plotter.plotGroups(groupMapping, title=title)
    return output

aparser = argparse.ArgumentParser(description='Python implementation of the TPC-C Benchmark for HYRISE')
aparser.add_argument('benchmarks', metavar='benchmarks', type=str, nargs='+', help="Benchmarks to be run")
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

    "hyriseDBPath"      : "/home/hoewelmeyer/vldb-tables/scaler/output",
    # Set this value according to the data in hyriseDBPath: full/narrow
    "schema"            : "narrow",

    "separateOLAPTables": True,

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
BENCHMARK_START = 0
def start_timing():
    global BENCHMARK_START
    BENCHMARK_START = time.time()

def stop_timing(title="Time taken:"):
    global BENCHMARK_START
    delta_seconds = time.time() - BENCHMARK_START
    print "%s %s" % (title, str(datetime.timedelta(seconds=delta_seconds)))

filename = "results_" + str(int(time.time()))
f = open(filename,'w')

# Benchmarks method signatures have to start with this prefix. The only
# parameters they can take are the groupId, Settings, named parameters, and
# **kwargs.
BENCHMARK_PREFIX = "runBenchmark_"

# You can run benchmarks by supplying the name without the prefix and all
# underscores replaced by dashes.

# Pairs of names and benchmarks to run
run_list = [(b, BENCHMARK_PREFIX + b.replace("-", "_")) for b in args['benchmarks']]

for benchmark_name, method_name in run_list:
    if not method_name in locals():
        print "Could not find benchmark %s" % benchmark_name
        print "Possible benchmarks are:"
        for name in [i[len(BENCHMARK_PREFIX):].replace("_", "-") for i in locals().keys() if i.startswith(BENCHMARK_PREFIX)]:
            print "- %s" % name
        sys.exit("Unkown benchmark")

for benchmark_name, method_name in run_list:
    start_timing()
    output += locals()[method_name](benchmark_name, s1, **kwargs)
    stop_timing(title="%s took:" % benchmark_name)

f.write(output) # python will convert \n to os.linesep
f.close() # you can omit in most cases as the destructor will call if

print output
