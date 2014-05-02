import argparse
import benchmark
import os
import pprint
import time

from benchmark.benchmark import Benchmark
from benchmark.continuous_user import ContinuousUser
from benchmark.settings import Settings
from benchmark.operations_plotter import OperationsPlotter

def runbenchmarks(groupId, **kwargs):
    output = ""
    instances = [1, 16, 32, 64, 128, 1024]
    settings = Settings("standard_release")

    # Benchmark
    if not kwargs["evaluationOnly"]:
        for num_instances in instances:
            print "Starting benchmark with %i instances for probe" \
                % num_instances
            kwargs["userClass"] = ContinuousUser
            kwargs["prepareQueries"] = ("load_lineitem_orders", )
            kwargs["benchmarkQueries"] = ("join_lineitem_orders", )
            kwargs["userArgs"] = {"instances": num_instances}
            b = Benchmark(groupId, "num_instances_%d" % num_instances, settings, **kwargs)
            b.addQueryFile("load_lineitem_orders", "queries/pipelining/load_lineitem_orders.json")
            b.addQueryFile("join_lineitem_orders", "queries/pipelining/join_lineitem_orders.json")
            b.run()
    
    # Evaluation
    pl = OperationsPlotter(groupId)
    pl.plot_histograms()

    return output



aparser = argparse.ArgumentParser(description='Python benchmark for pipelining in Hyrise')
aparser.add_argument('--duration', default=20, type=int, metavar='D',
                     help='How long to run the benchmark in seconds')
aparser.add_argument('--evaluation-only', action='store_true',
                     help='Do not run the experiment and only evaluate the results of the last run')
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
aparser.add_argument('--rebuild', action='store_true', default=False,
                     help='Force `make clean` before each build')
aparser.add_argument('--regenerate', action='store_true',
                     help='Force regeneration of TPC-C table files')
aparser.add_argument('--perfdata', default=True, action='store_true',
                     help='Collect additional performance data. Slows down benchmark.')
aparser.add_argument('--json', default=False, action='store_true',
                     help='Use JSON queries instead of stored procedures.')
args = vars(aparser.parse_args())

kwargs = {
    "port"              : args["port"],
    "manual"            : args["manual"],
    "warmuptime"        : 20,
    "runtime"           : 4 * 60,
    "showStdout"        : True,
    "showStderr"        : args["stderr"],
    "rebuild"           : args["rebuild"],
    "regenerate"        : args["regenerate"],
    "noLoad"            : args["no_load"],
    "collectPerfData"   : args["perfdata"],
    "useJson"           : args["json"],
    "hyriseDBPath"      : "/home/Kai.Hoewelmeyer/hyrise-tpch/hyrise",
    "scheduler"         : "CentralScheduler",
    "serverThreads"     : 31,
    "remote"            : False,
    "remoteUser"        : "Kai.Hoewelmeyer",
    "evaluationOnly"    : args["evaluation_only"]
}

output = runbenchmarks("ophistogram", **kwargs)

filename = "results_" + str(int(time.time()))
f = open(filename,'w')
f.write(output) # python will convert \n to os.linesep
f.close() # you can omit in most cases as the destructor will call if
print output
