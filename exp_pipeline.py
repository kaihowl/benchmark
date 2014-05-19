import argparse
import benchmark
import os
import pprint
import time
import sys

from benchmark.benchmark import Benchmark
from benchmark.continuous_user import ContinuousUser
from benchmark.settings import Settings
from benchmark.operations_plotter import OperationsPlotter

def run_join_histograms(**kwargs):
    groupId = "histograms"
    output = ""
    instances = [1, 4, 8, 16, 32]
    settings = Settings("standard_release")

    # Optional clean of folder
    if kwargs["cleanResultFolder"]:
        Benchmark.cleanResultFolder(groupId)

    # Benchmark
    if kwargs["runBenchmark"]:
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
    if kwargs["runEvaluation"]:
        pl = OperationsPlotter(groupId)
        pl.plot_histograms()

def run_scheduler_modality(**kwargs):
    groupId = "scheduler_modality"
    output = ""
    instances = [16]
    settings = Settings("standard_release")
    schedulers = ["CentralScheduler", "CoreBoundQueuesScheduler"]

    if kwargs["cleanResultFolder"]:
        Benchmark.cleanResultFolder(groupId)

    if kwargs["runBenchmark"]:
        for scheduler in schedulers:
            kwargs["scheduler"] = scheduler
            print "Starting benchmark for scheduler %s" % scheduler
            kwargs["userClass"] = ContinuousUser
            kwargs["prepareQueries"] = ("load_lineitem_orders", )
            kwargs["benchmarkQueries"] = ("join_lineitem_orders", )
            kwargs["userArgs"] = {"instances": 16}
            b = Benchmark(groupId, "scheduler_%s" % scheduler, settings, **kwargs)
            b.addQueryFile("load_lineitem_orders", "queries/pipelining/load_lineitem_orders.json")
            b.addQueryFile("join_lineitem_orders", "queries/pipelining/join_lineitem_orders.json")
            b.run()

    if kwargs["runEvaluation"]:
        pl = OperationsPlotter(groupId)
        pl.plot_histograms()

aparser = argparse.ArgumentParser(description='Python benchmark for pipelining in Hyrise')
aparser.add_argument('benchmarks', metavar='benchmarks', type=str, nargs='+', help="Benchmarks to be run")
aparser.add_argument('--duration', default=20, type=int, metavar='D',
                     help='How long to run the benchmark in seconds')
aparser.add_argument('--benchmark', action='store_true',
                     help='Run the benchmark part of the benchmark.')
aparser.add_argument('--evaluation', action='store_true',
                     help='Run the evaluation part of the benchmark.')
aparser.add_argument('--clean-result-folder', action='store_true',
                     help='Clean the folder of the benchmark before running it again.')
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
    "showStderr"        : True,
    "rebuild"           : args["rebuild"],
    "regenerate"        : args["regenerate"],
    "noLoad"            : args["no_load"],
    "collectPerfData"   : args["perfdata"],
    "useJson"           : args["json"],
    "hyriseDBPath"      : "/home/Kai.Hoewelmeyer/hyrise-tpch/hyrise",
    "scheduler"         : "CoreBoundQueuesScheduler",
    "serverThreads"     : 31,
    "remote"            : False,
    "remoteUser"        : "Kai.Hoewelmeyer",
    "runBenchmark"      : args["benchmark"],
    "runEvaluation"     : args["evaluation"],
    "cleanResultFolder" : args["clean_result_folder"]
}

method_names = ["run_%s" % b.replace("-", "_") for b in args['benchmarks']]
for i, method_name in enumerate(method_names):
    if not method_name in locals():
        print "Could not find benchmark %s" % args['benchmarks'][i]
        print "Possible benchmarks are:"
        for name in [i[4:].replace("_", "-") for i in locals().keys() if i.startswith("run_")]:
            print "- %s" % name
        sys.exit("Unkown benchmark")

for method_name in method_names:
    locals()[method_name](**kwargs)
