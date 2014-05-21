import os
import threading
import subprocess
import getpass
import benchmark
import argparse
from benchmark.mixedWLPlotter import MixedWLPlotter
from benchmark.bench_mixed import MixedWLBenchmark
import argparse
import benchmark
import os
import getpass
import shutil
import commands
import copy
import time

def clear_dir(path):
    print "Clearing directory:", path
    if not os.path.exists(path):
        return
    for root, dirs, files in os.walk(path):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))

def clear_file(filename):
    if os.path.isfile(filename):
        os.remove(filename)
        print "Deleted file:", filename
        
aparser = argparse.ArgumentParser(description='Python implementation of the TPC-C Benchmark for HYRISE')
aparser.add_argument('--scalefactor', default=1, type=float, metavar='SF',
                     help='Benchmark scale factor')
aparser.add_argument('--warehouses', default=1, type=int, metavar='W',
                     help='Number of Warehouses')
aparser.add_argument('--duration', default=20, type=int, metavar='D',
                     help='How long to run the benchmark in seconds')
aparser.add_argument('--clients', default=-1, type=int, metavar='N',
                     help='The number of blocking clients to fork (note: this overrides --clients-min/--clients-max')
aparser.add_argument('--clients-min', default=1, type=int, metavar='N',
                     help='The minimum number of blocking clients to fork')
aparser.add_argument('--clients-max', default=1, type=int, metavar='N',
                     help='The maximum number of blocking clients to fork')
aparser.add_argument('--clients-step', default=1, type=int, metavar='N',
                     help='The step-width for the number of clients to fork')
aparser.add_argument('--no-load', action='store_true',
                     help='Disable loading the data')
aparser.add_argument('--no-execute', action='store_true',
                     help='Disable executing the workload')
aparser.add_argument('--host', default="localhost", type=str, metavar="H",
                     help='IP on which HYRISE should be run remotely')
aparser.add_argument('--remoteUser', default=getpass.getuser(), type=str, metavar="R",
                     help='remote User for remote host on which HYRISE should be run remotely')
aparser.add_argument('--remotePath', default="/home/" + getpass.getuser() +"/benchmark", type=str,
                     help='path of benchmark folder on remote host')
aparser.add_argument('--port', default=5001, type=int, metavar="P",
                     help='Port on which HYRISE should be run')
aparser.add_argument('--threads', default=0, type=int, metavar="T",
                     help='Number of server threadsto use')
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
aparser.add_argument('--perfdata', default=False, action='store_true',
                     help='Collect additional performance data. Slows down benchmark.')
aparser.add_argument('--json', default=False, action='store_true',
                     help='Use JSON queries instead of stored procedures.')
aparser.add_argument('--ab', default=None,
                     help='Queryfile with prepared requests. If specified ab tool is used to fire queries.')
aparser.add_argument('--verbose', default=1,
                     help='Verbose output level. Default is 1. Set to 0 if nothing should be printed.')
aparser.add_argument('--abCore', default=2,
                     help='Core to bind ab to.')
aparser.add_argument('--tabledir', default=None, type=str, metavar="T",
                     help='Directory for TPCC tables to use.')
aparser.add_argument('--genCount', default=None, type=str,
                     help='Number of queries to generate')
aparser.add_argument('--genFile', default=None, type=str, metavar="T",
                     help='File to store generated queries')
aparser.add_argument('--onlyNeworders', default=False, action='store_true',
                     help='Only do new-order transactions. Otherwise full mix of tpcc transactions is executed/generated.')
aparser.add_argument('--csv', default=False, action='store_true',
                     help='Load data from csv files and do not user binary import.')
aparser.add_argument('--vtune', default=None, type=str,
                     help='Automatically resume running vTune session once load is complete and stop when benchmark is done (implies --manual) - give vTune project folder (e.g. ~/intel/amplxe/projects/hyrise/) - assumes vTune environment is set (i.e., amplxe-cl exists)')
aparser.add_argument('--coreoffset', default=None, type=str,
                     help='Core Offset for Hyrise Worker threads.')

args = vars(aparser.parse_args())


def run_benchmark(name, settings, groupId, benchmark_kwargs, n):
    for i in range(n): 
        runId = str(kwargs["scheduler"]) + "_" + str(kwargs["numUsers"]) + "_" + str(i) 
        s = settings
        MixedWLBenchmark(groupId, runId, s, **benchmark_kwargs).run()

def run_benchmark_none(name, groupId, benchmark_kwargs, n):
    s1 = benchmark.Settings("Standard", PERSISTENCY="NONE", COMPILER="autog++")
    return run_benchmark(name, s1, groupId, benchmark_kwargs, n)



kwargs = {
    "port"              : args["port"],
    "runtime"           : 20,
    "showStdout"        : False,
    "showStderr"        : args["stderr"],
    "rebuild"           : args["rebuild"],
    "regenerate"        : args["regenerate"],
    "noLoad"            : args["no_load"],
    "serverThreads"     : args["threads"],
    "collectPerfData"   : args["perfdata"],
    "useJson"           : args["json"],
   # "dirBinary"         : "/home/Johannes.Wust/hyrise/build/",
    "hyriseDBPath"      : "/home/Johannes.Wust/vldb-tables/scaler/output",
    "scheduler"         : "CentralScheduler",
    "serverThreads"     : 11,
    "remote"            : False,
    "manual"            : False,
    "host"              : "127.0.0.1"
}

groupId = "ab_nop_throughput_tmp"
kwargs["abQueryFile2"] = "olapqueries.txt"
kwargs["abQueryFile"] = "oltpqueries.txt"
kwargs["abCore"] = 0

output=""

schedulers = [
   #    "WSThreadLevelQueuesScheduler",
   # "ThreadLevelQueuesScheduler",
   #    "CoreBoundQueuesScheduler",
   #    "WSCoreBoundQueuesScheduler",
   #    "WSThreadLevelPriorityQueuesScheduler",
   #    "ThreadLevelPriorityQueuesScheduler",
   #    "CoreBoundPriorityQueuesScheduler",
   #    "WSCoreBoundPriorityQueuesScheduler",
   # "CentralScheduler",
       "CentralPriorityScheduler",
    #   "ThreadPerTaskScheduler",
   #    "DynamicPriorityScheduler",
   #   "DynamicScheduler",
   # "NodeBoundQueuesScheduler",
      #   "WSNodeBoundQueuesScheduler",
   #     "NodeBoundPriorityQueuesScheduler",
    #    "WSNodeBoundPriorityQueuesScheduler"
]

users = [4, 8, 11, 12, 20]
for scheduler in schedulers:
    kwargs["scheduler"] = scheduler
    for user in users:
        kwargs["numUsers"] = user
        run_benchmark_none("None", groupId, kwargs, 1)
    

output = "\n\n <<<<<<<< Output >>>>>>>\n"
plotter = MixedWLPlotter(groupId, True)
output += str(plotter.printABStatistics())

filename = "results_" + str(int(time.time()))
f = open(filename,'w')
f.write(output) # python will convert \n to os.linesep
f.close() # you can omit in most cases as the destructor will call if


#print output




