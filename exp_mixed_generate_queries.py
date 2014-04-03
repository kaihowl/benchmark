import os
import threading
import subprocess
import getpass
import benchmark
import argparse

from benchmark.bench_mixed import MixedWLBenchmark

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

kwargs = {
    "port"              : args["port"],
    "runtime"           : 100000,
    "showStdout"        : True,
    "showStderr"        : args["stderr"],
    "rebuild"           : args["rebuild"],
    "regenerate"        : args["regenerate"],
    "noLoad"            : args["no_load"],
    "serverThreads"     : args["threads"],
    "collectPerfData"   : args["perfdata"],
    "useJson"           : args["json"],
    "dirBinary"         : "/home/Johannes.Wust/hyrise/build/",
    "hyriseDBPath"      : "/home/Johannes.Wust/vldb-tables/scaler/output",
    "scheduler"         : "DynamicPriorityScheduler",
    "serverThreads"     : 12,
    "remote"            : False,
    "manual"            : False,
    "host"              : "127.0.0.1"
}


#kwargs["oltpQueries"] = ("q6a", "q6b", "q7", "q8", "q9")
#kwargs["oltpUser"] = 1
kwargs["olapQueries"] = ("q10i","q11i","q12i")
kwargs["olapUser"] = 1
kwargs["olapInstances"] = 12

def create_benchmark(name, settings_kwargs, groupId, parameters, benchmark_kwargs):
    runId = str(parameters).replace(",", "@")
    s = benchmark.Settings(name, **settings_kwargs)
    return MixedWLBenchmark(groupId, runId, s, **benchmark_kwargs) 

def create_benchmark_none(name, groupId, parameters, benchmark_kwargs):
    settings_kwargs = {"PERSISTENCY":"NONE"}
    return create_benchmark(name, settings_kwargs, groupId, parameters, benchmark_kwargs)


if args["genCount"] == None:
  print "Please specify number of queries to generate."
  exit(0)
if args["genFile"] == None:
  print "Please specify file to save the generated queries."
  exit(0)

args["genFile"] = os.path.abspath(args["genFile"])
print "Creating file with generated queries:", args["genFile"]
print "Create queries:", args["genCount"]

groupId = "mixed_" + args["genFile"]
num_clients = 1
runId = "numClients_%s" % num_clients
kwargs["numUsers"] = num_clients
kwargs["write_to_file"] = args["genFile"]
kwargs["write_to_file_count"] = args["genCount"]
parameters = {"none":None}    
b1 = create_benchmark_none("None", groupId, parameters, kwargs)
b1.run()