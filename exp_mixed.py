import argparse
import benchmark
import os
import pprint
import time

from benchmark.bench_mixed import MixedWLBenchmark
from benchmark.mixedWLPlotter import MixedWLPlotter

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


def runBenchmark_varying_users(groupId, s1, **kwargs):
    output = ""
    #users = [1, 2, 4, 8, 16]#, 24, 32]#, 48, 64, 96, 128]
    
    kwargs["olapQueries"] = ("q10","q11","q12")
    kwargs["tolapUser"] = 1
    kwargs["tolapThinkTime"] = 1
    kwargs["tolapQueries"] = ("xselling",)
    kwargs["oltpUser"] = 1
    kwargs["oltpQueries"] = ("q7idx_vbak","q8idx_vbap")

    instances = [1, 12]
    users = [1, 12]
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
    output += plotter.printFormattedStatistics(kwargs["oltpQueries"]+kwargs["tolapQueries"] +kwargs["olapQueries"])
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


def runBenchmark_varying_mts(groupId, s1, **kwargs):
    output = ""

    kwargs["oltpQueries"] = ("q6a", "q6b", "q7", "q8", "q9")
    kwargs["oltpUser"] = 1
    kwargs["tolapQueries"] = ("xselling",)
    # TODO why was that 0?
    kwargs["tolapUser"] = 1
    # TODO is this in seconds?
    kwargs["tolapThinkTime"] = 1
    kwargs["olapQueries"] = ("q10", "q11", "q12")
    kwargs["olapUser"] = 32

    mts_list = [30, 50, 200, 300]
    for mts in mts_list:
        print "starting benchmark with mts=" + str(mts)
        runId = str(mts)        
        kwargs["mts"] = mts
        kwargs["numUsers"] = kwargs["olapUser"] + kwargs["oltpUser"] + kwargs["tolapUser"]
        b1 = MixedWLBenchmark(groupId, runId, s1, **kwargs)
        b1.run()
        time.sleep(5)
    plotter = MixedWLPlotter(groupId)
    output += groupId + "\n"
    output += plotter.printStatistics(kwargs["oltpQueries"]+kwargs["tolapQueries"] +kwargs["olapQueries"])
   # output += plotter.printOpStatistics ()
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
args = vars(aparser.parse_args())

s1 = benchmark.Settings("Standard", PERSISTENCY="NONE", COMPILER="autog++")



#gaza remote
#kwargs = {
#    "port"              : args["port"],
#    "manual"            : True,
#    "warmuptime"        : 10,
#    "runtime"           : 180,
#    "prepareQueries"    : ("preload_cbtr", "create_vbak_index","create_vbap_index"),
#    "showStdout"        : True,
#    "showStderr"        : args["stderr"],
#    "rebuild"           : args["rebuild"],
#    "regenerate"        : args["regenerate"],
#    "noLoad"            : args["no_load"],
#    "serverThreads"     : args["threads"],
#    "collectPerfData"   : args["perfdata"],
#    "useJson"           : args["json"],
#    "dirBinary"         : "/home/Johannes.Wust/hyrise/build/",
#    "hyriseDBPath"      : "/home/Johannes.Wust/hyrise/test/",
#    "scheduler"         : "CentralScheduler",
#    "serverThreads"     : 32,
#    "remote"            : True,
#    "remoteUser"        : "Johannes.Wust",
#    "host"              : "gaza"
#}

#gaza local
kwargs = {
    "port"              : args["port"],
    "warmuptime"        : 20,
    "runtime"           : 120,
    "prepareQueries"    : ("preload",),
    "showStdout"        : False,
    "showStderr"        : args["stderr"],
    "rebuild"           : args["rebuild"],
    "regenerate"        : args["regenerate"],
    "noLoad"            : args["no_load"],
    "serverThreads"     : args["threads"],
    "collectPerfData"   : args["perfdata"],
    "useJson"           : args["json"],
    "dirBinary"         : "/home/Kai.Hoewelmeyer/hyrise/build/",
    "hyriseDBPath"      : "/home/Kai.Hoewelmeyer/vldb-tables/scaler/output",
    "scheduler"         : "DynamicPriorityScheduler",
    "serverThreads"     : 31,
    "remote"            : False,
    "manual"            : False,
    "host"              : "127.0.0.1"
}

##begram local
#kwargs = {
#    "port"              : args["port"],
#    "warmuptime"        : 1,
#    "runtime"           : 10,
#    "prepareQueries"    : ("preload_cbtr_small", "create_vbak_index","create_vbap_index"),
#    "showStdout"        : False,
#    "showStderr"        : args["stderr"],
#    "rebuild"           : args["rebuild"],
#    "regenerate"        : args["regenerate"],
#    "noLoad"            : args["no_load"],
#    "serverThreads"     : args["threads"],
#    "collectPerfData"   : args["perfdata"],
#    "useJson"           : args["json"],
#    "manual"            : False,
#    "dirBinary"         : "/home/Johannes.Wust/hyrise/build/",
#    "hyriseDBPath"      : "/home/Johannes.Wust/hyrise/test/",
#    "scheduler"         : "CoreBoundQueuesScheduler",
#    "serverThreads"     : 12,
#    "remote"            : False,
#    "host"              : "127.0.0.1"
#}


output = ""
output += "kwargs\n"
output += str(kwargs)
output += "\n"
output += "\n"
output += "Varying MTS 31 OLTP users on 31 threads\n"
output += "\n"

#schedulers = [
#         #"WSThreadLevelQueuesScheduler",
#         #"ThreadLevelQueuesScheduler",
#         #"CoreBoundQueuesScheduler",
#         #"WSCoreBoundQueuesScheduler",
#         #"WSThreadLevelPriorityQueuesScheduler",
#         #"ThreadLevelPriorityQueuesScheduler",
#         #"CoreBoundPriorityQueuesScheduler",
#         #"WSCoreBoundPriorityQueuesScheduler",
#         "CentralScheduler"
#         #"CentralPriorityScheduler",
#         #"ThreadPerTaskScheduler",
#         ###"DynamicPriorityScheduler",
#         ###"DynamicScheduler",
#         #"NodeBoundQueuesScheduler",
#         #"WSNodeBoundQueuesScheduler",
#         #"NodeBoundPriorityQueuesScheduler",
#         #"WSNodeBoundPriorityQueuesScheduler"
#         ]

#for scheduler in schedulers:
#    print "OLTP benchmark with " + scheduler 
#    kwargs["scheduler"] = scheduler
#    output += runBenchmark_varying_users("Var_q3" + kwargs["scheduler"] + "_OLAP_" + str(kwargs["serverThreads"]), s1, **kwargs)
#
output += runBenchmark_varying_mts("Var_mts", s1, **kwargs)
filename = "results_" + str(int(time.time()))
f = open(filename,'w')
f.write(output) # python will convert \n to os.linesep
f.close() # you can omit in most cases as the destructor will call if

print output
