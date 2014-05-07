import httplib
import json
import logging
import os
import requests
import shutil
import subprocess
import sys
import multiprocessing
import time
import random
import datetime

from benchmark import Benchmark
from user import User

from queries import *
import queries

# disable py-tpcc internal logging
logging.getLogger("requests").setLevel(logging.WARNING)

class MixedWLUser(User):

    def __init__(self, userId, host, port, dirOutput, queryDict, **kwargs):
        User.__init__(self, userId, host, port, dirOutput, queryDict, **kwargs)

#        self.scaleParameters = kwargs["scaleParameters"]
        self._benchmarkQueries = kwargs["queries"] if kwargs.has_key("queries") else list()
            
       # print self._benchmarkQueries

        if kwargs.has_key("distincts"):
          self._distincts = kwargs["distincts"]
        else:
          raise RuntimeError("No distinct values for query placeholders supplied.")

        self._core = kwargs["core"] if kwargs.has_key("core") else 1
        self._prio = kwargs["prio"] if kwargs.has_key("prio") else 2
        # default assumes hyperthreading count of 2
        self._instances = kwargs["instances"] if kwargs.has_key("instances") else multiprocessing.cpu_count()/2
        self._microsecs = kwargs["microsecs"] if kwargs.has_key("microsecs") else 1000
        self._db = kwargs["db"] if kwargs.has_key("db") else "cbtr"
        self._tableSuffix = kwargs["tableSuffix"] if kwargs.has_key("tableSuffix") else ""

        self._oltpQC = {}
        for q in OLTP_QUERY_FILES:
            self._oltpQC[q] = open(OLTP_QUERY_FILES[q], "r").read()

        self._olapQC = {}
        for q in OLAP_QUERY_FILES:
            self._olapQC[q] = open(OLAP_QUERY_FILES[q], "r").read()
        self.perf = {}

    def prepareUser(self):
        self.userStartTime = time.time()

    def runUser(self):
        """ main user activity """
        # choose query from _queries
        self.perf = {}
        query = ''
        current_query = self._totalRuns % len(self._benchmarkQueries)
        tStart = time.time()
        element = self._benchmarkQueries[current_query]

        # Execute all queries in order                                                                                                                                                           
        if reduce(lambda i,q: True if q[0] == element or i == True else False, OLTP_WEIGHTS, False):
            result = self.oltp(element)
        else:
            result = self.olap(element)

        #result = self.fireQuery(querystr, paramlist, sessionContext=self.context, autocommit=commit, stored_procedure=stored_procedure).json()
        if not self._write_to_file:
            if not self._stopevent.is_set():
                #print "user " + str(self._userId) + " logged!"
                tEnd = time.time()
                self.addPerfData(result.get("performanceData", None))
                self.log("transactions", [element, tEnd-tStart, tStart-self.userStartTime, self.perf])
            else: 
                pass

    def oltp(self, predefined=None):
        if predefined == None:
            queryid = self.weighted_choice(OLTP_WEIGHTS)
        else:
            queryid = predefined

        initFormatDict = {
            "papi": self._papi,
            "core": str(self._core),
            "db": self._db,
            "tableSuffix": self._tableSuffix,
            "sessionId": str(self._userId),
            "priority": str(self._prio),
            "microsecs": str(self._microsecs)}
        randFormatDict = self.getQueryFormatDict()
        
        query = self._oltpQC[queryid] % dict(initFormatDict.items() + randFormatDict.items())
        if self._write_to_file:
            result = self.fireQuery(query)
        else:
            resp = self.fireQuery(query)
            resp.encoding = 'ISO-8859-1'
            result = resp.json()
        #  self._queries[queryid] += 1
        #self._queryRowsFile.write("%s %d\n" % (queryid,  len(result[0]["rows"])))
        #return result[1]
        return result

    def olap(self, predefined=None):
        if predefined == None:
            queryid = self.weighted_choice(OLAP_WEIGHTS)
        else:
            queryid = predefined

        initFormatDict = {
            'papi': self._papi,
            "db": self._db,
            "tableSuffix": self._tableSuffix,
            "instances": self._instances,
            "sessionId": str(self._userId),
            "priority": str(self._prio),
            "microsecs": str(self._microsecs)}
        randFormatDict = self.getQueryFormatDict()
        query = self._olapQC[queryid] % dict(initFormatDict.items() + randFormatDict.items())
        if self._write_to_file:
            result = self.fireQuery(query)
        else:
            resp = self.fireQuery(query)
            resp.encoding = 'ISO-8859-1'
            result = resp.json()
        return result

        #self._queries[queryid] += 1
        #self._queryRowsFile.write("%s %d\n" % (queryid, len(result[0]["rows"])))                                                                                                                           
        #return result[1]


    def stopUser(self):
        """ executed once after stop request was sent to user """
        pass

    def formatLog(self, key, value):
        logStr = "%s;%f;%f;%s" % (value[0], value[1], value[2], value[3])
       # for op, opData in value[3].iteritems():
       #     logStr += ";%s,%i,%f" % (op, opData["n"], opData["t"])
        logStr += "\n"
        return logStr

    def addPerfData(self, perf):
        if perf:
          self.perf = perf
     #      for op in perf:
     #          self.perf.setdefault(op["name"], {"n": 0, "t": 0.0})
     #          self.perf[op["name"]]["n"] += 1
     #          self.perf[op["name"]]["t"] += op["endTime"] - op["startTime"]


    #def stats(self):
        #""" Print some execution statistics about the User """

        #print "Overall tp/%ds (mean): %f" % (self._interval, numpy.array(self._throughputAll).mean())
        #print "Overall tp/%ds (median): %f" % (self._interval, numpy.median(numpy.array(self._throughputAll)))

        ## Mean over all queries                                                                                                                                                                             
        #all_mean = numpy.array(self._throughputAll).mean()
        #all_median = numpy.median(numpy.array(self._throughputAll))

        ## Build the ordered list of all queryids                                                                                                                                                            
        #query_ids = map(lambda k: k[0], OLTP_WEIGHTS) + map(lambda k: k[0], OLAP_WEIGHTS)

    def getQueryFormatDict(self):
        unescaped_dict = {
          'rand_vbeln': "".join([str(random.choice(range(0,9))) for i in range(0,10)]),  # random 10 digit vbeln
          'rand_date': int(random.choice([datetime.datetime.today()-datetime.timedelta(days=x) for x in range(0, 360)]).strftime("%Y%m%d")), # random date within today-360 days
          'rand_kunnr_vbak': random.choice(self._distincts['distinct-kunnr-vbak'])[0],
          'rand_matnr_vbap': random.choice(self._distincts['distinct-matnr-vbap'])[0],
          'rand_matnr_vbap_2': random.choice(self._distincts['distinct-matnr-vbap'])[0],
          'rand_kunnr_kna1': random.choice(self._distincts['distinct-kunnr-kna1'])[0],
          'rand_addrnumber_adrc': random.choice(self._distincts['distinct-addrnumber-adrc'])[0],
          'rand_name1_adrc': random.choice(self._distincts['distinct-name1-adrc'])[0],
          'rand_matnr_makt': random.choice(self._distincts['distinct-matnr-makt'])[0],
          'rand_matnr_mara': random.choice(self._distincts['distinct-matnr-mara'])[0],
          'rand_vbeln_vbap': random.choice(self._distincts['distinct-vbeln-vbap'])[0],
          'rand_vbeln_vbak': random.choice(self._distincts['distinct-vbeln-vbak'])[0],
          'rand_netwr': random.normalvariate(100,5),
          'rand_kwmeng': random.normalvariate(100,5)
        }
        format_dict = {}
        for key, value in unescaped_dict.iteritems():
            format_dict[key] = json.dumps(value)
        return format_dict

    @staticmethod
    def weighted_choice(choices_and_weights):
        ''' method used for weighted choice of queries according to its input '''
        totals = []
        running_total = 0

        for c, w in choices_and_weights:
            running_total += w
            totals.append(running_total)

        rnd = random.random() * running_total

        for i, total in enumerate(totals):
            if rnd < total:
                return choices_and_weights[i][0]


class MixedWLBenchmark(Benchmark):

    def __init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs):
        Benchmark.__init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs)

        #self._dirHyriseDB = os.path.join(os.getcwd(), "hyrise")
        os.environ['HYRISE_DB_PATH'] = self._dirHyriseDB

        self._distincts = kwargs["distincts"] if kwargs.has_key("distincts") else None

        self._olapUser = kwargs["olapUser"] if kwargs.has_key("olapUser") else 0
        self._olapQueries = kwargs["olapQueries"] if kwargs.has_key("olapQueries") else ()
        self._olapThinkTime = kwargs["olapThinkTime"] if kwargs.has_key("olapThinkTime") else 0
        self._olapInstances = kwargs["olapInstances"] if kwargs.has_key("olapInstances") else 1

        self._tolapUser = kwargs["tolapUser"] if kwargs.has_key("tolapUser") else 0
        self._tolapQueries = kwargs["tolapQueries"] if kwargs.has_key("tolapQueries") else ()
        self._tolapThinkTime = kwargs["tolapThinkTime"] if kwargs.has_key("tolapThinkTime") else 0
        
        self._oltpUser = kwargs["oltpUser"] if kwargs.has_key("oltpUser") else 0
        self._oltpQueries = kwargs["oltpQueries"] if kwargs.has_key("oltpQueries") else ()
        self._oltpThinkTime = kwargs["oltpThinkTime"] if kwargs.has_key("oltpThinkTime") else 0

        self.setUserClass(MixedWLUser)
        self._queryDict = self.loadQueryDict()

        self._seperateOLAPTables = kwargs["separateOLAPTables"] if kwargs.has_key("separateOLAPTables") else False

    def _createPreloadArgs(self, num_users=0):
        vertices_template = """
           "loadvbak%(num)d" : {
             "type" : "LoadDumpedTable",
             "name" : "vbak"
           },
           "setvbak%(num)d" : {
             "type" : "SetTable",
             "name" : "vbak_%(num)d"
           },
           "loadvbap%(num)d" : {
             "type" : "LoadDumpedTable",
             "name" : "vbap"
           },
           "setvbap%(num)d" : {
             "type" : "SetTable",
             "name" : "vbap_%(num)d"
           },
        """
        edges_template = """
        ["loadvbak%(num)d", "setvbak%(num)d"],
        ["loadvbap%(num)d", "setvbap%(num)d"],
        ["setvbap%(num)d", "nop"],
        ["setvbak%(num)d", "nop"],
        """
        preload_additional_vertices = "".join([vertices_template % {"num": i} for i in
          range(num_users)])
        preload_additional_edges = "".join([edges_template % {"num": i} for i in
          range(num_users)])

        return {
            "preload_additional_vertices": preload_additional_vertices,
            "preload_additional_edges": preload_additional_edges}

    def benchPrepare(self):
        # Preload separate tables for OLAP users
        if self._seperateOLAPTables:
            self._tableLoadArgs = self._createPreloadArgs(self._olapUser)
        else:
            self._tableLoadArgs = {"preload_additional_edges": "", "preload_additional_vertices": ""}

    def benchAfterLoad(self):
        if self._distincts is None:
            self.initDistinctValues()

    def _createUsers(self):
        self._userArgs["distincts"] = self._distincts
        for i in range(self._olapUser):
            self._userArgs["thinkTime"] = self._olapThinkTime 
            self._userArgs["queries"] = self._olapQueries
            self._userArgs["instances"] = self._olapInstances
            self._userArgs["tableSuffix"] = "_%d" % i if self._seperateOLAPTables else ""
            self._users.append(self._userClass(userId=i, host=self._host, port=self._port, dirOutput=self._dirResults, queryDict=self._queryDict, collectPerfData=self._collectPerfData, useJson=self._useJson, write_to_file=self._write_to_file, write_to_file_count=self._write_to_file_count, **self._userArgs))
        for i in range(self._olapUser, self._olapUser + self._tolapUser):
            self._userArgs["thinkTime"] = self._tolapThinkTime 
            self._userArgs["queries"] = self._tolapQueries 
            self._users.append(self._userClass(userId=i, host=self._host, port=self._port, dirOutput=self._dirResults, queryDict=self._queryDict, collectPerfData=self._collectPerfData, useJson=self._useJson, write_to_file=self._write_to_file, write_to_file_count=self._write_to_file_count, **self._userArgs))
        for i in range(self._olapUser + self._tolapUser, self._olapUser + self._tolapUser + self._oltpUser):
            self._userArgs["thinkTime"] = self._oltpThinkTime 
            self._userArgs["queries"] = self._oltpQueries
            self._users.append(self._userClass(userId=i, host=self._host, port=self._port, dirOutput=self._dirResults, queryDict=self._queryDict, collectPerfData=self._collectPerfData, useJson=self._useJson, write_to_file=self._write_to_file, write_to_file_count=self._write_to_file_count, **self._userArgs))
        if (self._olapUser + self._oltpUser + self._tolapUser) == 0:
            for i in range(self._numUsers):
                self._users.append(self._userClass(userId=i, host=self._host, port=self._port, dirOutput=self._dirResults, queryDict=self._queryDict, collectPerfData=self._collectPerfData, useJson=self._useJson, write_to_file=self._write_to_file, write_to_file_count=self._write_to_file_count, **self._userArgs))

    def initDistinctValues(self):
        self._distincts = {}
        status_string = "beginning preparation of placeholder distinct values ..."
        sys.stdout.write(status_string+"\r")
        sys.stdout.flush()
        num_prep = 0
        query_pairs = [(k, open(v, "r").read()) for (k,v) in PREPARE_DISTINCTS_SERVER.iteritems()]
        resp_pairs = [(query_pairs[i][0], resp) for i, resp in enumerate(self._fireQueryParallel([v for (k,v) in query_pairs]))]
        for queryName, resp in resp_pairs:
          data = json.loads(resp.text.encode('utf-8'))
          if "rows" in data:
            self._distincts[queryName] = data["rows"]
        print "finished prepare for placeholders ...                                            "

    def getDistinctValues(self):
        return self._distincts

    def loadQueryDict(self):
        queryDict = {}
        # read LOAD queries
        for q in TABLE_LOAD_QUERIES_SERVER:
            queryDict[q] = open(TABLE_LOAD_QUERIES_SERVER[q], "r").read()
        # read PREPARE queries
        for q in PREPARE_QUERIES_USER:
            queryDict[q] = open(PREPARE_QUERIES_USER[q], "r").read()
        for q in PREPARE_QUERIES_SERVER:
            queryDict[q] = open(PREPARE_QUERIES_SERVER[q], "r").read()
        # read OLTP queries
        for q in OLTP_QUERY_FILES:
            queryDict[q] = open(OLTP_QUERY_FILES[q], "r").read()
        # read OLAP queries
        for q in OLAP_QUERY_FILES:
            queryDict[q] = open(OLAP_QUERY_FILES[q], "r").read()
        return queryDict
