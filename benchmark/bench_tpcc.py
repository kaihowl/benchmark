
import httplib
import logging
import os
import requests
import shutil
import signal
import subprocess
import sys

# include py-tpcc files
sys.path.insert(0, os.path.join(os.getcwd(), "benchmark", "bench-tpcc"))
from util import *
from runtime import *
import constants
import drivers
from tpcc import *

from benchmark import Benchmark
from user import User


# disable py-tpcc internal logging
logging.getLogger("requests").setLevel(logging.WARNING)

class TPCCUser(User):

    def __init__(self, userId, host, port, dirOutput, queryDict, **kwargs):
        User.__init__(self, userId, host, port, dirOutput, queryDict, **kwargs)

        self.scaleParameters = kwargs["scaleParameters"]
        self.config = kwargs["config"]
        self.config["reset"] = False
        self.config["execute"] = True
        self.perf = {}
        self.numErrors = 0
        self.onlyNeworders   = kwargs["onlyNeworders"] if kwargs.has_key("onlyNeworders") else False

    def prepareUser(self):
        """ executed once when user starts """
        self.driver = drivers.hyrisedriver.HyriseDriver("")
        self.driver.loadConfig(self.config)
        self.driver.conn = self
        self.context = None
        self.lastResult = None
        self.lastHeader = None
        self.e = executor.Executor(self.driver, self.scaleParameters)
        self.e.setOnlyNeworders(self.onlyNeworders)
        self.userStartTime = time.time()

    def runUser(self):
        """ main user activity """
        self.perf = {}
        txn, params = self.e.doOne()
        tStart = time.time()
        self.context = None
        try:
            self.driver.executeTransaction(txn, params, use_stored_procedure= not self._useJson)
        except requests.ConnectionError:
            self.numErrors += 1
            if self.numErrors > 5:
                print "*** TPCCUser %i: too many failed requests" % (self._userId)
                self.stopLogging()
                os.kill(os.getppid(), signal.SIGINT)
            return
        except RuntimeWarning, e:
            # these are transaction errors, e.g. abort due to concurrent commits
            tEnd = time.time()
            self.log("failed", [txn, tEnd-tStart, tStart-self.userStartTime])
            return
        except RuntimeError, e:
            print "%s: %s" % (txn, e)
            tEnd = time.time()
            self.log("failed", [txn, tEnd-tStart, tStart-self.userStartTime])
            return
        except AssertionError, e:
            return
        self.numErrors = 0
        tEnd = time.time()
        self.log("transactions", [txn, tEnd-tStart, tStart-self.userStartTime, self.perf])

    def stopUser(self):
        """ executed once after stop request was sent to user """
        pass

    def formatLog(self, key, value):
        if key == "transactions":
            logStr = "%s;%f;%f" % (value[0], value[1], value[2])
            for op, opData in value[3].iteritems():
                logStr += ";%s,%i,%f" % (op, opData["n"], opData["t"])
            logStr += "\n"
            return logStr
        elif key == "failed":
            return "%s;%f;%f\n" % (value[0], value[1], value[2])
        else:
            return "%s\n" % str(value)

    def addPerfData(self, perf):
        if perf:
            for op in perf:
                self.perf.setdefault(op["name"], {"n": 0, "t": 0.0})
                self.perf[op["name"]]["n"] += 1
                self.perf[op["name"]]["t"] += op["endTime"] - op["startTime"]

    # HyriseConnection stubs
    # ======================
    def stored_procedure(self, stored_procedure, querystr, paramlist=None, commit=False):
        if paramlist:
            for k,v in paramlist.iteritems():
                if v == True:    v = 1;
                elif v == False: v = 0;

        query_result = self.fireQuery(querystr, paramlist, sessionContext=self.context, autocommit=commit, stored_procedure=stored_procedure)
        if query_result != None:
            if query_result.status_code < 200 and query_result.status_code >= 300:
                print "ERROR:", query_result
                # result = query_result.json()
                # self.addPerfData(result.get("performanceData", None))
            return None
        else:
            return None

    def query(self, querystr, paramlist=None, commit=False):
        if paramlist:
            for k,v in paramlist.iteritems():
                if v == True:    v = 1;
                elif v == False: v = 0;

        result = self.fireQuery(querystr, paramlist, sessionContext=self.context, autocommit=commit).json()

        self.lastResult = result.get("rows", None)
        self.lastHeader = result.get("header", None)

        # check session context to make sure we are in the correct transaction
        new_session_context = result.get("session_context", None)
        if self.context != new_session_context:
            if self.context != None and new_session_context != None:
                raise RuntimeError("Session context was ignored by database")

        self.context = new_session_context
        self.addPerfData(result.get("performanceData", None))


    def commit(self):
        if not self.context:
            raise RuntimeError("Should not commit without running context")
        self.query("""{"operators": {"cm": {"type": "Commit"}}}""", commit=False)
        self.context = None

    def rollback(self):
        if not self.context:
            raise RuntimeError("Should not rollback without running context")
        result = self.fireQuery("""{"operators": {"rb": {"type": "Rollback"}}}""", sessionContext=self.context)
        self.context = None
        return result

    def runningTransactions(self):
        return self._session.get("http://%s:%s/status/tx" % (self._host, self._port)).json()

    def fetchone(self, column=None):
        if self.lastResult:
            r = self.lastResult.pop(0)
            return r[self.lastHeader.index(column)] if column else r
        return None

    def fetchone_as_dict(self):
        if self.lastResult:
            return dict(zip(self.lastHeader, self.lastResult.pop(0)))
        return None

    def fetchall(self):
        tmp = self.lastResult
        self.lastResult = None
        return tmp

    def fetchall_as_dict(self):
        if self.lastResult:
            r = [dict(zip(self.lastHeader, cur_res)) for cur_res in self.lastResult]
            self.lastResult = None
            return r
        return None


class TPCCBenchmark(Benchmark):

    def __init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs):
        Benchmark.__init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs)

        self.scalefactor     = kwargs["scalefactor"] if kwargs.has_key("scalefactor") else 1
        self.warehouses      = kwargs["warehouses"] if kwargs.has_key("warehouses") else 4
        self.driverClass     = createDriverClass("hyrise")
        self.driver          = self.driverClass(os.path.join(os.getcwd(), "pytpcc", "tpcc.sql"))
        self.scaleParameters = scaleparameters.makeWithScaleFactor(self.warehouses, self.scalefactor)
        self.regenerate      = False
        self.noLoad          = kwargs["noLoad"] if kwargs.has_key("noLoad") else False
        self.table_dir       = kwargs["tabledir"] if kwargs.has_key("tabledir") else None
        self.onlyNeworders   = kwargs["onlyNeworders"] if kwargs.has_key("onlyNeworders") else False
        self.setUserClass(TPCCUser)

    def generateTables(self, path):
        dirPyTPCC   = os.path.join(os.getcwd(), "pytpcc", "pytpcc")
        dirTables   = path
        
        if not os.path.exists(dirTables):
            os.makedirs(dirTables)
        
        rand.setNURand(nurand.makeForLoad())   
        sys.stdout.write("generating... ")
        sys.stdout.flush()
        self.driver.setTableLocation(dirTables)
        self.driver.deleteExistingTablefiles(dirTables)
        self.driver.createFilesWithHeader(dirTables)
        generator = loader.Loader(self.driver, self.scaleParameters, range(1,self.warehouses+1), True)
        generator.execute()
        print "done"

    def createBinaryTableExport(self, import_path, export_path):
        self._buildServer()
        self._startServer()

        sys.stdout.write("createBinaryTableExport... ")
        sys.stdout.flush()
        if not os.path.exists(export_path):
            os.makedirs(export_path)
        self.driver.executeLoadCSVExportBinary(import_path, export_path)
        print "done"


    def benchPrepare(self):
        # make sure the TPC-C query and table directories are present
        dirPyTPCC   = os.path.join(os.getcwd(), "pytpcc", "pytpcc")
        dirTables   = os.path.join(self._dirHyriseDB, "test", "tpcc", "tables")

        defaultConfig = self.driver.makeDefaultConfig()
        rand.setNURand(nurand.makeForLoad())
        config = dict(map(lambda x: (x, defaultConfig[x][1]), defaultConfig.keys()))
        config["querylog"] = None
        config["print_load"] = False
        config["port"] = self._port
        config["table_location"] = dirTables
        config["query_location"] = os.path.join("queries", "tpcc-queries")
        self.driver.loadConfig(config)        

        self.setUserArgs({
            "scaleParameters": self.scaleParameters,
            "config": config,
            "onlyNeworders": self.onlyNeworders
        })

    def loadTables(self):
        if self.noLoad:
            print "Skipping table load"
        else:
            sys.stdout.write("Importing tables into HYRISE... ")
            sys.stdout.flush()
            self.driver.executeStart(self.table_dir, use_csv = self._csv)
            print "done"
