from user import User

class ContinuousUser(User):

    def runUser(self):
        cur_query = self._totalRuns % len(self._queries)
        query_name = self._queries[cur_query]
        query = self._queryDict[query_name] % self._userArgs
        result = self.fireQuery(query).json()
        perf = result.get("performanceData", None)
        if not self._stopevent.is_set():
            self.log("queries", [query_name,  perf])

    def formatLog(self, key, value):
        return "%s;%s\n" % (value[0], value[1])