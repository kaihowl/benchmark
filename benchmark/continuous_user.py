from user import User

class ContinuousUser(User):

    def runUser(self):
        cur_query = self._totalRuns % len(self._queryDict)
        cur_query_key = self._queryDict.keys()[cur_query]
        query = self._queryDict[cur_query_key]
        print cur_query_key
        result = self.fireQuery(query).json()
        perf = result.get("performanceData", None)
        self.log("queries", [cur_query,  perf])

    def formatLog(self, key, value):
        return "%s;%s\n" % (value[0], value[1])
