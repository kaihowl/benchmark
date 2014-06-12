from user import User

# Repeats queries for a fixed number of runs
# The number is supplied in userArgs["repetitions"]
class RepeatingUser(User):

    def __init__(self, userId, host, port, dirOutput, queryDict, **kwargs):
        User.__init__(self, userId, host, port, dirOutput, queryDict, **kwargs)
        self._repetitions = kwargs["repetitions"] if kwargs.has_key("repetitions") else 1

    def runUser(self):
        if self._totalRuns >= self._repetitions:
            self._stopevent.set()
            return
        cur_query = self._totalRuns % len(self._queries)
        query_name = self._queries[cur_query]
        query = self._queryDict[query_name] % self._userArgs
        response = self.fireQuery(query)
        response.encoding = 'ISO-8859-1'
        json_result = response.json()
        perf = json_result.get("performanceData", None)
        self.log("queries", [query_name,  perf])

    def formatLog(self, key, value):
        return "%s;%s\n" % (value[0], value[1])
