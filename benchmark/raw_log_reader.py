import os
import ujson
import pandas
import ast


class RawLogReader:

    """
    Reads raw mixed_workloads logs into Pandas DF with one row per operator.
    """

    def __init__(self, benchmark_group_id, head=None, verbose=False, **kwargs):
        """
            head -- specifies after how many rows of each user's file the parsing
            should stop. 'None' will read the entire file, any integer value
            represents the number of rows to be read from each file.

            verbose -- print progress on stdout during read-in of logs
        """
        self._head = head
        self._verbose=verbose
        self._group_id = benchmark_group_id
        self._data = self._collect()
        self._df = pandas.DataFrame(self._data)

    def get_data_frame(self):
        return self._df

    def _collect(self):
        """ Return a list of dictionaries with the following keys

        run
        build
        user
        query_name
        op_id, op_name, start, end, duration
        """
        data = list()
        dir_results = os.path.join(os.getcwd(), "results", self._group_id)
        if not os.path.isdir(dir_results):
            raise Exception(
                "Group result directory '%s' not found!" % dir_results)

        for str_run in os.listdir(dir_results):
            dirs = os.listdir(dir_results)
            if self._verbose:
                print "Reading run %d of %d" % (dirs.index(str_run), len(dirs))
            dir_run = os.path.join(dir_results, str_run)

            if not os.path.isdir(dir_run):
                continue

            for str_build in os.listdir(dir_run):
                dir_build = os.path.join(dir_run, str_build)
                if not os.path.isdir(dir_build):
                    continue

                for str_user in os.listdir(dir_build):
                    dir_user = os.path.join(dir_build, str_user)
                    if not os.path.isdir(dir_user):
                        continue

                    log_file_name = os.path.join(dir_user, "transactions.log")
                    if not os.path.isfile(log_file_name):
                        print "WARNING: no transactions log found in %s!" % dir_user
                        continue

                    for query_id, rawline in enumerate(open(log_file_name)):
                        if self._head and query_id >= self._head:
                            break
                        linedata = rawline.split(";")
                        query_name = linedata[0]
                        python_runtime = float(linedata[1])
                        startTime = float(linedata[2])
                        # convert python literal to json
                        # in order to use the faster ujson instead of ast
                        jsonData = linedata[3].replace("u'", '"').replace("'", '"')
                        try:
                            perf_data = ujson.loads(jsonData)
                        except ValueError:
                            # If integers outside range are involved
                            if self._verbose:
                                print "Falling back to slow ast parsing..."
                            perf_data = ast.literal_eval(linedata[3])

                        # add new item to main data list structure for every op
                        for op_data in perf_data:
                            dur  = op_data["endTime"] - op_data["startTime"]
                            data.append({
                                "run": str_run,
                                "build": str_build,
                                "user": str_user,
                                "query_id": query_id,
                                "query_name": query_name,
                                "python_runtime": python_runtime,
                                "startTime": startTime,
                                "op_id": op_data["id"],
                                "op_name": op_data["name"],
                                "start": op_data["startTime"],
                                "end": op_data["endTime"],
                                "duration": dur})

        return data
