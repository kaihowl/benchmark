import ujson
import os
import sys
import pandas
from natsort import natsorted
import numpy as np
import time


class QueryTablePlotter:

    def __init__(self, benchmarkGroupId):
        self._groupId = benchmarkGroupId
        self._df = pandas.DataFrame(self._collect())

    def print_query_table(self):
        # filter out one run per queryName and instances combination
        runs = list(self._df.groupby(['instances', 'queryId'])['runningNum'].max())
        criterion = self._df.apply(lambda x: x["runningNum"] in runs, axis=1)
        single_runs = self._df[criterion]

        # filter out helper tasks
        def is_helper_task(item):
            helper_tasks = ["NoOp",
                            "RequestParseTask",
                            "ResponseTask",
                            "GetTable"]
            return item["opName"] in helper_tasks
        criterion = single_runs.apply(is_helper_task, axis=1)
        filtered_runs = single_runs[~criterion]

        groups = filtered_runs.groupby(["queryId", "instances"])["opName"].count()
        columns = groups.unstack(level="instances")

        var_ops_per_query = (columns[16] - columns[8]) / 8

        fix_ops_per_query = columns[1]

        abs_ops_per_query = columns

        # startTime of ResponseTask - endTime of QueryParseTask
        def get_true_resp_time(data):
            response_tasks = data[data["opName"] == "ResponseTask"]
            assert(len(response_tasks) == 1)

            parse_tasks = data[data["opName"] == "RequestParseTask"]
            assert(len(parse_tasks) == 1)

            response_start = response_tasks.iloc[0]["opStartTime"]
            parse_end = parse_tasks.iloc[0]["opEndTime"]
            return response_start - parse_end

        group = self._df.groupby(["queryId", "instances", "runningNum"])
        true_durations = group.apply(get_true_resp_time).reset_index(level="runningNum", drop=True)
        result = true_durations.groupby(level=["queryId", "instances"]).mean().unstack(level="instances")

        format_rows = []

        for query_name in natsorted(list(result.index)):
            data = result.loc[query_name]
            if np.isnan(var_ops_per_query[query_name]):  # constant size
                instances_formula = "%d" % fix_ops_per_query[query_name]
            else:
                slope = var_ops_per_query[query_name]
                intercept = fix_ops_per_query[query_name] - slope
                instances_formula = "%d + n*%d" % (intercept, slope)
            format_rows.append({
                "label": query_name,
                "instances_formula": instances_formula,
                "time_1": data[1],
                "time_8": data[8],
                "time_16": data[16],
                "time_31": data[31]})

        filename = "query_table_%d.tex" % int(time.time())
        with open(filename, "w") as f:
            f.write(self._latex_format_table(format_rows))
        print ">>>%s" % filename

    def _latex_format_table(self, format_rows):

        table_header = """
\\begin{table}[tb]
    \centering
    \\begin{tabular}{|p{2cm}||p{2cm}|p{2cm}|p{2cm}|p{2cm}|p{2cm}|}
        \hline
        Query & Number of tasks & Execution time n=1 & Execution time n=8 & Execution time n=16 & Execution time n=32\\\\
        \hline
        """

        table_line = """
        %(label)s & %(instances_formula)s & %(time_1)s ms & %(time_8)s ms & %(time_16)s ms & %(time_31)s ms \\\\
        """

        table_footer = """
        \hline
    \end{tabular}
    \caption{Query characteristics of enterprise application types}
    \label{tab:perf_benchmark}
\end{table}
        """

        table_lines = ""

        for format_dict in format_rows:
            table_lines += table_line % format_dict

        return table_header + table_lines + table_footer

    def _collect(self):
        data = []
        dirResults = os.path.join(os.getcwd(), "results", self._groupId)
        if not os.path.isdir(dirResults):
            raise Exception(
                "Group result directory '%s' not found!" % dirResults)

        dirRun = os.path.join(dirResults, "run")

        if not os.path.isdir(dirRun):
            raise Exception("Could not find directory of run!")

        # We expect only one build to exist
        if len(os.listdir(dirRun)) > 1:
            sys.exit("Expected exactly one build but found several!")

        dirBuild = os.path.join(dirRun, os.listdir(dirRun)[0])
        if not os.path.isdir(dirBuild):
            sys.exit("Expected a build dir, but only found a file!")

        for user in os.listdir(dirBuild):
            dirUser = os.path.join(dirBuild, user)
            if not os.path.isdir(dirUser):
                continue

            logFileName = os.path.join(dirUser, "queries.log")
            if not os.path.isfile(logFileName):
                message = "WARNING: no transaction log found in %s!" % dirUser
                raise Exception(message)

            running_num = 0
            for rawline in open(logFileName):
                linedata = rawline.split(";")
                if len(linedata) < 2:
                    continue
                txId = linedata[0]
                query_name, instances = txId.split("_")
                # convert python literal to json
                # in order to use the faster ujson instead of ast
                jsonData = linedata[1].replace("u'", '"').replace("'", '"')
                opData = ujson.loads(jsonData)

                for op in opData:
                    op_name = op["name"]
                    op_id = op["id"]
                    op_start = op["startTime"]
                    op_end = op["endTime"]
                    op_duration = op_end - op_start
                    if query_name == "xselling":
                        query_id = "q13"
                    else:
                        query_id = query_name
                    data.append({
                        "queryName": query_name,
                        "queryId": query_id,
                        "runningNum": running_num,
                        "instances": int(instances),
                        "txId": txId,
                        "opName": op_name,
                        "opId": op_id,
                        "opStartTime": op_start,
                        "opEndTime": op_end,
                        "opDuration": op_duration,
                        "opData": op})

                running_num += 1

        return data
