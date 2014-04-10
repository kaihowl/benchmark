PREPARE_DISTINCTS_SERVER = {
    "distinct-name1-adrc": "queries/vldb-mixed/distinct-name1-adrc.json",
    "distinct-kunnr-kna1": "queries/vldb-mixed/distinct-kunnr-kna1.json",
    "distinct-kunnr-vbak": "queries/vldb-mixed/distinct-kunnr-vbak.json",
    "distinct-matnr-makt": "queries/vldb-mixed/distinct-matnr-makt.json",
    "distinct-matnr-mara": "queries/vldb-mixed/distinct-matnr-mara.json",
    "distinct-matnr-vbap": "queries/vldb-mixed/distinct-matnr-vbap.json",
    "distinct-vbeln-vbap": "queries/vldb-mixed/distinct-vbeln-vbap.json",
    "distinct-vbeln-vbak": "queries/vldb-mixed/distinct-vbeln-vbak.json"
}

PREPARE_QUERIES_USER = {}
TABLE_LOAD_QUERIES_SERVER = {
    "preload" : "queries/vldb-mixed/preload.json"
}

PREPARE_QUERIES_SERVER = {
    "index-name1-adrc": "queries/vldb-mixed/index-name1-adrc.json",
    "index-kunnr-kna1": "queries/vldb-mixed/index-kunnr-kna1.json",
    "index-matnr-makt": "queries/vldb-mixed/index-matnr-makt.json",
    "index-matnr-mara": "queries/vldb-mixed/index-matnr-mara.json",
    "index-vbeln-vbak": "queries/vldb-mixed/index-vbeln-vbak.json",
    "index-vbeln-vbap": "queries/vldb-mixed/index-vbeln-vbap.json"
    }

# locations for json query files sorted by OLTP (TPCC) and OLAP (TPC-H).                                                                                                                                      

OLAP_QUERY_FILES = {
    "q10"                    : "queries/mixed/q10.json",
    "q11"                    : "queries/mixed/q11.json",
    "q12"                    : "queries/mixed/q12.json",
    "xselling"               : "queries/mixed/xselling.json",
    "vldb_q10"               : "queries/vldb-mixed/q10.json",
    "vldb_q11"               : "queries/vldb-mixed/q11.json",
    "vldb_q12"               : "queries/vldb-mixed/q12.json",
    "vldb_xselling"          : "queries/vldb-mixed/xselling.json",
    "q6_ch"                  : "queries/mixed/q6_ch.json",
    "ccsched1"               : "queries/other/ccsched_test.json",
    "ccsched2"               : "queries/other/ccsched_test2.json",
    "ccsched3"               : "queries/other/ccsched_test3.json",
    "ccneutral"               : "queries/other/ccneutral.json",
    "q10i":        "queries/mixed/q10.json",
    "q11i":        "queries/mixed/q11.json",
    "q12i":        "queries/mixed/q12.json",
    "xsellingi":   "queries/mixed/xselling.json"
    }


OLTP_QUERY_FILES ={
      "vldb_q6a":  "queries/vldb-mixed/q6a.json",
      "vldb_q6b":  "queries/vldb-mixed/q6b.json",
      "vldb_q7":  "queries/vldb-mixed/q7.json",
      "vldb_q8":  "queries/vldb-mixed/q8.json",
      "vldb_q9":  "queries/vldb-mixed/q9.json"
}


# relative weights for queries in format (id, weight)                                                                                                                                                         
OLTP_WEIGHTS = (
    ("vldb_q6a", 1),
    ("vldb_q6b", 1),
    ("vldb_q7", 1),
    ("vldb_q8", 1),
    ("vldb_q9", 1)
    )

OLAP_WEIGHTS = (
    ("q10", 1),
    ("q11", 1),
    ("q12", 1),
    ("xselling", 1),
    ("vldb_q10", 1),
    ("vldb_q11", 1),
    ("vldb_q12", 1),
    ("vldb_xselling", 1),
    ("q6_ch", 1),
    ("ccsched1", 1),
    ("ccsched2", 1),
    ("ccsched3", 1),
    ("ccneutral", 1)
    )

# All queries                                                                                                                                                                                                 
ALL_QUERIES = OLTP_WEIGHTS + OLAP_WEIGHTS                                                                                                                                                                     


