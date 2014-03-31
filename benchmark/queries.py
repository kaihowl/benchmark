PREPARE_DISTINCTS_SERVER = {
    "distinct-addrnumber-adrc": "queries/vldb-mixed/distinct-addrnumber-adrc.json",
    "distinct-kunnr-kna1": "queries/vldb-mixed/distinct-kunnr-kna1.json",
    "distinct-kunnr-vbak": "queries/vldb-mixed/distinct-kunnr-vbak.json",
    "distinct-matnr-makt": "queries/vldb-mixed/distinct-matnr-makt.json",
    "distinct-matnr-mara": "queries/vldb-mixed/distinct-matnr-mara.json",
    "distinct-matnr-vbap": "queries/vldb-mixed/distinct-matnr-vbap.json",
    "distinct-vbeln-vbap": "queries/vldb-mixed/distinct-vbeln-vbap.json",
    "distinct-vbeln-vbak": "queries/vldb-mixed/distinct-vbeln-vbak.json",
    "max-erdat-vbap": "queries/vldb-mixed/max-erdat-vbap.json",
    "max-erdat-vbak": "queries/vldb-mixed/max-erdat-vbak.json"
    #"indices": "queries/mixed/create_indices.json" ,
}

PREPARE_QUERIES_USER = {}

PREPARE_QUERIES_SERVER = {
    #"distinct_kunnr_adrc" : "queries/mixed/distinct_kunnr_adrc.json",                                                                                                                                              
    #"distinct_kunnr_kna1" : "queries/mixed/distinct_kunnr_kna1.json",                                                                                                                                              
    #"distinct_matnr_mara" : "queries/mixed/vbap_mara_group.json",                                                                                                                                                  
    #"distinct_vbeln_vbak" : "queries/mixed/distinct_vbeln_vbak.json",                                                                                                                                              
    #"indices": "queries/mixed/create_indices.json" ,
#    "preload_vbap" : "queries/mixed/preload_vbap.json",
#    "preload_cbtr_small" : "queries/mixed/preload_cbtr_small.json",
#    "preload_cbtr" : "queries/mixed/preload_cbtr.json",
    "preload" : "queries/vldb-mixed/preload.json",
    "index-addrnumber-adrc": "queries/vldb-mixed/index-addrnumber-adrc.json",
    "index-kunnr-kna1": "queries/vldb-mixed/index-kunnr-kna1.json",
    "index-matnr-makt": "queries/vldb-mixed/index-matnr-makt.json",
    "index-matnr-mara": "queries/vldb-mixed/index-matnr-mara.json",
    "index-vbeln-vbak": "queries/vldb-mixed/index-vbeln-vbak.json",
    "index-vbeln-vbap": "queries/vldb-mixed/index-vbeln-vbap.json"

#    "create_vbak_index" : "queries/mixed/create_vbak_index.json",
#    "create_vbap_index" : "queries/mixed/create_vbap_index.json"                             
    }

# locations for json query files sorted by OLTP (TPCC) and OLAP (TPC-H).                                                                                                                                      
OLTP_QUERY_FILES ={
      "q6a":  "queries/vldb-mixed/q6a.json",
      "q6b":  "queries/vldb-mixed/q6b.json",
      "q7":  "queries/vldb-mixed/q7.json",
      "q8":  "queries/vldb-mixed/q8.json",
      "q9":  "queries/vldb-mixed/q9.json"
}

OLAP_QUERY_FILES = {
    "q10":        "queries/vldb-mixed/q10.json",
    "q11":        "queries/vldb-mixed/q11.json",
    "q12":        "queries/vldb-mixed/q12.json",
    "xselling":   "queries/vldb-mixed/xselling.json",
    "q10i":        "queries/mixed/q10.json",
    "q11i":        "queries/mixed/q11.json",
    "q12i":        "queries/mixed/q12.json",
    "xsellingi":   "queries/mixed/xselling.json"
}



# relative weights for queries in format (id, weight)                                                                                                                                                         
OLTP_WEIGHTS = (
    ("q6a", 1),
    ("q6b", 1),
    ("q7", 1),
    ("q8", 1),
    ("q9", 1)
    )

OLAP_WEIGHTS = (
    ("q10", 1),
    ("q11", 1),
    ("q12", 1),
    ("xselling", 1)
)

# All queries                                                                                                                                                                                                 
ALL_QUERIES = OLTP_WEIGHTS + OLAP_WEIGHTS                                                                                                                                                                     


