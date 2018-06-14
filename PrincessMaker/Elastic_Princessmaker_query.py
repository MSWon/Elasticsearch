# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 14:26:02 2018

@author: jbk48
"""

import elasticsearch
import pandas as pd
import os

os.chdir("C:\\Users\\jbk48\\OneDrive\\바탕 화면")

log_type = 13  ## 로그 타입 (13 : 유저 로그인)
time = "3h"  ## 시간 간격 (3h : 3시간)

es_client = elasticsearch.Elasticsearch("115.145.170.72:9214")

## 엘라스틱 쿼리
def query(log_type,time):
    
    query = {

            "query" : {
                    "term" : {
                            "log_type" : "{}".format(log_type)
                            }
                    },
            "aggs" : {
                    "histo_checkin" : {
                            "date_histogram" : {
                                    "field" : "@timestamp",
                                    "interval" : "{}".format(time),
                                    "format" : "yyyy-MM-dd HH:mm:ss"
                                    }
                            }
                    }
            }
                    
    return query
    


docs = es_client.search(index = 'pmotest1-*', doc_type = 'logs', body = query(log_type,time), request_timeout = 3000)


raw_data = docs['aggregations']['histo_checkin']['buckets']

time = pd.DataFrame({"time":[row['key_as_string'] for row in raw_data]})
count = pd.DataFrame({"count":[row['doc_count'] for row in raw_data]})
df = pd.concat([time, count], axis = 1)

df.to_csv("3h_login.csv" , sep ="," , index = False)


