# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 16:36:01 2018

@author: jbk48
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 14:26:02 2018

@author: jbk48
"""

import elasticsearch
import pandas as pd
import os

os.chdir("C:\\Users\\jbk48\\OneDrive\\바탕 화면")


es_client = elasticsearch.Elasticsearch("115.145.170.72:9214")

## 엘라스틱 쿼리
def query():
    
    query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"log_type": "22"}},
                            {"bool": {
                                "should": [
                                    {"term": {"weventtype": "20"}},
                                    {"term": {"weventtype": "100"}},
            			{"term": {"weventtype": "122"}},
            			{"term": {"weventtype": "113"}}
                                ]
                            }}
                        ]
                    }
                },
                "aggs" : {
                    "histo_checkin" : {
                      "date_histogram" : {
                      	"field" : "@timestamp",
             		"interval" : "3h",
            		"format" : "yyyy-MM-dd HH:mm:ss"
            		},
            	  "aggs" : {
                        "gold_sum" : {
             	   "sum" : {"field" : "dwsendmoney"}
            	 }
                   }
                 }
               }
            }
                    
    return query
    


docs = es_client.search(index = 'pmotest1-2018.06.17', doc_type = 'logs', body = query(), request_timeout = 3000)


raw_data = docs['aggregations']['histo_checkin']['buckets']

time = pd.DataFrame({"time":[row['key_as_string'] for row in raw_data]})
count = pd.DataFrame({"gold":[row['gold_sum']['value'] for row in raw_data]})
df = pd.concat([time, count], axis = 1)

df.to_csv("./3h_logtype_21.csv" , sep ="," , index = False)



