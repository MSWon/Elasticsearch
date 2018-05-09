# -*- coding: utf-8 -*-
"""
Created on Thu May  3 14:05:16 2018

@author: jbk48
"""

import elasticsearch
import json
import pandas as pd
import numpy as np
import os

os.chdir("C:\\Users\\jbk48\\OneDrive\\바탕 화면")

log_type = 1  ## 로그 타입
bytype = 51   ## 51 : 골드 , 52 : 루비 , 53 : 티아라

es_client = elasticsearch.Elasticsearch("localhost:9200")

## 엘라스틱 쿼리
def query(log_type,bytype):
    
    query = {

            "query":{
                    "term":{
                            "log_type":"{}".format(log_type)
                            },
                    "term":{
                            "bytype":"{}".format(bytype)
                            }
                    }
            }
                    
    return query
    


docs = es_client.search(index = 'pmotest1-2018.03.28', doc_type = 'logs', body = query(log_type,bytype), scroll ='1m',size=1000)

scroll_id = docs['_scroll_id']
num_docs = len(docs['hits']['hits'])

final_df = pd.DataFrame()

while(num_docs > 0):
    
    docs = es_client.scroll(scroll_id = scroll_id,
                            scroll = '1m')
    raw_data = docs['hits']['hits']
    num_docs = len(raw_data)
    
    time = pd.DataFrame({"time":[row['_source']["writetime"] for row in raw_data]})
    gold = pd.DataFrame({"gold":[row['_source']["ncount"] for row in raw_data]})
    df = pd.concat([time,gold], axis= 1)
    
    final_df = pd.concat([final_df,df], axis=0)
    
    print("{} docs retrieved".format(num_docs))
    
final_df = final_df.sort_values(by="time", ascending = True)
final_df.to_csv("2018.03.28.csv" , sep ="," , index = False)


print(json.dumps(doc,indent=2))

