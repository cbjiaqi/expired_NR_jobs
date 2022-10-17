import json
import requests as requests
from datetime import datetime
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *

#parser parameters
parser = argparse.ArgumentParser()
parser.add_argument('--algorithm', help='algorithm')
#parser.add_argument('--queue', help='queue')
parser.add_argument('--db', help='database for saving table')
args = parser.parse_args()
algorithm = args.algorithm
#queue = args.queue
db = args.db

spark = SparkSession \
    .builder \
    .appName("Get Rec Job from New Relic") \
    .getOrCreate()

libpath = "hdfs://qtmhdfs:8020/user/svcsearchjobs/jixu/expired-NR-jobs-dev/"
src_dir_path = libpath + "get_api_response_jobs/"
spark.sparkContext.addPyFile(src_dir_path + 'api_key.py')

import api_key


#algorithm ['GBR_MIX_A', 'GBR_MIX_B', 'GBR_MIX_C', 'GBR_SCALE_TEN', 'GBR_ONLY']
#queue svcdataservicesjobs
#spark2-submit NR_API_test3.py --queue svcdataservicesjob --algorithm 'GBR_SCALE_TEN'

#import api key
Account_Id = api_key.Account_Id
API_KEY = api_key.API_KEY

#define nrql
def nrql(key, account_id, algorithm='GBR_SCALE_TEN'):
  # Query Builder
    #request_id = '[ed59d470-d431-4d33-8168-b0bc20c53713]'
    #print(type(request_id))
    query1 = """{ 
        actor { account(id:"""+str(account_id)+""") 
          { nrql
          (query: "SELECT jobID as src_job, tuple(`Job Id's in Recs Response Data-0`, `Job Id's in Recs Response Data-1`,\
          `Job Id's in Recs Response Data-2`,`Job Id's in Recs Response Data-3`) as rec_jobs, timestamp \
          FROM Transaction WHERE appName= 'JobRecommendationUS' and \
          name like 'WebTransaction/RestWebService/consumer/recommendations/platform (POST)' \
          and Algorithm='"""

    query2 = str(algorithm)

    #query2 = """and timestamp>'""" + str(timestamp_id) + """' """

    query3 = """' SINCE 2 hours ago \
          limit 2000")
          {results
            queryProgress {
               queryId
               completed
               retryAfter
               retryDeadline
               resultExpiration
            }
            }
        }
    }
    }"""
    query = query1+query2+query3
    #print(query)
    endpoint = "https://api.newrelic.com/graphql"
    headers = {'API-Key': key}

    #start = time.time()
    response = requests.post(endpoint, headers=headers, json={"query": query})
    if response.status_code == 200:
        dict_response = json.loads(response.text)
        results_response = dict_response['data']['actor']['account']['nrql']['results']
        return results_response
    else:
        raise Exception(f'Query failed with a {response.status_code}.')

def get_results_resonse():
    src_jobs_cur = []
    rec_jobs_cur = []
    send_time_cur = []
    try:
        results_response = nrql(API_KEY, Account_Id, algorithm)
        m = len(results_response)
        if m > 0:
            for j in range(m):
                rec_jobs = list(filter(None, results_response[j]['rec_jobs']))
                n = len(rec_jobs)
                if n > 0:
                    rec_jobs = [item.split(",") for item in rec_jobs if item is not None]
                    rec_jobs = [item for elem in rec_jobs for item in elem]
                    k = len(rec_jobs)
                    if k > 0:
                        src_jobs = [results_response[j]['src_job']] * k
                        send_time = [datetime.fromtimestamp(results_response[j]['timestamp'] / 1000.0)] * k
                        src_jobs_cur = src_jobs_cur + src_jobs
                        rec_jobs_cur = rec_jobs_cur + rec_jobs
                        send_time_cur = send_time_cur + send_time
        return src_jobs_cur, rec_jobs_cur, send_time_cur
    except Exception as e:
        return src_jobs_cur, rec_jobs_cur, send_time_cur


if __name__ == "__main__":

    src_jobs_all = []
    rec_jobs_all = []
    send_time_all = []
    for i in range(20):
        src_jobs_cur, rec_jobs_cur, send_time_cur = get_results_resonse()
        columns = ['srcjob_id', 'recjob_id', 'send_time']
        sparkDF = spark.createDataFrame(zip(src_jobs_cur, rec_jobs_cur, send_time_cur), columns)
        sparkDF = sparkDF.dropDuplicates()
        if i==0:
            #destn_tbl = f"dataservices_test.qqq_get_rec_jobs_new_relic"
            #spark.sql(f"drop table if exists {destn_tbl}")
            destn_tbl = "qqq_get_rec_jobs_new_relic_"+algorithm
            spark.sql('''drop table if exists {}.{}'''.format(db, destn_tbl))
            sparkDF.write.saveAsTable('''{}.{}'''.format(db, destn_tbl))
        else:
            #destn_tbl1 = f"dataservices_test.qqq_get_rec_jobs_new_relic_tmp"
            #spark.sql(f"drop table if exists {destn_tbl1}")
            destn_tbl1 = "qqq_get_rec_jobs_new_relic_tmp" + algorithm
            spark.sql('''drop table if exists {}.{}'''.format(db, destn_tbl1))
            sparkDF.write.saveAsTable('''{}.{}'''.format(db, destn_tbl1))
            #spark.sql(f"insert overwrite table {destn_tbl} select * from {destn_tbl1}")
            spark.sql('''insert overwrite table {}.{} select * from {}.{}'''.format(db, destn_tbl, db, destn_tbl1))