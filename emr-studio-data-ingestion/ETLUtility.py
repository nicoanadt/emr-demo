#!/usr/bin/env python
# coding: utf-8

# # ETL Utility functions

# In[ ]:


import os
import sys

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import to_date, col, udf


import boto3
import botocore



# ## Find latest event time from a dataframe

# In[ ]:


def find_latest_event_time (inputDF, column_name,spark):
    inputDF.createOrReplaceTempView("df_table")
    sql_string = "SELECT MAX(" + column_name + ") as maxval FROM df_table"
    return spark.sql(sql_string).collect()[0].asDict()['maxval']


# ## Uppercase functions as UDF

# In[ ]:


from pyspark.sql.functions import udf
@udf
def my_uppercase(my_string):
    return my_string.upper()


# ## Get bookmark value from Redshift Bookmark table

# In[ ]:


def get_bookmark_value(v_bookmark_job_id):
    
    sql = "select job_id,bookmark_col, bookmark_val from etlfw_rs.bookmark_table where job_id ='" + v_bookmark_job_id + "'"
    v_cluster_identifier = 'lakehouse-redshift-cluster'
    v_secret_arn = 'arn:aws:secretsmanager:ap-southeast-1:99999999999:secret:prod/redshift-DqAAI9'
    v_database_name = 'dev'
    
    rsd = boto3.client('redshift-data', region_name='ap-southeast-1')

    resp = rsd.execute_statement(
        Database=v_database_name,
        ClusterIdentifier=v_cluster_identifier,
        SecretArn=v_secret_arn,
        Sql=sql
    )
    qid = resp["Id"]
    
    while True:
        desc = rsd.describe_statement(Id=qid)
        if desc["Status"] == "FINISHED":
            break
            print(desc["ResultRows"])

    if desc and desc["ResultRows"]  > 0:
        result = rsd.get_statement_result(Id=qid)
    
    return result["Records"][0][2]["stringValue"]


# In[ ]:


## Update bookmark value with new information


# In[ ]:


def update_bookmark_value(v_bookmark_job_id, v_new_bookmark_val):
    
    sql = "update etlfw_rs.bookmark_table set bookmark_val='" + v_new_bookmark_val + "' where job_id ='" + v_bookmark_job_id + "'"
    v_cluster_identifier = 'lakehouse-redshift-cluster'
    v_secret_arn = 'arn:aws:secretsmanager:ap-southeast-1:99999999999:secret:prod/redshift-DqAAI9'
    v_database_name = 'dev'

    rsd = boto3.client('redshift-data', region_name='ap-southeast-1')

    resp = rsd.execute_statement(
        Database=v_database_name,
        ClusterIdentifier=v_cluster_identifier,
        SecretArn=v_secret_arn,
        Sql=sql
    )
    qid = resp["Id"]
    


# Run the following to convert 
# 
# `jupyter nbconvert ETLUtility.ipynb --to python`
# 
# 
