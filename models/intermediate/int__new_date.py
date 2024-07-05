from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, rand
from pyspark.sql.types import TimestampType


def model(dbt, session):
    dbt.config(materialized="table")
    
    
    df = dbt.ref('int__join_pmv')
    start_timestamp = "2023-01-01 00:00:00"
    end_timestamp = "2023-12-31 23:59:59"

    df1 = df.withColumn("random_timestamp", expr(f"cast((unix_timestamp('{start_timestamp}') + (unix_timestamp('{end_timestamp}') - unix_timestamp('{start_timestamp}')) * rand()) as timestamp)"))
    
    return df1

    
