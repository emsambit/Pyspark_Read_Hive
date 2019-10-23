'''
## Variables included are -                                                                       #
##================================================================================================#
## REVISION HISTORY                                                                               #
##------------------------------------------------------------------------------------------------#
## 2019-09-26:: v1.0  :: Sambit Baliarsingh     :: Created                                        #
##  pyspark --queue <<queue>>
    spark-submit --master yarn --deploy-mode cluster --queue <<queue>>
    ./run_spark.sh
##================================================================================================#
'''

from pyspark.sql.functions import current_date, datediff, unix_timestamp,next_day
from pyspark.sql.functions import rank
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, DecimalType,IntegerType,FloatType,DateType,TimestampType,DoubleType
import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf, SparkContext, StorageLevel
import os
from pyspark.sql.functions import lit,trim,lower
from pyspark.sql import functions as Fu
import sys
from pyspark.sql.functions import *

os.environ["PYSPARK_PYTHON"]="python2.7"
os.environ["PYSPARK_DRIVER_PYTHON"]="python2.7"


import argparse
parser = argparse.ArgumentParser()
parser.add_argument("spark_setting", help="get spark settings",type=str)
parser.add_argument("target_table", help="get target_table",type=str)
args = parser.parse_args()



def genrate_setting(conf_string):
    setting_temp =conf_string.replace('@','"')
    setting_list=setting_temp.split("|")
    setting = [x for x in setting_list if x]
    spark_setting=[]
    for i in setting:
        k=i.split(''","'')
        tup = k[0].rstrip('"').lstrip('("'), k[1].rstrip('")').lstrip('"')
        spark_setting.append(tup)
    return spark_setting

settings=genrate_setting(args.spark_setting)

target_table=args.target_table

print(args.spark_setting)
print("target table set to --> {}".format(target_table))

spark_conf = SparkConf().setAppName("appname").setAll(settings)
spark = SparkSession.builder. \
            config(conf = spark_conf). \
            enableHiveSupport(). \
            getOrCreate()
