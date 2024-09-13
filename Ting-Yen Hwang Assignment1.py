from __future__ import print_function

import os
import sys
import requests
from operator import add
import bz2

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *
from pyspark.rdd import RDD

#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False
    
def parse_line(line):
    fields = line.split(',')
    return (
        fields[1],  # Driver ID (hack license)
        float(fields[16]),  # Total amount
        float(fields[4]) / 60  # Trip time in minutes (convert seconds to minutes)
    )

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p
            


#Main
if __name__ == "__main__":

    #if len(sys.argv) != 4:
        #print("Usage: main_task1 <file> <<output>> ", file=sys.stderr)
        #exit(-1)

    sc = SparkContext(appName="Assignment-1")
    
   #rdd = sc.textFile(sys.argv[1])
    rdd = sc.textFile(sys.argv[1])
    rdd1 = rdd.map(lambda line: line.split(","))

    #Task 1
    #Your code goes here


    distinct_drivers_per_taxi = rdd1.map(lambda x: (x[0], x[1])) \
                               .distinct() \
                               .map(lambda x: (x[0], 1)) \
                               .reduceByKey(lambda a, b: a + b)

    sorted_taxis = distinct_drivers_per_taxi.map(lambda x: (x[1], x[0])) \
                                        .sortByKey(False) \
                                        .take(10)
    
    
    result1 = sc.parallelize(sorted_taxis)
    result1.coalesce(1).saveAsTextFile(sys.argv[2])

    #Task 2
    #Your code goes here

    parsed_rdd = rdd.map(parse_line)
    parsed_rdd1 = parsed_rdd.filter(correctRows)
    driver_money_per_minute_rdd = parsed_rdd.map(lambda x: (x[0], (x[1], x[2])))

    driver_totals_rdd = driver_money_per_minute_rdd.reduceByKey(lambda a, b: (
    a[0] + b[0],  # Sum of total_amount
    a[1] + b[1]   # Sum of trip_time_in_minutes
))
    average_money_per_minute_rdd = driver_totals_rdd.map(lambda x: (
    x[0], 
    x[1][0] / x[1][1] if x[1][1] != 0 else 0  # Avoid division by zero
))
    
    top_drivers_rdd = average_money_per_minute_rdd.sortBy(lambda x: x[1], ascending=False).take(10)
    result2 = sc.parallelize(top_drivers_rdd)
    result2.coalesce(1).saveAsTextFile(sys.argv[3])


    #Task 3 - Optional 
    #Your code goes here

    #Task 4 - Optional 
    #Your code goes here


    sc.stop()