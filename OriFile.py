## Spark Application - execute with spark-submit

## Imports
from pyspark import SparkConf, SparkContext
from collections import namedtuple
from StringIO import StringIO
from datetime import datetime
from operator import add
from math import *
import csv
import time
import os
start = time.time()

## Module Constants
APP_NAME = "My Spark Application"
DATE_FMT = "%Y-%m-%d  %H:%M:%S"
fields   = ('VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 
            'pickup_longitude','pickup_latitude', 'RateCodeID', 'store_and_fwd_flag', 'dropoff_longitude', 
            'dropoff_latitude','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount',
            'improvement_surcharge','total_amount')
Taxi   = namedtuple('Taxi', fields)

Time_step_std = 2
cell_size_std = 0.0025
Grid_x = int((74.25-73.7)/cell_size_std)
Grid_y = int((40.9-40.5)/cell_size_std)
Grid_size = Grid_x * Grid_y


## Locally process

def parse(row):
    """
    Parses a row and returns a named tuple.
    """
 
    row[0] = int(row[0],0)
    row[1]  = datetime.strptime(row[1], DATE_FMT)
    row[2]  = datetime.strptime(row[2], DATE_FMT)
    row[3] = int(row[3],0)
    row[4] = float(row[4])
    row[5] = float(row[5])
    row[6] = float(row[6])
    row[7]  = int(row[7],0)
    row[9]  = float(row[9])
    row[10] = float(row[10])
    row[11] = float(row[11])
    row[12] = float(row[12])
    row[13] = float(row[13])
    row[14] = float(row[14])
    row[15] = float(row[15])
    row[16] = float(row[16])
    row[17] = float(row[17])
    row[18] = float(row[18])
    return Taxi(*row[:19])

def split(line):
    """
    Operator function for splitting a line with csv module
    """
    reader = csv.reader(StringIO(line))
    return reader.next()


# 0.0025 degree 
OriTime = datetime(2009,1,1,0,0,0)
def standarizeTime(line):
    cell_x = (abs(line.pickup_longitude)-73.7)/0.0025
    cell_y = (line.pickup_latitude-40.5)/0.0025
    
    TimeDiff = line.tpep_pickup_datetime - OriTime
    cell_z = (TimeDiff.days*24*3600+TimeDiff.seconds)/(Time_step_std*3600)
    return ((cell_z,int(cell_x),int(cell_y)),1)



## Main functionality
def main(sc):
    ## Remove the first line and the unuseable data of csv
    OriginalRDD = sc.textFile("/home/summer/Desktop/secondEDI/day1seperate/*.csv").map(split)
    #OriginalRDD = sc.textFile("hdfs://10.119.176.10:9000/test/*.csv").map(split)
    
    header = OriginalRDD.first()
    def ReduceUnusable(row):
        return row != header and \
               row[5]!=0 and float(row[5])>-74.25 and float(row[5])<-73.7 and float(row[6])!=0 \
               and float(row[6])>40.5 and float(row[6])<40.9
    SourceRDD = OriginalRDD.filter(ReduceUnusable).map(parse)
    #### 目前读取了6个文件，如果不用partitionBy或partitionBy(6)则不会多一个stage去shuffle，
    #### 也就是默认每个文件分在了一个partition
    FirstPartition = SourceRDD.map(standarizeTime).reduceByKey(add).partitionBy(3).persist().take(1000)
    print FirstPartition
    

    ## Make grid
    """
    Grid = {}
    def MakeGrid(line):
        # Standarize the time
        TimeDiff = line.tpep_pickup_datetime - StartTime
        TimeKey = int((TimeDiff.days * 24 * 3600 + TimeDiff.seconds) / (Time_CELL * 3600))

        # Standarize the position
        longtitude = int((abs(line.pickup_longitude) - 73.7) / 0.0023605)
        latitude = int((line.pickup_latitude - 40.5) / 0.0018018)

        # Update grid
        if TimeKey not in Grid:
            newGridItem = [[0 for y in range(y_coor)] for x in range(x_coor)]
            Grid[TimeKey] = newGridItem
            print Grid.keys()
        
        Grid[TimeKey][longtitude][latitude] += 1
        return (TimeKey, longtitude, latitude)

    GridRDD = SourceRDD.map(MakeGrid)
    
    ## Make the grid of the number of cars

    result = GridRDD.take(140000)
    #print result
    ## Rise Error when do the following sentence
    """


if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[3]")
    sc   = SparkContext(conf=conf)
 
    # Execute Main functionality
    main(sc)
endtime = (time.time()-start)
print ("Time used: %s seconds" % endtime)
raw_input("input::")
