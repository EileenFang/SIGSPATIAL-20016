## Spark Application - execute with spark-submit
 
## Imports
from pyspark import SparkConf, SparkContext
from StringIO import StringIO
from datetime import datetime
from collections import namedtuple
from operator import add, itemgetter
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

Time_CELL = 2
x_coor = 233
y_coor = 222
CELL_size = x_coor * y_coor

## Closure Functions
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

StartTime = datetime(2009,1,1,0,0,0)
#depend on hour
def splitByTime(f):
    #unit x (74.25-73.7)/233=0.0023605  unit y (40.9-40.5)/222=0.0018018
    x1 = (abs(f.pickup_longitude)-73.7)/0.0023605
    y1 = (f.pickup_latitude-40.5)/0.0018018   

    TimeDiff = f.tpep_pickup_datetime-StartTime
    TimePosition = (TimeDiff.days*24*3600+TimeDiff.seconds)/(Time_CELL*3600)
    return ((TimePosition,int(x1),int(y1)),1)

def changeFormat(f):
    return (f[0][0],[[f[0][1],f[0][2]],f[1]])

def UnionEmptyGrid(g):
    myMap = [[[0,0],0]  for i in range(CELL_size)]
    for i in range(0,CELL_size):
        myMap[i][0][0] = i%x_coor
        myMap[i][0][1] = int(i/x_coor)
    #print myMap
    for item in g[1]:
        for i in myMap:
            if i[0]==item[0]:
                i[1] = item[1]
    return (g[0],myMap)

def preWeightGrid():
    WeightFirstLineGrid = [[0 for x in range(x_coor)] for y in range(y_coor)]
    for i in range(x_coor):
        for j in range(y_coor):
            for k in range(x_coor):
                WeightFirstLineGrid[i][j] += (j**2 + (k-i)**2)
     
    WeightGrid = [[0 for x in range(x_coor)] for y in range(y_coor)]
    for i in range(x_coor):
        for j in range(y_coor):
            if j==0:
                for k in range(y_coor):
                    WeightGrid[i][j] += WeightFirstLineGrid[i][k]
            else:
                WeightGrid[i][j] = WeightGrid[i][j-1] - WeightFirstLineGrid[i][y_coor-j] + WeightFirstLineGrid[i][j]
    return WeightGrid

    #square
    def preWeightGrid2():
    WeightFirstLineGrid = [[0 for x in range(x_coor)] for y in range(y_coor)]
    for i in range(x_coor):
        for j in range(y_coor):
            for k in range(x_coor):
                WeightFirstLineGrid[i][j] += (j**2 + (k-i)**2)**2
     
    WeightGrid = [[0 for x in range(x_coor)] for y in range(y_coor)]
    for i in range(x_coor):
        for j in range(y_coor):
            if j==0:
                for k in range(y_coor):
                    WeightGrid[i][j] += WeightFirstLineGrid[i][k]
            else:
                WeightGrid[i][j] = WeightGrid[i][j-1] - WeightFirstLineGrid[i][y_coor-j] + WeightFirstLineGrid[i][j]
    return WeightGrid

## Main functionality
def main(sc):
    
    #remove the first line of csv in RDD
    taxi = sc.textFile("sample.csv").map(split)
    header = taxi.first()
    taxi = taxi.filter(lambda x:x!=header).map(parse)
    
    #get the pickup position and put into the standard grid maps
    pickup = taxi.filter(lambda f: f.pickup_longitude!=0 and f.pickup_latitude!=0)
    pickup = pickup.map(splitByTime).reduceByKey(add).map(changeFormat)
    
    GroupedPickup = pickup.groupByKey().mapValues(list)
    GroupedPickup = GroupedPickup.map(UnionEmptyGrid).persist()
    dictGrid = dict(GroupedPickup.collect())
    
    gridMap = sc.broadcast(dictGrid)
    time_layers = len(gridMap.value.keys())

    #claculate the z value of each node
    #use a dictionary to record x_avr and stadardDev
    DictionaryPerTimelayer = {}
    def average(tCell):
        if(!DictionaryPerTimelayer.has_key(tCell)):     
            sum = 0.0
            for p in gridMap.value.keys():
                for k in gridMap.value[p]:
                    sum += k[1]
            return sum/(CELL_size*time_layers) 
        else:
            return DictionaryPerTimelayer[tCell][0]
    
    def standardDev(tCell,x_avr):
        if(!DictionaryPerTimelayer.has_key(tCell)):    
            sum = 0.0
            for p in gridMap.value.keys():
                for k in gridMap.value[p]:
                    sum += ((k[1])**2)
            result = sqrt(sum/(CELL_size*time_layers) - x_avr**2)
            DictionaryPerTimelayer[tCell] = (x_avr,result)
        return DictionaryPerTimelayer[tCell][1]
    
    #Get the standard weight grid to simplify the cal
    WeightGrid = preWeightGrid()
    WeightGrid2 = preWeightGrid2()
    dict= sorted(gridMap.iteritems(), key=lambda d:d[0], reverse = True)
    start = dict[-1]
    end = dict[0]

    def Gi(self):
        x_avr = average(self[0])
        S = standardDev(self[0],x_avr)
        item1 = 0.0
        item2 = 0.0
        item3 = 0.0
        item4 = 0.0
        for i in gridMap.value.keys():
            for j in gridMap.value[i]:
                pow_longi = pow((self[1][0][0] - j[0][0]),2)
                pow_latitu = pow((self[1][0][1] - j[0][1]),2)
                pow_time = pow((self[0]-i),2)
                weight = 1-(pow_latitu + pow_longi + pow_time)/(x_coor**2+y_coor**2+time_layers**2)
                attr = j[1]
                item1 += weight * attr
        t = 0
        for i in range(self[0]-start):
            t += (i+1)**2
        for i in range(end-self[0]):
            t += (i+1)**2

        item2 = time_layers*CELL_size - ((WeightGrid[self[1][0][0]][self[1][0][1]])*time_layers + t*CELL_size)/(x_coor**2+y_coor**2+time_layers**2)
        item3 = time_layers*CELL_size - ((WeightGrid2[self[1][0][0]][self[1][0][1]])*time_layers + t*CELL_size)/(x_coor**2+y_coor**2+time_layers**2)
        item4 = item2**2
        result = (item1-item2*x_avr)/(S*sqrt(abs(CELL_size*time_layers*item3-item4)/(CELL_size*time_layers-1)))
        return (self[0],self[1],result)
    
    #In the real program
    
    partitionedData = GroupedPicku.partitionBy(12).persist()
    output = partitionedData.map(Gi).sortBy(lambda x: x[2],False)
    output = output.take(50)
    
    #jili = jili.map(Gi).collect()
    #jili = sorted(jili,key = itemgetter(2))
    print output
    #print jili[-20:]

         
 
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
 
    # Execute Main functionality
    main(sc)
endtime = (time.time()-start)
print ("Time used: %s seconds" % endtime)
raw_input()
