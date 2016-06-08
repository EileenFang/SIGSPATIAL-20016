## Spark Application - execute with spark-submit
 
## Imports
from pyspark import SparkConf, SparkContext
from StringIO import StringIO
from datetime import datetime
from collections import namedtuple
from operator import add, itemgetter
from math import *
import csv
 
## Module Constants
APP_NAME = "My Spark Application"
DATE_FMT = "%Y/%m/%d  %H:%M"
fields   = ('VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 
            'pickup_longitude','pickup_latitude', 'RateCodeID', 'store_and_fwd_flag', 'dropoff_longitude', 
            'dropoff_latitude','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount',
            'improvement_surcharge','total_amount')
Taxi   = namedtuple('Taxi', fields)

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

def calcDistance(Lat_A, Lng_A, Lat_B, Lng_B):
     ra = 6378.140  # equator radius (km)
     rb = 6356.755  # polar radius (km)
     flatten = (ra - rb) / ra  # compression of the earth
     rad_lat_A = radians(Lat_A)
     rad_lng_A = radians(Lng_A)
     rad_lat_B = radians(Lat_B)
     rad_lng_B = radians(Lng_B)
     pA = atan(rb / ra * tan(rad_lat_A))
     pB = atan(rb / ra * tan(rad_lat_B))
     xx = acos(sin(pA) * sin(pB) + cos(pA) * cos(pB) * cos(rad_lng_A - rad_lng_B))
     c1 = (sin(xx) - xx) * (sin(pA) + sin(pB)) ** 2 / cos(xx / 2) ** 2
     c2 = (sin(xx) + xx) * (sin(pA) - sin(pB)) ** 2 / sin(xx / 2) ** 2
     dr = flatten / 8 * (c1 - c2)
     distance = ra * (xx + dr)
     return distance

     
def transferTo(car):
    #unit x (74.25-73.7)/233=0.0023605  unit y (40.9-40.5)/222=0.0018018
    x1 = (abs(car[0])-73.7)/0.0023605
    y1 = (car[1]-40.5)/0.0018018
    return ((int(x1),int(y1)),1)
    
## Main functionality
def main(sc):
    #taxi = sc.textFile("first600.csv").map(split).map(parse)
    
    #remove the first line of csv in RDD
    taxi = sc.textFile("first600.csv").map(split)
    header = taxi.first()
    taxi = taxi.filter(lambda x:x!=header).map(parse)
        
    #cal the width and lenth
    """x_size = calcDistance(40.5,73.7,40.5,74.25)
    y_size = calcDistance(40.5,73.7,40.9,73.7)
    print x_size
    print "\n"
    print y_size
    """
    
    #get the pickup position and put into the standard grid maps
    pickup = taxi.map(lambda f:(f.pickup_longitude,f.pickup_latitude,1))
    
    #pickup = pickup.filter(lambda f: f[0]!=0 and f[1]!=0).map(transferTo).sortByKey()
    pickup = pickup.filter(lambda f: f[0]!=0 and f[1]!=0).map(transferTo)
    
    #empty grid map
    myMap = [([0,0],0)  for i in range(233*222)]
    for i in range(0,233*222):
        myMap[i][0][0] = i%233
        myMap[i][0][1] = int(i/233)
    #print myMap         
    emptyRDD = sc.parallelize(myMap)
    def change(f):
        return ((f[0][0],f[0][1]),0)
    emptyRDD = emptyRDD.map(change)   
    pickup = pickup.union(emptyRDD)
    
    #create map with number of cars
    pickup = pickup.reduceByKey(add).sortByKey()
    gridsource = pickup.collect()
    gridsource = sorted(gridsource, key=itemgetter(1))
    print gridsource[-50:]
    
    gridMap = sc.broadcast(gridsource)
    
    def average():
        sum = 0.0
        for p in gridMap.value:
            sum += p[1]
        return sum/(233*222)
    avr_x = average()  
    
    def standardDev():
        sum = 0.0
        for p in gridMap.value:
            sum += ((p[1])**2)
        result = sqrt(sum/(233*222) - avr_x**2)
        return result
    S = standardDev()
    
    
    def Gi(self):
        item1 = 0.0
        item2 = 0.0
        item3 = 0.0
        item4 = 0.0
        for j in gridMap.value:
            #confuse the location value
            pow_longi = pow((self[0][0] - j[0][0]),2)
            pow_latitu = pow((self[0][1] - j[0][1]),2)
            weight = sqrt(pow_latitu + pow_longi)
            attr = j[1]
            item1 += weight * attr
            item2 += weight
            item3 += weight**2
        item4 = item2**2
        
        result = (item1-item2*avr_x)/(S*sqrt((233*222*item3-item4)/(233*222-1)))    
        
        return ((self[0][0],self[0][1]),result)

    jili = pickup.map(Gi).collect()
    jili = sorted(jili,key = itemgetter(1))
    
    #In the real program
    #jili = pickup.map(Gi).sortBy(lambda x:x[1],false)
    #jili = jili.top(20)
    #print jili
    
    print jili[-20:]

         
 
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
 
    # Execute Main functionality
    main(sc)
