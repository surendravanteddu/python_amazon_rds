#import boto3
#import boto
#import csv
#import pandas as pd
#from boto.s3.key import Key
import mysql.connector
import time
import urllib2
import random
import memcache
from datetime import datetime

#s3 = boto3.resource('s3')
cnx =mysql.connector.connect(user='surendranaidu04', password='Surendra04',
                              host='project37754.c92wi7qbbarw.us-west-2.rds.amazonaws.com',
                              database='project37754',port=3306)
mem = memcache.Client(['project377542.irzg4t.cfg.usw2.cache.amazonaws.com:11211'],debug=1)

#s3 = boto3.client('s3')
#obj = s3.get_object(Bucket='project37754', Key='project3.csv')
#df = pd.read_csv(obj['Body'])
#df.to_sql('mydata', cnx, flavor='mysql', schema=None, if_exists='replace', index=False, index_label=None, chunksize=10, dtype=None)
#csv_reader = csv.reader(StringIO(df))
#data=[]
#CREATE TABLE `project37754`.`dump` (`id` INT NOT NULL AUTO_INCREMENT,`Date_received` VARCHAR(145) NULL,`Product` VARCHAR(145) NULL,`Sub_product` VARCHAR(145) NULL,`Issue` VARCHAR(145) NULL,PRIMARY KEY (`id`));

def insertData():
    data  = urllib2.urlopen("https://s3-us-west-2.amazonaws.com/project37754/project.csv")
    dat = list()

    for line in data:
        s = line.split(",")
        d = list()    
        for i in range(4):
           if(s[i]==''):
             s[i] = 'none'
           d.append(s[i])    
        dat.append(d)
           
    cursor = cnx.cursor() 
    starttime = int(time.time()*1000)
    cursor.executemany("INSERT INTO dump (Date_received,Product,Sub_product,Issue) VALUES (%s,%s,%s,%s)",dat) 
    cnx.commit()
    endtime = int(time.time()*1000)
    tt = endtime-starttime
    print "total time is "+str(tt)+" sec"

#insertData()  
def make_key(a):
    a=a.replace(' ','_')
    return a

def randomQuery(count):
    #dt = datetime.now()
    starttime = int(time.time()) 
    #dt.microsecond
    print 'running.....'
    for x in range(count):
        id=random.randint(1,56726)
        cursor = cnx.cursor()
        query = 'select id,Date_received,Product,Sub_product,Issue from dump where id='+str(id)+''
        temp = mem.get(make_key(query))
        if not temp:
           cursor.execute(query)
           result = cursor.fetchall()
           mem.set(make_key(query),result,300)
           #for res in result:
           #    print res
           cnx.commit()
        else:
            print 'from cache'
            for z in temp:
                print z
            
    #dt = datetime.now()   
    endtime = int(time.time())
    #dt.microsecond
    tt = endtime-starttime
   
    print "total time is "+str(tt)+" sec"
    
#randomQuery(100)
def memcachetest(id):
    starttime = int(time.time()) 
    cursor = cnx.cursor()
    query = 'select id,Date_received,Product,Sub_product,Issue from dump where id='+str(id)+''
    for i in range(2):
        temp = mem.get('s')
        print temp
        if not temp:
           cursor.execute(query)
           result = cursor.fetchall()
           mem.set('s',result,300)
           for res in result:
               print res
           cnx.commit()
        else:
           print 'from cache'
           for z in temp:
               print z
    
    endtime = int(time.time())
    #dt.microsecond
    tt = endtime-starttime
   
    print "total time is "+str(tt)+" sec"
    
#memcachetest(500)
def memtest():
    print mem
    mem.set('x','surendra')
    print mem.get('x')
    
    
memtest()    


