#NIRZARI IYER
#ASSIGNMENT 4
#STUDENT ID: 1001117633
#BATCH_TIME: 6:00 - 8:00 p.m.
import boto
import urllib2
import time
import csv
import StringIO
import random
import MySQLdb
import memcache
import hashlib
import sys
import pdb
from boto.s3.key import Key

#Connect with s3 bucket
s3 = boto.connect_s3()

#Establish connection with database
connobj = MySQLdb.connect(host="awsdb.crtq9f6w5zvu.us-west-2.rds.amazonaws.com", user="username", passwd="password", db="aws_db")
#Create cursor
c = connobj.cursor()

#Calculate start time for file download
start_time = time.time()
#Open the file over http using the file url
response = urllib2.urlopen('http://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_master.csv')
#Read the file data
myfile = response.read()
#Create a file named myfile.csv
filename = open('myfile.csv','w')
#write previously fetched filedata into newly created file
filename.write(myfile)
#Close the file
filename.close()
#Calculate End time for file download
stop_time = time.time()
#Calculate total time for file download
download_time = stop_time - start_time
print "Time taken to download file: " + str(download_time) + "sec" 

#Create client object for memcache
m = memcache.Client(['mfree.whnkhx.cfg.usw2.cache.amazonaws.com:11211'])

#Read data and Insert data into relational database
def parse_insert():
    try:
        myfile = open('myfile.csv','r')
        csv_reader = csv.reader(myfile)
 	      #Skip the header line
        csv_reader.next()
        #Calculate start time for data insertion
        start_time = time.time()
        counter = 0
        for row in csv_reader:
            for i in range(0,10):
                if row[i] == "":
                    row[i] = "0"    
            place_name = row[4].replace("'","")  
            insert = "INSERT INTO data (hpi_type, hpi_flavor, frequency, level, place_name, place_id, yr, period,"\
                "index_nsa, index_sa) values('"+row[0]+"','"+row[1]+"','"+row[2]+"','"+row[3]+"','"+place_name+"','"+row[5]+"',"+row[6]+", "\
                ""+row[7]+","+row[8]+","+row[9]+")"
            data = c.execute(insert)
            counter += 1
            if counter == 60000:
                break
        #Calculate end time for data insertion
        stop_time = time.time()
        #Calculate total time for data insertion
        insert_time = stop_time - start_time
        print "Time taken to insert data is: " + str(insert_time) + "sec"   

    except Exception as e:
        print ("Data can't be inserted:  " + str(e))

def create_table():
    try:
        #pdb.set_trace()
        #use database aws_db
        connectdb = 'USE aws_db'
        #connect with the database aws_db
        c.execute(connectdb)
        #Drop table if already existed earlier
        c.execute("DROP TABLE data")
        #create table named data
        table = 'CREATE TABLE IF NOT EXISTS data '\
                '(id INT NOT NULL AUTO_INCREMENT,'\
                'hpi_type varchar(100),'\
                'hpi_flavor varchar(100),'\
                'frequency varchar(100),'\
                'level varchar(100),'\
                'place_name varchar(100),'\
                'place_id varchar(100),'\
                'yr INT,'\
                'period INT,'\
                'index_nsa DOUBLE,'\
                'index_sa DOUBLE, PRIMARY KEY(id))'
        c.execute(table)
        #parse data and insert into table
        parse_insert()
        
    except Exception as e:
            print "Data can't be inserted" + str(e)

#function to use memcache 
def memc(query):
    #get hash function for query
    hash = hashlib.sha224(query).hexdigest()
    #get query from cache if already available
    query_memc = m.get(hash)
    #add query to cache if not available
    if not query_memc:
        c.execute(query)
        rows = c.fetchall()
        m.set(hash,rows)       

#function to generate random query         
def generate_query(num_f,flag = True):
    query1 = "SELECT "
    attrlist = ["hpi_type", "hpi_flavor", "frequency", "level", "place_name", "place_id", "yr", "period", "index_nsa", "index_sa"]
    query2 = " FROM data "
    query3 = "WHERE yr  "
    ran_id = random.randint(1975,2004)
    ran_id2 = random.randint(1,4)
    ran_tuple = str(random.randint(200,800))
    #create an empty list
    list_l = []
    while len(list_l) <= num_f:
        r1 = random.randint(0,9)
        field = attrlist[r1]
        if field in list_l:
            pass
        else:
            #append field into the list
            list_l.append(field)
    for x in list_l:
        query1 += x + ","
    query1 = query1.strip(",")
    query =  query1+ query2+ query3 +" = " +str(ran_id) + " LIMIT "+ran_tuple
    if flag == False:
        query =  query1+ query2+ query3 +" = " +str(ran_id)+" AND "+"period = "+str(ran_id2)
    return query

#function to get particular number of queries into list    
def get_query(x,flag = True):
    #create an empty list
    querylist = []
    for i in range(50000):
        r = random.randint(0,1)
        query = generate_query(r,flag)
        #append query into the list    
        querylist.append(query)
        querylist = list(set(querylist))
        if len(querylist) == x:
            break
    start_time = time.time()
    for item in querylist:
        r = c.execute(item)
        fetch = c.fetchall()
    stop_time = time.time()
    time_nomemc = stop_time - start_time
    start_time = time.time() 
    for item in querylist:
        memc(item)
    stop_time = time.time()
    time_memc = stop_time - start_time
    return time_nomemc, time_memc
    
def main():
    #Calculate start time for file upload
    start_time = time.time() 
    #Creating new bucket with name cloudbuc3
    bucket_name = "cloudbuc3"
    bucket = s3.create_bucket(bucket_name)
    #define key for bucket     
    k = Key(bucket)
    k.key = 'data.csv'
    #Uploading data to bucket with key
    k.set_contents_from_string(myfile)
    #Calculate end time for file upload
    stop_time = time.time()
    #Calculate total time for file upload
    upload_time = stop_time - start_time
    print "Time taken to upload file: " + str(upload_time) + "sec"  
    create_table()
    query_time = get_query(1,False)
    for x in (1000,5000,20000):
        query_time = get_query(x,False)
        print "Time taken to execute " + str(x) + " queries is: "
        print "Without memcache: " + str(query_time[0])
        print "With memcache: " + str(query_time[1])
    print "Queries for tuples in range of 200-800: "
    for x in (1000,5000,20000):
        query_time = get_query(x,False)
        print "Time taken to execute " + str(x) + " queries is: "
        print "Without memcache: " + str(query_time[0])
        print "With memcache " + str(query_time[1])

#Call the main function 
#import pdb
#pdb.set_trace()         
main()
#Save the database changes
connobj.commit()
#END ALL
