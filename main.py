import boto
import boto.dynamodb
import urllib2
import csv
from  boto.dynamodb.condition import *
import bottle
from bottle import route, request, response, template, get, run, HTTPResponse
import json
#bottle = Bottle()

#establish connection to dynamodb
conn = boto.dynamodb.connect_to_region('us-west-2')
response = urllib2.urlopen('http://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_master.csv')
#Read the file data
myfile = response.read()
#Create a file named myfile.csv
filename = open('myfile.csv','w')
#write previously fetched filedata into newly created file
filename.write(myfile)
#Close the file
filename.close()

#function to create table
def create_table():
    try:
        #create schema with hash key as 'id'
        myschema=conn.create_schema(hash_key_name='id',hash_key_proto_value=str)
        #create table using schema
        table=conn.create_table(name='data', schema=myschema, read_units=1, write_units=1)
        print "Table created successfully"
    
    except Exception as e:
        print "Table can't be created: " + str(e)

def insert_data():
    try:
        #open the file 'myfile.csv' in read mode
        myfile = open('myfile.csv','r')
        #DictReader will read the data and create an object of the data in dictionary form
        csv_reader = csv.DictReader(myfile)
        #Skip the header line
        csv_reader.next()
        #get the table    
        table = conn.get_table('data')
        #initialise the counter
        c = 0
        #replace any null string in data with zero
        for row in csv_reader:
            for i in row:
                if row[i] == "":
                    row[i] = "0"
            item = table.new_item(hash_key=str(c),attrs=row)
            print c
            #increment counter
            c += 1
            #it will break the loop if 30000 data is inserted because read write is taking much time 
            if c == 30000:
                break
            #insert the data    
            item.put()
        print "Data inserted successfully" 
        
    except Exception as e:
        print "Something wrong with data: " + str(e)

@bottle.route('/')
def main():
    #commented the create_table() command because table already exists
    #create_table()
    #import pdb
    #pdb.set_trace()
    #commented the insert_data() function because read write operations are taking too much time 
    #insert_data()
    return template('webinterface.tpl')

@bottle.route('/dynamic_query', method = "POST")
def dynamic_query():
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        #import pdb
        #pdb.set_trace()
        #it will fetch the data from tpl file using ajax request
        dict_data = request.forms.dict
        data = {}
        #filter the data
        for x in dict_data:
            if dict_data[x][0]!='':
                data[x] = dict_data[x][0]
        for x in data:
            data[x]= CONTAINS(data[x])
        #run the query
        query_ans= get_data(data)
        #format the result into html form
        table = query_format(query_ans)
        #dump the formatted result
        data = json.dumps(table)
        #send an ajax response
        return HTTPResponse(body=str(data), status=200)
        
def query_format(query_ans):
    table = "<table border='2'><tr><th>level</th><th>index_nsa</th><th>place_id</th><th>hpi_type</th><th>period</th><th> "\
        "hpi_flavor</th><th>frequency</th><th>index_sa</th><th>yr</th><th>id</th><th>place_name</th>"
    ans = " "
    for x in query_ans:
        ans += "<tr>"
        ans += "<td>"+str(x["level"])+"</td>"
        ans += "<td>"+str(x["index_nsa"])+"</td>"
        ans += "<td>"+str(x["place_id"])+"</td>"
        ans += "<td>"+str(x["hpi_type"])+"</td>"
        ans += "<td>"+str(x["period"])+"</td>"
        ans += "<td>"+str(x["hpi_flavor"])+"</td>"
        ans += "<td>"+str(x["frequency"])+"</td>"
        ans += "<td>"+str(x["index_sa"])+"</td>"
        ans += "<td>"+str(x["yr"])+"</td>"
        ans += "<td>"+str(x["id"])+"</td>"
        ans += "<td>"+str(x["place_name"])+"</td>"
        ans += "</tr>"
    table += ans + "</table>"
    return table

def get_data(query):
    #get the table
    table = conn.get_table('data')
    #items = table.scan(scan_filter={'c_id': EQ('1427578')})
    items = table.scan(scan_filter=query)
    #print items.response["Items"]
    return items.response["Items"]
    
if __name__ == '__main__':
    run(host='ec2-52-26-244-170.us-west-2.compute.amazonaws.com', port=8080, debug=True)
