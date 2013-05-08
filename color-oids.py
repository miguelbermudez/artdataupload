import psycopg2
import requests
import json
import sys


# Connect to an existing database
try:
    #conn = psycopg2.connect("dbname='embeeapi_test' user='api_user' host='localhost' password='api_user'")
    conn = psycopg2.connect("dbname='embeeapi_test' user='miguelb' host='localhost'")
except:
    print "I am unable to connect to the database"

# Open a cursor to perform database operations
cur = conn.cursor()

# Get JSON for work colors
#r = requests.get('https://s3.amazonaws.com/metlinkdb/test.json')                    #test file
r = requests.get('https://s3.amazonaws.com/metlinkdb/embeeapi_work_color.json')      #real file

#iterate over returned json docs and populate id => oid dict
counter = 1
for item in r.iter_lines():
    work_dict = json.loads(item)
    for field in work_dict.keys():
        value = work_dict[field]
        #if field in "title": print "%s" % value
        if field == "_id":
            oid = value['$oid']

            #upload data to db
            cur.execute("""INSERT INTO "TEMP_id_oid" (oid) VALUES (%s)""", [oid])

            #print "\t%s => %s" % ("oid", oid)
            sys.stdout.write("#%s: %s => %s       \r" % (counter, "oid", oid))
            sys.stdout.flush()
            counter += 1

# Make the changes to the database persistent
conn.commit()

# Close communication with the database
cur.close()