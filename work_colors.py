import psycopg2
import requests
import json
import sys


# Connect to an existing database
try:
    # conn = psycopg2.connect("dbname='embeeapi_test' user='api_user' host='localhost' password='api_user'")
    conn = psycopg2.connect("dbname='embeeapi_test' user='miguelb' host='localhost'")
except:
    print "I am unable to connect to the database"

# Open a cursor to perform database operations
cur = conn.cursor()

# Get JSON for work colors
#r = requests.get('https://s3.amazonaws.com/metlinkdb/test_color.json')                   #test file
r = requests.get('https://s3.amazonaws.com/metlinkdb/embeeapi_work_color.json')           #real file

#iterate over returned json docs and populate id => oid dict
counter = 1
for item in r.iter_lines():
    work_dict = json.loads(item)
    oid = work_dict['_id']['$oid']

    #find color in temp-id-oid lookup table
    cur.execute('SELECT * FROM "TEMP_id_oid" WHERE oid = %s', [oid])
    result = cur.fetchone()
    result_id = result[0]

    #add mapped id to work dict
    work_dict['id'] = result_id

    #upload data to db
    sql = 'INSERT INTO "Entry_workcolor" (id, red, green, blue, intvalue, hexvalue) VALUES (%(id)s, %(red)s, %(green)s, %(blue)s, %(intvalue)s, %(hexvalue)s);'

    try:
        cur.execute(sql, work_dict)
        sys.stdout.write("#%s Adding color (hex): %s  oid: %s          \r" % (counter, work_dict['hexvalue'], oid))
        sys.stdout.flush()

    except psycopg2.IntegrityError:
        conn.rollback()

    counter += 1


# Make the changes to the database persistent
conn.commit()

# Close communication with the database
cur.close()