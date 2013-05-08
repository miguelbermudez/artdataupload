import psycopg2
import requests
import json
import sys


def get_id_from_oid(oid):
    #find color in temp-id-oid lookup table
    cur.execute('SELECT * FROM "TEMP_id_oid" WHERE oid = %s', [oid])
    result = cur.fetchone()
    return result[0]


def insert_new_work(w_dict):
    #upload work data to db
    sql = 'INSERT INTO "Entry_work" ("accessionNumber", artist, "catalogueEntry", "catalogueRaisonne", classification, ' \
                        '"creditLine", date, description, designer, dimensions, dynasty, "galleryLabel", geography, ' \
                        '"imageUrl", imgfilename, markings, medium, period, provenance, reign, "rightsReproduction", ' \
                        'title, workid, workurl) VALUES (%(accessionNumber)s, %(artist)s, %(catalogueEntry)s, ' \
                        '%(catalogueRaisonne)s, %(classification)s, %(creditLine)s, %(date)s, ' \
                        '%(description)s, %(designer)s, %(dimensions)s, %(dynasty)s, %(galleryLabel)s, %(geography)s, ' \
                        '%(imageUrl)s, %(imgfilename)s, %(markings)s, %(medium)s, %(period)s, %(provenance)s, ' \
                        '%(reign)s, %(rightsReproduction)s, %(title)s, %(workid)s, %(workurl)s);'

    try:
        cur.execute(sql, w_dict)
    except psycopg2.IntegrityError:
        conn.rollback()

    return cur.lastrowid


def insert_new_ref_color(field, work_id, color_id):
    color_join_table = r'"Entry_work_' + field.lower() + r'"'

    #upload work color data to appropriate color field in db
    sql = 'INSERT INTO %s (work_id, workcolor_id) VALUES (%%s, %%s);' % color_join_table

    try:
        cur.execute(sql, (work_id, color_id))
    except:
        conn.rollback()

    return cur.lastrowid


def insert_new_ref_text(field, work_id, text):
    text_join_table = r'"Entry_' + field.lower() + r'"'

    #upload work color data to appropriate color field in db
    sql = 'INSERT INTO %s (entry, work_id) VALUES (%%s, %%s);' % text_join_table

    try:
        cur.execute(sql, (text, work_id))
    except psycopg2.IntegrityError:
        conn.rollback()

    return cur.lastrowid


#------------------------------------------------------------------------------------------------------------
# Connect to an existing database
try:
    #conn = psycopg2.connect("dbname='embeeapi_test' user='api_user' host='localhost' password='api_user'")
    conn = psycopg2.connect("dbname='embeeapi_test' user='miguelb' host='localhost'")
except:
    print "I am unable to connect to the database"

# Open a cursor to perform database operations
cur = conn.cursor()

# Get JSON for work colors
r = requests.get('https://s3.amazonaws.com/metlinkdb/test.json')                      #test file
# r = requests.get('https://s3.amazonaws.com/metlinkdb/embeeapi_work.json')           #real file

#referenced fields
referenced_color_fields = ['palette', 'dominantcolor', 'searchbycolors', 'mostsaturated', ]
referenced_text_fields = ['references', 'exhibitionHistory', 'notes']

#iterate over returned json docs and populate id => oid dict
counter = 1
for item in r.iter_lines():
    work_dict = json.loads(item)

    #first creat and insert new work
    work_id = insert_new_work(work_dict)

    for field in work_dict.keys():
        value = work_dict[field]

        if field in referenced_color_fields:
            #loop through value which is a list of work_colors
            for wc in value:
                color_oid = wc['$id']['$oid']
                color_id = get_id_from_oid(color_oid)
                insert_id = insert_new_ref_color(field, work_id, color_id)

        if field in referenced_text_fields:
            #loop through value which is a list of text ref fields
            for text_item in value:
                insert_id = insert_new_ref_text(field, work_id, text_item)

    sys.stdout.write("#%s Work: %s         \r" % (counter, work_dict['title']))
    sys.stdout.flush()
    counter += 1


# Make the changes to the database persistent
conn.commit()

# Close communication with the database
cur.close()

