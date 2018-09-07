import sys
import os
import csv
import zipfile

from StringIO import StringIO

import boto3

from botocore.handlers import disable_signing

import psycopg2
from psycopg2 import sql, extras


"""
Function used to create connection to postgresql database through the psycopg2 module. Credentials are assumed to be 
set as environmental variables.
"""
def connect_to_postgresql():

    postgres_user = os.getenv('POSTGRES_USER', 'default')
    postgres_pass = os.getenv('POSTGRES_PASS', 'default')
    postgres_dbname = os.getenv('POSTGRES_DBNAME', 'default')

    try:
        conn = psycopg2.connect(user=postgres_user, password=postgres_pass, dbname=postgres_dbname)
    except:
        print "psycopg2 cannot connect to postgres database"
        return -1

    return conn


"""
Function used to create connection to citibike s3 bucket. Because this is a public dataset, there is no need for
credentials. Therefore, the s3 client is set to disable_signing, defined by the botocore module
"""
def connect_to_public_s3(bucket):

    s3 = boto3.resource('s3')
    s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
    
    try:
        s3_bucket = s3.Bucket(bucket)
    except:
        print "Cannot connect to s3 bucket: " + bucket
        return -1

    return s3_bucket


"""
data_for_month gets a copy of the bike data in the s3 bucket according to the bucket key's dated name, reads it into memory,
uncompresses the data if it is zipped, reads the corresponding files within the unzipped object (In this exercise, there
is always one csv file, but the algorithm allows for the possibility that there are more), creates a postgres table 
(if it doesn't already exist) and copies the data into the table. The copy_from function performs the best with reading
in large delimited files
"""
def data_for_month(month, table_name, checked_table_exists, s3_bucket, bucket_key_format, conn):

    cur = conn.cursor()

    bucket_key = bucket_key_format % (month)
        
    try:
        file_object = s3_bucket.Object(bucket_key)
    except:
        print "s3 bucket does not contain key: " + bucket_key
        return (checked_table_exists, -1)

    if 'Body' in file_object.get():
        file_object_body = file_object.get()['Body']
    else:
        print "Body not found in file object: " + bucket_key 
        return (checked_table_exists, -1)

    #Write bucket key data into stringIO to go through files in memory

    bike_data = StringIO()
    bike_data.write(file_object_body.read())

    if zipfile.is_zipfile(bike_data):
        bike_data = zipfile.ZipFile(bike_data)

    for bike_data_file in bike_data.namelist():

        try:
            data_csv = bike_data.open(bike_data_file)
        except:
            continue

        #Get the first row to get column names, if creating the table for the first time,
        #or, to get the data_reader to skip the first row for reading in the data

        data_reader = csv.reader(data_csv, delimiter=',')
        title_row = data_reader.next()

        if not title_row:
            continue

        #Postgres method for checking existence of tables. 

        if not checked_table_exists:

            check_table_sql = "SELECT 1 FROM pg_tables WHERE tablename = %s;"

            cur.execute(check_table_sql, (table_name,))
            
            if cur.fetchone() == None:
                create_trip_fact_table(title_row, table_name, conn)

            checked_table_exists = True
        
        cur.copy_from(data_csv,table_name,sep=',')

        conn.commit()

    return (checked_table_exists, 0)


"""
Called by the data_for_month function, create_trip_fact_table is called if the table (either trip_fact or
trip_fact_stg) does not exist. Using the sql.Identifier function to guard the table identfier names (i.e. 
column names, table names) against SQL injection, the table is created, getting the column names from
the title row in the csv file. Because we are not assuming we have knowledge of all the column names 
of the csv file, by default the columns are cast as type CHAR
"""
def create_trip_fact_table(title_row, table_name, conn):

    cur = conn.cursor()

    sql_identifier_array = [sql.Identifier(table_name)]
    create_table_string = "CREATE TABLE {} ("

    for column_title in title_row: 
        sql_identifier_array.append(sql.Identifier(column_title))
        create_table_string += "{} VARCHAR,"

    create_table_string = create_table_string[:-1]+");"

    cur.execute(sql.SQL(create_table_string).format(*sql_identifier_array))

    conn.commit()


"""
Select_from_table is used whenever a function needs to iterate through the results of a select statement.
SQL query is composed by using the sql.Identifier function for the table names and fields. The position of
the table names within the query are returned as a dictionary, id_idx (identifier_index), to parse through 
the returned rows. The first row and the cursor are also returned.
"""
def select_from_table(cur, id_names, fetch_sql):

    id_idx = {id_names[i]: i for i in range(len(id_names))}

    sql_identifier_array = [sql.Identifier(name) for name in id_names]

    cur.execute(sql.SQL(fetch_sql).format(*sql_identifier_array))

    row = cur.fetchone()

    return (id_idx, row, cur)


"""
extract_from_trip_fact creates a dictionary that uses the station pair as a key, and the number
of trips between that pair as the value. Station pairs are first assembled using their ids, and a map
of the ids to the names is also created. If there are multiple stations assigned to an id, only the first one
is kept. A final dictionary is created that uses the station names as the key, and is assigned two values:
the first is an initialization, declaring that the station pair is not already in most_used_routes. The
second is the number of trips.
"""
def extract_from_trip_fact(table_name, conn):

    #Indices of start and end station within tuple key
    START_STATION = 0
    END_STATION = 1

    total = {}
    names = {}
    total_names = {}

    id_names = [
        "start station id",
        "start station name",
        "end station id",
        "end station name",
        table_name
        ]

    fetch_sql = "SELECT {}, {}, {}, {} FROM {};"

    id_idx, row, cur = select_from_table(conn.cursor(), id_names, fetch_sql)

    #id_idx (identifier index) maps the column name the position of column within the row
    #Populate the total dictionary with station pair tuples, with values set to number of trips

    while row:
        if (row[id_idx["start station id"]], row[id_idx["end station id"]]) not in total:
            
            total[ (row[id_idx["start station id"]], row[id_idx["end station id"]]) ] = 1
        else:
            total[ (row[id_idx["start station id"]], row[id_idx["end station id"]]) ] += 1


        #Create a dictionary that maps station id to station name

        if row[id_idx["start station id"]] not in names:

            names[row[id_idx["start station id"]]] = row[id_idx["start station name"]]

        if row[id_idx["end station id"]] not in names:

            names[row[id_idx["end station id"]]] = row[id_idx["end station name"]]

        row = cur.fetchone()


    #Map station ids within key tuples to station names.
    #route_id in value pair initialized to None (Later used to check against most_used_routes)
    #Second element in value pair is total number of trips

    for key in total:
        if (names[key[START_STATION]], names[key[END_STATION]]) not in total_names:

            total_names[ (names[key[START_STATION]], names[key[END_STATION]]) ] = {'route_id': None, 'num_trips': total[key]}            
        else:
            total_names[ (names[key[START_STATION]], names[key[END_STATION]]) ]['num_trips'] +=  total[key]

    return total_names


"""
A hint towards distributed computing: In this script, most_used_routes is read into memory
to check against the new month's trips between station pairs within a dictionary. If this project were to
scale up, mapReduce can be used to reduce the old trip data and new incoming data by key, and add their
values. In this case, it is done serially. 
"""
def reduceByKey_most_used_routes(total_names, conn):

    id_names = [
        "route_id",
        "start_station_name",
        "end_station_name",
        "num_trips"
        ]

    fetch_sql = "SELECT {}, {}, {}, {} FROM most_used_routes;"
    
    id_idx, row, cur = select_from_table(conn.cursor(), id_names, fetch_sql)


    #If the station pair in the dictionary matches the station pair in most_used_routes, 
    #set the route_id element of the dictionary value to the route_id value in most_used_routes,
    #and update the dictionary value of the number of trips

    while row:
        if (row[id_idx["start_station_name"]], row[id_idx["end_station_name"]]) in total_names:

            total_names[ (row[id_idx["start_station_name"]], row[id_idx["end_station_name"]]) ]["route_id"] = row[id_idx["route_id"]] 

            total_names[ (row[id_idx["start_station_name"]], row[id_idx["end_station_name"]]) ]["num_trips"] += row[id_idx["num_trips"]] 

        row = cur.fetchone()


"""
If the only_insert option is true (as would be the case after only reading in the older months), the trip data is inserted 
into most_used_routes from the data dictionary. If it is set false, then depending on whether the station pair key is
already in most_used_routes, the new data is either inserted into the table as a new row and new station pair, or is used 
to update the old data. Determining whether to INSERT or UPDATE within the python script performs much better than
using 'UPSERT" methods within SQL
"""
def modify_most_used_routes(total_names, only_insert, conn):

    #Indices of start and end station within tuple key
    START_STATION = 0   
    END_STATION = 1

    cur = conn.cursor()

    insert_sql = "INSERT INTO most_used_routes (start_station_name, end_station_name, num_trips) VALUES ( %s,%s,%s);"

    if only_insert:
        insert_array = [(key[START_STATION], key[END_STATION], total_names[key]["num_trips"]) for key in total_names]
    else:
        insert_array = []
        update_array = []
        
        for key in total_names:
            if total_names[key]["route_id"] == None:

                insert_array.append( (key[START_STATION], key[END_STATION], total_names[key]["num_trips"]) )
            else:
                update_array.append( (total_names[key]["num_trips"], total_names[key]["route_id"]) )

        update_sql = "UPDATE most_used_routes SET num_trips = %s WHERE route_id = %s;"

        extras.execute_batch(cur,update_sql, update_array)           
    
    extras.execute_batch(cur,insert_sql, insert_array)           

    conn.commit()


def main(*argv):

    #The initial options. Future implementation can have these options read in argv, or as a input JSON file 
 
    bucket = "tripdata"
    bucket_key_format = "2018%.02d-citibike-tripdata.csv.zip"
    first_month = 1
    last_month = 7

    conn = connect_to_postgresql()
    
    if conn == -1:
        return -1

    cur = conn.cursor()

    s3_bucket = connect_to_public_s3(bucket)

    if s3_bucket == -1:
        return -1

    #1. Read in citibike data from January 2018 to June 2018

    checked_table_exists = False

    for month in range(first_month, last_month):
        checked_table_exists, retval = data_for_month(month, "trip_fact", checked_table_exists, s3_bucket, bucket_key_format, conn)

        if retval == -1:
            return -1


    #2. Create most_used_routes and insert the data from the older months. (The station pairs are not symmetric)

    cur.execute("CREATE TABLE most_used_routes (route_id serial PRIMARY KEY, start_station_name VARCHAR, end_station_name VARCHAR, num_trips INTEGER);")
    conn.commit()

    total_names = extract_from_trip_fact("trip_fact", conn)


    #Since there is no data to update, set only_insert to TRUE in modify_most_used_routes

    modify_most_used_routes(total_names, True, conn)


    #3. Read in citibike data from July 2017

    _, retval = data_for_month(last_month, "trip_fact_stg", False, s3_bucket, bucket_key_format, conn)

    if retval == -1:
        return -1


    #4. Update most_used_routes with the new data

    total_names = extract_from_trip_fact("trip_fact_stg", conn)

    #Using older month data, update total_names dictionary, with station pair keys and route-trip values, in reduceByKey_most_used_routes
    #Insert new pairs and update old ones in modify_most_used_routes (only_insert set FALSE)

    reduceByKey_most_used_routes(total_names, conn)

    modify_most_used_routes(total_names, False, conn)

    conn.close()
    
if __name__=="__main__":
    main(*sys.argv)
