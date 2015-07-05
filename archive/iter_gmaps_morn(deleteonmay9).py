
import json
import urllib2
#import pandas as pd
import numpy as np
from numpy import division
import MySQLdb as mdb
from datetime import datetime
import MySQL_data_file as MySQL_data
import query_data_file

#districts
russian_hill = "Russian+Hill+San+Francisco+CA"
north_beach = "North+Beach+San+Francisco+CA"
pacific_heights = "Pacific+Heights+San+Francisco+CA"
outer_richmond = "Outer+Richmond+San+Francisco+CA"
outer_sunset = "Outer+Sunset+San+Francisco+CA"
mission_district = "Mission+District+San+Francisco+CA"
noe_valley = "Noe+Valley+San+Francisco+CA"

oakland = "Oakland+CA"
berkeley = "Berkeley+CA"

soma_district = "SoMA+San+Francisco+CA"
financial_district = "Financial+District+San+Francisco+CA"
mountain_view = "Mountain+View+CA"

def add_districts(list_of_districts):
    district_list_string = ''
    for x in list_of_districts:
        district_list_string = district_list_string + x + "|"
    return district_list_string

current_origins = add_districts([russian_hill, north_beach,\
                            pacific_heights, outer_richmond,\
                            outer_sunset, mission_district, noe_valley,\
                            oakland, berkeley])
current_destinations = add_districts([oakland, financial_district, mountain_view])

travel_mode = "transit"

gmaps_query = urllib2.urlopen("""
https://maps.googleapis.com/maps/api/\
distancematrix/json?\
origins={}&\
destinations={}&\
mode={}&\
key={}&\
departure_time=now""".\
format(current_origins, current_destinations, travel_mode, query_data_file.gmaps_api_key))

query_result = json.loads(gmaps_query.read())

array_of_entries = np.array([])
query_time = datetime.now().isoformat(' ')
query_origins = np.array(query_result['origin_addresses'])
query_destinations = np.array(query_result['destination_addresses'])
#iterate through and print all trips
if query_result['status'] == 'OK':
    query_rows = query_result['rows']
    for i in np.arange(len(query_rows)):
        query_elements = query_rows[i]['elements']
        for n in np.arange(len(query_elements)):
            if query_origins[i] != query_destinations[n]:
                try:
                    #print "Trip from {} to {}".format(query_origins[i],query_destinations[n])
                    if 'duration' in query_elements[n]:
                        this_trip_duration = query_elements[n]['duration']['value']
                    else:
                        this_trip_duration = "NULL"
                    if 'distance' in query_elements[n]:
                        this_trip_distance = query_elements[n]['distance']['value']
                    else:
                        this_trip_distance = "NULL"
                    if 'fare' in query_elements[n]:
                        this_trip_fare = query_elements[n]['fare']['value']
                    else:
                        this_trip_fare = "NULL"
                    #print "Duration:", this_trip_duration
                    #print "Distance:", this_trip_distance
                    #print "Fare:", this_trip_fare
                except:
                    #print error?
                    print "Exception triggered at the element level"
                    if (query_elements[n]['status'] != "OK"):
                        print "Element status is: {}".format(query_elements[n]['status'])
                entry_array = np.array([query_time, query_origins[i],\
                                        query_destinations[n], travel_mode,
                                        this_trip_duration, this_trip_distance,\
                                        this_trip_fare])
                array_of_entries = np.append(array_of_entries, entry_array, axis=0)
else:
    print 'The status of the query was not listed as "Ok"'
    print 'The status of the query was listed as: {}'.format(query_result['status'])
#print datetime.now().isoformat(' ')
array_of_entries = np.reshape(array_of_entries, (len(array_of_entries)/7,7))

#connect to database
def query_db(command):
    con = mdb.connect(MySQL_data.my_sql_host, MySQL_data.my_sql_user, \
                        MySQL_data.my_sql_passwd,\
                        MySQL_data.my_sql_database)
    cur = con.cursor()
    cur.execute(command)
    con.commit()
    con.close()

for array in array_of_entries:
    query_db("""INSERT INTO gmaps_data\
                (datetime,origins,destinations,travel_mode,duration,distance,fare)
                VALUES ('{}','{}','{}','{}',{},{},{})"""\
                .format(array[0],array[1],array[2],array[3], array[4],array[5],array[6]))
