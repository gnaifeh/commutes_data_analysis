#!/usr/bin/python

import json
import time
import urllib
import urllib2
#import pandas as pd
import numpy as np
from numpy import division
import MySQLdb as mdb
from datetime import datetime
import MySQL_data_file as MySQL_data
import query_data_file

#Neighborhoods to be used
russian_hill = "Russian Hill, San Francisco, CA"
north_beach = "North Beach, San Francisco, CA"
pacific_heights = "Pacific Heights, San Francisco, CA"
outer_richmond = "Outer Richmond, San+Francisco, CA"
outer_sunset = "Outer Sunset, San Francisco, CA"
mission_district = "Mission District, San Francisco, CA"
noe_valley = "Noe Valley, San+Francisco, CA"
berkeley = "Berkeley, CA"
oakland = "Oakland, CA"
financial_district = "Financial District, San Francisco, CA"
mountain_view = "Mountain View, CA"
#russian_hill = "Russian Hill, San Francisco, CA"
#north_beach = "North Beach, San Francisco, CA"

residential_neighborhoods = [russian_hill, north_beach, pacific_heights, outer_richmond,\
                            outer_sunset, mission_district, noe_valley,\
                            oakland, berkeley]
work_neighborhoods = [oakland, financial_district, mountain_view]
residential_coordinates = ['37.8010963,-122.4195558', '37.8060532,-122.4103311',\
                        '37.7925153,-122.4382307', '37.777677,-122.49531',\
                         '37.755445,-122.494069', '37.7598648,-122.4147977',\
                            '37.7502378,-122.4337029', '37.8043637,-122.2711137',\
                             '37.8715926,-122.272747']
work_coordinates = ['37.8043637,-122.2711137', '37.7945742,-122.3999445',\
                    '37.3860517,-122.0838511']


#connect to database and execute query
#does not return anything
def query_db(command):
    con = mdb.connect(MySQL_data.my_sql_host, MySQL_data.my_sql_user,\
                        MySQL_data.my_sql_passwd,\
                        MySQL_data.my_sql_database)
    cur = con.cursor()
    cur.execute(command)
    con.commit()
    con.close()


#query the Bing maps API wiht a given origin and destination
#returns a json object
def queryMmaps(query_origin, query_destination, mmaps_api_key):
    try_counter = 1
    while try_counter < 6:
        try:
            this_url = """http://dev.virtualearth.net/REST/v1/Routes?\
wayPoint.1={}&\
wayPoint.2={}&\
optimize=timeWithTraffic&\
key={}""".\
format(query_origin, query_destination, query_data_file.mmaps_api_key)
            mmaps_query = urllib2.urlopen(this_url)
            query_result = json.loads(mmaps_query.read())
            return query_result
        except:
            print "HTTP Request Failure on mmaps (morning). Attempt #{}".format(str(try_counter))
            time.sleep(60)
            try_counter += 1

#function that takes the json object and returns an
#np array to be entered into the database
def createEntry(query_result, query_time, query_origin, \
                    query_destination, travel_mode):
    if 'statusDescription' in query_result:
        if query_result['statusDescription'] != 'OK':
            print "There is an issue with the high level status description.\
                    It is currently listed as {}".format(status_description)
    travel_duration = "NULL"
    travel_duration_traffic = "NULL"
    travel_distance = "NULL"
    traffic_congestion = "NULL"
    try:
        resource_sets_dict = query_result['resourceSets'][0]#use '0' as an index because the dictionary object is embedded in a list
        resources_dict = resource_sets_dict['resources'][0]#use '0' as an index because the dictionary object is embedded in a list
        if 'travelDuration' in resources_dict:
            travel_duration = resources_dict['travelDuration']
        if 'travelDurationTraffic' in resources_dict:
            travel_duration_traffic = resources_dict['travelDurationTraffic']
        if 'travelDistance' in resources_dict:
            travel_distance = resources_dict['travelDistance']
        if 'trafficCongestion' in resources_dict:
            traffic_congestion = resources_dict['trafficCongestion']
    except:
        print "Exception triggered when trying to query 'resources_dict' object"
    entry_array = np.array([query_time, query_origin,\
                            query_destination, travel_mode, travel_duration,\
                            travel_duration_traffic, travel_distance,\
                            traffic_congestion])
    return entry_array


def saveToDatabase(array_of_entries):
    for array in array_of_entries:
        query_db("""INSERT INTO mmaps_data(datetime,origins,destinations,travel_mode,duration,duration_traffic,distance,congestion)
                    VALUES ('{}','{}','{}','{}',{},{},{},'{}')"""\
                    .format(array[0],array[1],array[2],array[3], array[4],array[5],array[6],array[7]))


def run_trip (start_neighborhoods, start_coordinates,\
                end_neighborhoods, end_coordinates):
    array_of_entries = np.array([])
    for i in np.arange(len(start_neighborhoods)):
        for n in np.arange(len(end_neighborhoods)):
            if start_neighborhoods[i] != end_neighborhoods[n]:
                query_origin = start_coordinates[i]
                query_destination = end_coordinates[n]
                mmaps_api_key = query_data_file.mmaps_api_key
                query_time = datetime.now().isoformat(' ')
                travel_mode = "driving"
                query_result = queryMmaps(query_origin, query_destination,\
                                            mmaps_api_key)
                entry_array = createEntry(query_result, query_time,\
                                            start_neighborhoods[i],\
                                            end_neighborhoods[n],\
                                            travel_mode)
                array_of_entries = np.append(array_of_entries,\
                                                entry_array, axis=0)
    number_of_columns = 8
    number_of_rows = (len(array_of_entries))/number_of_columns
    array_of_entries = np.reshape(array_of_entries,\
                                    (number_of_rows, number_of_columns))
    saveToDatabase(array_of_entries)


run_trip(residential_neighborhoods, residential_coordinates, work_neighborhoods, work_coordinates)
