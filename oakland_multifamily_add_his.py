'''
Script to add histories to points in the Oakland multifamily site in the Code Readiness Research project in SkySpark.
Iterate through a folder of CSV files. Each CSV contains a SkySpark point ID in the filename. Using this ID, add
the data in the CSV as a history to the point in SkySpark.
'''

### IMPORT ###
import os
import time
import math
import csv
import urllib
import glob
from zoneinfo import ZoneInfo
from phable import open_haxall_client, open_haystack_client, DateRange, Ref, Grid, Number, Marker
from datetime import datetime, date, timedelta, timezone
from dotenv import load_dotenv

from skyspark_database_funcs import *

### FUNCTION ###



### MAIN ###  
print("start:", datetime.now(), "\n")

#load credentials from .env file
username, password, uri = loadCredentials() #"https://codereadiness.com/api/crResearch"
uri = "https://codereadiness.com/api/crResearch"

#csvFolder = "/Users/ericswanson/Library/CloudStorage/OneDrive-2050Partners,Inc/Documents/test-csv" JUST FOR TESTING
csvFolder = "/Users/ericswanson/Library/CloudStorage/OneDrive-SharedLibraries-2050Partners,Inc/Elise Wall - sensorFiles"
csvList = glob.glob(csvFolder + '/*.csv')
#csvList.sort()

errorCount = 0
idFailList = []
for i,file in enumerate(csvList):

    # #pick up where script left off
    # if i > 13:
    nextFile = False

    while nextFile == False:

        #create the Ref from the point ID in the filename
        filename = file.split('/')[-1]
        pointId = filename.split('_')[0]
        #idRef = Ref("p:firstThing:r:" + pointId) JUST FOR TESTING
        idRef = Ref("p:crResearch:r:" + pointId)
        
        with open_haystack_client(uri, username, password) as haystack_client:

            #each point may have different units, so need to grab those from SkySpark before creating a grid of data
            pointUnits = haystack_client.read_by_id(idRef)['unit']

            #Create a mock Haystack grid for the point history from the csv file
            his = csv2grid(file, units=pointUnits)
            mid = math.ceil(len(his)/2)

            try:
                response = haystack_client.his_write_by_id(idRef, his)
                nextFile = True
                print("successful first attempt for", idRef, "-- i =", i)

            # except urllib.error.URLError as eUrl:
            #     print(f"URL error for", idRef, "-- i =", i)
            #     test = 0

            # except urllib.error.URLError as e:
            except Exception as e:

                if isinstance(e, urllib.error.URLError):
                    print(f"{e} error for", idRef, "-- skipping file -- i =", i)
                    nextFile = True
                    idFailList.append(idRef)

                elif isinstance(e, urllib.error.HTTPError):
                    print(f"{e} error for", idRef, "-- Trying again -- i =", i)

                    #if you get a 500 error back, the grid is mostly likely too big. Try again but with half the grid at a time
                    if e.status == 500:

                        print("# of rows for point history =", len(his))

                        try:
                            response = haystack_client.his_write_by_id(idRef, his[0:mid])
                            response = haystack_client.his_write_by_id(idRef, his[mid:-1])
                            nextFile = True
                            print("successful write for", idRef, "-- i =", i)

                        except Exception as e:
                            print('Failed again for', idRef, " -- i =", i)
                            errorCount += 1
                            nextFile = True
                            idFailList.append(idRef)

                    #if you get a 502 error back, there's a server error and it's most likely overloaded. Wait a few minutes and try again.
                    elif e.status == 502:

                        #wait 5 minutes
                        time.sleep(300)

                        try:
                            response = haystack_client.his_write_by_id(idRef, his)
                            nextFile = True
                            print("successful write for", idRef, "-- i =", i)

                        except Exception as e:
                            print('Failed again for', idRef, " -- i =", i)
                            nextFile = True
                            errorCount += 1
                            idFailList.append(idRef)


print("Number of errors", errorCount)
print("IDs that failed", idFailList)

print("\nEnd:", datetime.now())

        


        
    





