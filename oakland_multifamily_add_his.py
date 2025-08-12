'''
Script to add histories to points in the Oakland multifamily site in the Code Readiness Research project in SkySpark.
Iterate through a folder of CSV files. Each CSV contains a SkySpark point ID in the filename. Using this ID, add
the data in the CSV as a history to the point in SkySpark.
'''

### IMPORT ###
import os
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

#csvFolder = "/Users/ericswanson/Library/CloudStorage/OneDrive-2050Partners,Inc/Documents/test-csv" JUST FOR TESTING
csvFolder = "/Users/ericswanson/Library/CloudStorage/OneDrive-SharedLibraries-2050Partners,Inc/Elise Wall - sensorFiles"
csvList = glob.glob(csvFolder + '/*.csv')

for i,file in enumerate(csvList):

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

        errorCount = 0
        try:
            response = haystack_client.his_write_by_id(idRef, his)

        except urllib.error.HTTPError as eHttp:
            print("HTTP error with", idRef, "full grid. Trying again.")

            #if you get a 500 error back, the grid is mostly likely too big. Try again but with half the grid at a time
            try:
                mid = math.ceil(len(his)/2)
                response = haystack_client.his_write_by_id(idRef, his[0:mid])
                response = haystack_client.his_write_by_id(idRef, his[mid:-1])

            except Exception as e:
                print('Failed again for', idRef)
                errorCount += 1


print("Number of errors", errorCount)

print("\nEnd:", datetime.now())

        


        
    





