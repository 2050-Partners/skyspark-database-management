'''
Script to add a tag to each existing site in the Energy Smart Homes project in SkySpark.
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
username, password, uri = loadCredentials()

with open_haystack_client(uri, username, password) as haystack_client:

    #get list of all sites
    data, meta = haystack_client.read_all("site")

