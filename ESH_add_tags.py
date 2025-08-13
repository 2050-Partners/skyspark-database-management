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

#first, get list of all site IDs
with open_haystack_client(uri, username, password) as haystack_client:
    meta, data = haystack_client.read_all("site").to_pandas_all()
    siteIds = data.id.values

#Update the sites with new metadata tags
with open_haxall_client (uri, username, password) as haxall_client:

    for id in siteIds:

        siteRef = Ref(id.val)

        #query entire rec we want to modify to get the mod tag
        rec = haxall_client.read_by_id(siteRef)

        #define new tag to add to rec
        rec["ericTagTest"] = "test tag for each site"

        #commit update to rec and capture response
        meta, rec_modified_grid = haxall_client.commit_update(rec).to_pandas_all()


print("end:", datetime.now())

