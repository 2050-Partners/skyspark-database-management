'''
Python file containing useful functions for SkySpark database management through the API.
'''

### IMPORT ###
import os
import csv
import pandas
from zoneinfo import ZoneInfo
from phable import open_haxall_client, open_haystack_client, DateRange, Ref, Grid, Number, Marker
from datetime import datetime, date, timedelta, timezone
from dotenv import load_dotenv



### ERRORS ###
class EmptyUnitsError(Exception):
    '''
    Error raised when user tries to access a point in SkySpark without specifying units.

    help_msg: A display to help with troubleshooting.
    '''

    help_msg : str




### FUNCTIONS ###
def axon2Data(axon_expr):
    '''
    FUNCTION TO BE CALLED IN R.
    Returning a Pandas DataFrame after running an Axon expression with the SkySpark API. 
    Wrapper function for runAxon().
    Inputs:
        axon_expr - Axon expression to be run by the SkySpark API
    Outputs:
        data (Pandas DataFrame) - Grid of data returned from SkySpark 
    '''
    data, _ = runAxon(axon_expr)
    return data


def axon2PointIds(axon_expr):
    '''
    FUNCTION TO BE CALLED IN R.
    Returning a list of SkySpark point IDs after running an Axon expression with the SkySpark API. 
    Wrapper function for runAxon().
    Inputs:
        axon_expr - Axon expression to be run by the SkySpark API
    Outputs:
        pointIds (list) - list of SkySpark point IDs
    '''
    _, pointIds = runAxon(axon_expr)
    return pointIds


def csv2grid(filepath: str, tz=ZoneInfo('America/Los_Angeles'), units="", returnHeaders=False):
    '''
    Read a csv file to create a (mock Python version of a) Haystack Grid object. 
    The csv file should contain 2 columns: 1 column of data to be used for the point value and 1
    column of the corresponding datetime for that data. There should be 1 header row.

    Inputs:
        filepath (str) - Filepath to the csv file
        tz (ZoneInfo Python object) - ZoneInfo Python object specifying the timezone of the datetime data.
            Optional - default is 'America/Los_Angeles'
        units (str) - Units corresponding to the data values. 
        returnHeaders (bool) - If True, functions returns the history grid and the headers from the csv file. If
            False, function only returns the history grid.
    Outputs:
        his (list) - list of dictionary objects. Each dictionary has the same two fields: 'ts' and 'val'. 'val' is
            data to be used for the point value and 'ts' is the corresponding datetime that data was captured. This list
            is meant to represent a Haystack Grid object.
        headers (list) - Optional output. A list of strings corresponding to the headers of the csv file.
    '''

    if units=="":
        raise EmptyUnitsError("Point units not specified in function call parameters")

    #create mock Haystack grid structure
    his = []

    with open(filepath) as csvfile:
        csvReader = csv.reader(csvfile, delimiter=',')

        # Get the first row as headers. Headers are the key names in the grid dict
        headers = next(csvReader)  
        
        #load in csv into mock grid structure
        for i,row in enumerate(csvReader):

            #convert the date string from the csv into a timezone-aware datetime object
            if row[1] == "NA":
                continue
            else:
                ts = datetime.strptime(row[1], '%Y-%m-%dT%H:%M:%SZ')
                ts = ts.replace(tzinfo = tz)

                #add row of data to the grid. History should only have the keys 'ts' and 'val' (per API)
                #TODO: more efficiently load in data
                hisRow = {'ts': ts, 'val': Number(float(row[0]), units)}
                his.append(hisRow)

    if returnHeaders:
        return his, headers
    else:
        return his
    

def loadCredentials():
    '''
    Loads credentials from a .env file to allow the user to connect to the SkySpark API.
    .env file must live in the same directory as this file. 

    Inputs:
        none
    Returns:
        username (str) - SkySpark username
        password (str) - SkySpark password
        uri (str) - uri to the SkySpark project you'd like to access. For example, https://codereadiness.com/api/firstThing

    Example .env:
        DB_USERNAME="yourUsername"
        DB_PASSWORD="yourPasword"
        DB_URI="https://codereadiness.com/api/firstThing"
    '''

    load_dotenv()
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    uri      = os.getenv("DB_URI")

    return username, password, uri


def runAxon(axon_expr):
    '''
    Run an Axon expression in the SkySpark API and return a Haystack Grid.
    Inputs:
        axon_expr - Axon expression to be run by the SkySpark API
    Outputs:
        data (Pandas DataFrame) - Grid of data returned from SkySpark 
        pointIds (list) - list of SkySpark point IDs
    '''
    username, password, uri = loadCredentials()

    with open_haxall_client(uri, username, password) as client:
        #data is Grid returned from SkySpark
        meta, data = client.eval(axon_expr).to_pandas_all()

        #create a list of point IDs
        pointIds = []
        for id in list(data.id.values):
            pointIds.append(id)

    return data, pointIds