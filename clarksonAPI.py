# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 14:11:47 2022

@author: BarL
"""
import requests
import math
import pandas as pd
import time
import sys

def genToken(username,password):
    """
    get token for user
    """
    body = {
              "username": username,
              "password": password 
    }

    headers = {
            "accept": "application/json",
            "Content-Type": "application/json"
    }
    
    response = requests.post("https://www.clarksons.net/api/user/ApiAuthentication/GenerateAuthenticationToken", json=body, headers=headers)
    
    if (response.status_code == 200) and (response.json()["success"] == True):
        generatedToken = response.json()["token"]
        print(f"Your Token is:\n{generatedToken}")
        return generatedToken
    else:
        print("Ensure username/password is correct!")
        return None
 
        
def GetAPIHeaders(ExcelFilePath, Columns):
    """
    normzlize / convert labels to API labels
    """
    NormalizedColumns = {}
    API_Def_Data = pd.read_excel(ExcelFilePath)
    for column in Columns:
        #print(column)
        normalizedColumn = API_Def_Data[API_Def_Data['Label'] == column]
        try: 
            normalizedColumn = (normalizedColumn['API Field Name']).values[0]
            NormalizedColumns[column] = normalizedColumn
        except:
            print(column)
    return NormalizedColumns
        
        
def GetData(url,username,password):
    # Declare variables
    fullDataset = []
    maxPageSize = 1000 #This is set by the API so, 1000 is the upper limit on the size of a page.
    url = url+f"?PageSize={maxPageSize}"
    # Get Bearer Token
    token = genToken(username,password)
    if token is None: 
        sys.exit()
                    
    headers = {"accept" : "application/json", "Authorization" : f"Bearer {token}"}
    responseSuccess = False
    
    # Example of complex url where you can page, sort and filter a data set: 
    # "https://www.clarksons.net/api/vessels?Sort=BuiltDate&Order=DESC&BuiltDate.Min=2021-01-01"               
    #url = f'https://www.clarksons.net/api/vessels?PageSize={maxPageSize}&Databases=Fleet&Fleet Types=All Ships&grossTonnage.min=100&grossTonnage.max=499167'
    
    
    # Declare varialbes
    hasMorePages = True
    page = 1
    maxRetries = 3
    recordCount = None
    fullDataset = []
    
    # Loop through each page of results
    while hasMorePages == True:   
        vesselsFrom = page * maxPageSize - maxPageSize + 1
        vesselsTo = page * maxPageSize
        
        print(f"Requesting vessels {vesselsFrom} to {vesselsTo} of { recordCount if recordCount is not None else 'unknown'}...")
        
        pageURL = url.replace("?" , f"?Page={page}&")
        pageDataReturned = False        
    
        # Retry loop to get current page
        for requestNumber in range(1, maxRetries):        
            pageResponse = requests.get(pageURL, headers=headers)
            pageResponseData = pageResponse.json()
            if pageResponse.status_code == 200:
                for vessel in pageResponse.json()["results"]:
                    fullDataset.append(vessel)
    
                pageDataReturned = True
                page = page + 1                
               
                if recordCount is None:
                    recordCount = pageResponse.json()["recordCount"]
                    numOfPages = math.ceil(recordCount/maxPageSize)
                    
                hasMorePages = page <= numOfPages
                break
            elif pageResponse.status_code == 429:
                limit = pageResponseData["limit"]
                period = pageResponse.json()["period"]
                retryAfter = pageResponse.json()["retryAfter"] + 5
                print(f"429 - Too Many Requests : you exceeded the limit of {limit} requests within {period}. Waiting for {retryAfter} seconds and will then continue.")
                time.sleep(retryAfter)
            elif pageResponse.status_code == 401:
                print("401 - Unauthorised")
                #print(f"{response.json()}")
                print(f"Try:{requestNumber}. Will regenerate a token, and try to request data again.")
                token = genToken()
                headers["Authorization"] = f"Bearer {token}"
            elif pageResponse.status_code == 400:
                print("400 - Bad Request")
                #print(f"{response.json()}")
                #print(f"Skipped page {Page}")
                break
            elif pageResponse.status_code == 500:
                print("500 - Internal Server Error")
                print(f"Exception ID: {pageResponse.json()['exceptionId']}")
                print(f"Exception Message: {pageResponse.json()['message']}")
                #print(f"Skipped page {Page}")
                break
            else:
                print(f"{pageResponse.status_code} - Error")
                break            
        # Exit process if unable to get page
        if pageDataReturned == False:
            print("Maximum number of tries exceeded.")
            break   
    return fullDataset


ExcelFilePath = "D:/Imri/API_Definition.xlsx"
Columns = ["Name","GT","Dwt","Built Date","Main Status","Owner","Builder","Contract Date","Flag State","Type","Group Owner","Age","LDT","BWTS Ind",
           "Deballast Cap (t/h)","Pump Cap (cu m)","Classification Society","Last Hull Survey Date","Hull Survey Due Date","Keel Laid Date",
           "Owner Nationality/Region","Group Owner Full Company Name","Manager Full Company Name","BWMS 1 Number","BWMS Status",
           "BWMS 1 Designer Company","BWMS 1 Model","BWMS 1 Capacity (m3/h)","BWMS Technology Type","IMO Number","BWMS Detail"]

NormalizedColumns = GetAPIHeaders(ExcelFilePath,Columns)
NormalizedColumns = list(NormalizedColumns.values())
#age_sex = titanic[["Age", "Sex"]]

# set your use name and password
username = "imri@filtersafe.net"
password =  "Clarksons1!!" 
#set url
url = f"https://www.clarksons.net/api/vessels"
#get data
fullDataset = GetData(url,username,password)
print("Finished requesting all vessels")
df = pd.DataFrame(fullDataset)

df_subset = df[NormalizedColumns]
# Use .to_csv on the dataframe to automaitcally ouput the data to a csv file; index=False indicates to not include the number of each row in the output.
df_subset.to_csv("vesselSampleDataset.csv", index=False)
# Use .to_excel on the dataframe to automatically output the data to an excel file; index=False indicates to not include the number of each row in the output.
df_subset.to_excel("vesselSampleDataset.xlsx", index=False)
# This shows off the dataframe in Jupyter Lab/Notebook
df_subset


                 
                 