#!/usr/bin/env python
# coding: utf-8

# In[19]:


from requests import request
import json
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import datetime
import matplotlib.pyplot as plt


def fetch_data(url_link):
    # fetches data from URL and returns string variable for executionTime and dataframe with stationBeanList data
    response=request(url=url_link, method='get')
    dictr=response.json()
    recs=dictr['stationBeanList']
    execTime_str=dictr['executionTime']
    df=pd.json_normalize(recs)
    return df,execTime_str
    pass

def enrich_data(data):
    # Enrich data with "stationColor"
    # "brokenDocks": Number of docks that are not functional
    # "stationColor": defined by number of brokenDocks in the station
    # green <= 10 , 10 < yellow < 30, red >= 30
    
    data['brokenDocks']=data.totalDocks-data.availableDocks-data.availableBikes
    
    # creating a list of conditions for station color
    conditions=[
                (data['brokenDocks']<=10)
                ,(data['brokenDocks']>10) & (data['brokenDocks']<30)
                ,(data['brokenDocks']>=30)
                ]

    # creating a list of station colors to match with condition

    colors=['Green','Yellow','Red']

    # creating a new column and using np.select to assign colors
    data['stationColor']=np.select(conditions,colors)
    return data
    pass

def dashboard_data(enriched_data,timestamp):
    # creating a dataframe that will contain data needed for dashboard: date and number of red stations
    execTime=datetime.datetime.strptime(timestamp, '%Y-%m-%d %I:%M:%S %p')
    date=execTime.date()
    dashboard_df = pd.DataFrame.from_records({"red_stations": len(enriched_data[(enriched_data['stationColor']=='Red')])}, index=[date])
    return dashboard_df
    pass

def save_data(processed_data):
    # saving the business data in csv file
    # I assumed that you'll change the variable "file_location" with your accessible location
    # I'm also assuming that this script will be ran automatically once a day and we want to append all the data to one csv file
    # that's why I'm using mode='a'and appending the data without header, I'll attach the column names later in build_dashboard function
    
    processed_data.to_csv(file_location, mode='a', header=False)
    pass

def build_dashboard(file_location):
    df = pd.read_csv(file_location, header = None)
    df.columns =['date','redStations']
    df.plot(x='date',y='redStations')
    
##################################
### Main data ingestion logic ###
##################################

def start_data_ingestion(url):
    data,execution_string= fetch_data(url)
    enriched_data = enrich_data(data)
    processed_data = dashboard_data(enriched_data,execution_string)
    saved_data = save_data(processed_data)
    build_dashboard(file_location)
    
if __name__=="__main__":
    citibikenyc_url = "http://citibikenyc.com/stations/json"
    file_location= "D:\Downloads\processed_data.csv"
    start_data_ingestion(url=citibikenyc_url)


# In[ ]:





# In[ ]:




