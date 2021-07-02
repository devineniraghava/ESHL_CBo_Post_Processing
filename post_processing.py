# -*- coding: utf-8 -*-
"""
Created on Thu Jun 24 13:28:14 2021

@author: Raghavakrishna
"""


import pandas as pd
from datetime import timedelta
from sqlalchemy import create_engine
import numpy as np
import matplotlib.pyplot as plt

def prRed(skk): print("\033[31;1;m {}\033[00m" .format(skk))
def prYellow(skk): print("\033[33;1;m {}\033[00m" .format(skk))
# engine = create_engine("mysql+pymysql://root:Password123@localhost/",pool_pre_ping=True)


#%%

class DataCleaner:
    
    def __init__(self, experiment = "S_I_e0_ESHL", sensor_name = "1a_testo", column_name = 'hw_m/sec'):
        self.times = pd.read_excel("master_time_sheet.xlsx")
        self.experiment = experiment
        self.sensor_name = sensor_name
        self.column_name = column_name
        self.engine = create_engine("mysql+pymysql://wojtek:Password#102@wojtek.mysql.database.azure.com/",pool_pre_ping=True)
        self.engine = engine = create_engine("mysql+pymysql://root:Password123@localhost/",pool_pre_ping=True)

        self.database = self.times[self.times["experiment"] == self.experiment].iloc[0,3]
        self.t0 = self.times[self.times["experiment"] == experiment].iloc[0,1]
        self.tn = self.times[self.times["experiment"] == experiment].iloc[0,2]
        self.exclude = self.times[self.times["experiment"] == experiment].iloc[0,4].split(",")
        
        self.t0_20 = self.t0 - timedelta(minutes = 20)
        self.tn_20 = self.tn + timedelta(minutes = 20)                                
        
    def wind_velocity_indoor(self):
        
        self.df1 = pd.read_sql_query("SELECT * FROM {}.{} WHERE datetime BETWEEN '{}' AND '{}'".format(self.database, self.sensor_name, self.t0_20, self.tn_20), con = self.engine) 
        self.df2 = self.df1.loc[:, ["datetime", "hw_m/sec"]]
        self.df2 = self.df2.set_index("datetime")
        self.df2 = self.df2.truncate(str(self.t0), str(self.tn) )
        
        self.stats = self.df2.describe().iloc[[0,1,2,3,7],:]
        self.stats.columns = ["values"]
        
        self.data = {"values":[self.experiment, self.sensor_name, "hw_m/sec", self.t0, self.tn]}
        
        self.empty_df = pd.DataFrame(self.data, index =['experiment',
                                        'sensor name',
                                        'column name', "Start", "End"])
         
        self.res = pd.concat([self.empty_df, self.stats], axis = 0)
        
        return self.res
    
    
    
    def wind_velocity_outdoor(self):
        
        self.df1 = pd.read_sql_query("SELECT * FROM weather.weather_all WHERE datetime BETWEEN '{}' AND '{}'".format( self.t0_20, self.tn_20), con = self.engine) 
        self.df2 = self.df1.loc[:, ["datetime", "Wind Speed, m/s", "Gust Speed, m/s", "Wind Direction"]]
        self.df2 = self.df2.set_index("datetime")
        self.df2 = self.df2.truncate(str(self.t0), str(self.tn) )
        
        self.stats = self.df2.describe().iloc[[0,1,2,3,7],:]
        self.empty_df = pd.DataFrame(index =['experiment',
                                  'table name', 'Start', 'End'],
                    columns =["Wind Speed, m/s", "Gust Speed, m/s", "Wind Direction"])
        
        self.empty_df.loc["experiment", ["Wind Speed, m/s","Gust Speed, m/s", "Wind Direction"]] = self.experiment
        self.empty_df.loc["table name", ["Wind Speed, m/s","Gust Speed, m/s", "Wind Direction"]] = "weather_all"
        self.empty_df.loc["Start", ["Wind Speed, m/s","Gust Speed, m/s", "Wind Direction"]] = self.t0
        self.empty_df.loc["End", ["Wind Speed, m/s","Gust Speed, m/s", "Wind Direction"]] = self.tn

        self.res = pd.concat([self.empty_df, self.stats], axis = 0)
        
      
        return self.res
    
    def mean_curve(self):
        self.names = pd.read_sql_query('SHOW TABLES FROM {}'.format(self.database), con = self.engine)
        self.names = self.names.iloc[:,0].to_list()
        self.new_names = [x for x in self.names if (x not in self.exclude)]
        
        self.cdf_list = []
        for self.table in self.new_names:
            self.cdf1 = pd.read_sql_query("SELECT * FROM {}.{} WHERE datetime BETWEEN '{}' AND '{}'".format(self.database, self.table, self.t0, self.tn), con = self.engine) 
            self.cdf2 = self.cdf1.loc[:,["datetime", "CO2_ppm"]]
            self.cdf2.columns = ["datetime", str(self.table)]
            self.cdf2 = self.cdf2.set_index("datetime")
            self.cdf_list.append(self.cdf2)
            self.mega_cdf = pd.concat(self.cdf_list,axis = 1).interpolate(method = "linear")
            # self.mega_cdf.columns = self.new_names
            self.mega_cdf["mean"] = self.mega_cdf.mean(axis = 1)
    
        import plotly.io as pio
        
        pio.renderers.default='browser'
        pd.options.plotting.backend = "matplotlib"
        #######################################################################
        pd.options.plotting.backend = "plotly"

        import plotly.io as pio
        
        pio.renderers.default='browser'
        import plotly.express as px
        
       
        fig = px.line(self.mega_cdf, x=self.mega_cdf.index, y=self.mega_cdf.columns, title="mean of {}".format(self.experiment))

        fig.show()
        
        
        import plotly.io as pio
        
        pio.renderers.default='browser'
        pd.options.plotting.backend = "matplotlib"
        
        return self.mega_cdf
    
        
        
        
        
#%%
""" Inputs needed: 
    1) experiment name (look master_time_sheet.xlsx) 
    2) The name of the sensor (1a_testo, 2a_testo, 3a_testo) 
    
    If data is missing probably it is not available, 
    eitherways if you miss something please notify me
    Cheers
"""

a = DataCleaner("W_H_e0_ESHL" , "2a_testo")
print(a.wind_velocity_indoor())
print(a.wind_velocity_outdoor())
print(a.mean_curve())
b = a.mean_curve()
#%%

# a = ["apple", "carrot", "lemon"]
# b = ["pineapple", "apple", "tomato"]

# new_list = [x for x in a if (x not in b)]

# print(new_list)
#%%









