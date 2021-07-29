# -*- coding: utf-8 -*-
"""
Created on Mon Jan 11 15:52:37 2021

@author: Devineni
"""


import pandas as pd
import numpy as np
from statistics import mean
import time
import datetime as dt
import matplotlib.pyplot as plt
import operator # for plotting

from openpyxl import load_workbook

# import mysql.connector
import os
import pymysql
from sqlalchemy import create_engine

from easygui import *
import sys

#from recalibration import clean_sql_reg   

def prRed(skk): print("\033[31;1;m {}\033[00m" .format(skk)) 
import warnings
warnings.filterwarnings('ignore')
engine = create_engine("mysql+pymysql://root:Password123@localhost/",pool_pre_ping=True)
#%%
'''
This section deals with taking input selection of the experiment
easygui module was used to create the dialogue boxes for easy input
this is just a more visual way for experiment selection
'''

msg ="Please select a Location/Season you like to analyze"
title = "Season selection"
choices = ["ESHL_summer", "ESHL_winter", "CBo_summer", "CBo_winter"]
database = choicebox(msg, title, choices)


times = pd.read_excel('C:/Users/Devineni/OneDrive - bwedu/MA_Raghavakrishna/0_Evaluation/excel_files/Times_thesis.xlsx', sheet_name= database)

msg ="Please select an experiment you would like to analyse in {database}".format(database = str(database))
title = "Experiment selection"
choices = list(times['short name'])
experiment = choicebox(msg, title, choices)


z = int(times[times['short name'] == experiment].index.values)
Vdot_sheets = {"ESHL_summer":"ESHL_Vdot", "ESHL_winter":"ESHL_Vdot", "CBo_summer":"CBo_Vdot", "CBo_winter":"CBo_Vdot"}


t0 = times.loc[z,"Start"]
tn = times.loc[z,"End"]
folder_name = times.loc[z,"short name"]


#%% Upload Anemometere data


# =============================================================================
# df = pd.read_excel("C:/Users/Devineni/OneDrive - bwedu/0_ESHL/0_ESHL_SUMMER/Kü_Testo/06.07.20 10_00.xlsx")
# df1 = pd.read_excel("C:/Users/Devineni/OneDrive - bwedu/0_ESHL/0_ESHL_SUMMER/Kü_Testo/07.07.20 09_12.xlsx")
# df2 = pd.read_excel("C:/Users/Devineni/OneDrive - bwedu/0_ESHL/0_ESHL_SUMMER/Kü_Testo/08.07.20 09_33.xlsx")
# 
# df3 = pd.read_excel("C:/Users/Devineni/OneDrive - bwedu/0_ESHL/0_ESHL_SUMMER/Kü_Testo/09.07.20 11_12.xlsx")
# df4 = pd.read_excel("C:/Users/Devineni/OneDrive - bwedu/0_ESHL/0_ESHL_SUMMER/Kü_Testo/10.07.20 10_19.xlsx")
# df5 = pd.read_excel("C:/Users/Devineni/OneDrive - bwedu/0_ESHL/0_ESHL_SUMMER/Kü_Testo/14.07.20 08_27.xlsx")
# 
# df6 = pd.read_excel("C:/Users/Devineni/OneDrive - bwedu/0_ESHL/0_ESHL_SUMMER/Kü_Testo/16.07.20 01_23.xlsx")
# 
# 
# dfs1 = [df,df1,df2,df3,df4,df5,df6]
# dfs = []
# for df in dfs1:
#     df = df.iloc[:,[0,2,4]]
#     df.columns = ["datetime", "hb_m/sec", "hb_°C"]
#     df = df.set_index("datetime")
#     dfs.append(df)
# 
# kt_df = pd.concat(dfs)
# kt_df = kt_df.reset_index()
# kt_df["datetime"] = pd.to_datetime(kt_df["datetime"],format="%d-%m-%Y %H:%M:%S")
# 
# kt_df.to_sql("eshl_summer_kt", con =create_engine("mysql+pymysql://root:Password123@localhost/anemometere",pool_pre_ping=True), if_exists="replace" )
# 
# 
# #%%
# 
# df = pd.read_excel("C:/Users/Devineni/OneDrive - bwedu/0_ESHL/0_ESHL_SUMMER/WZ_Testo/06.07.20 09_59.xlsx")
# df1 = pd.read_excel("C:/Users/Devineni/OneDrive - bwedu/0_ESHL/0_ESHL_SUMMER/WZ_Testo/07.07.20 09_12.xlsx")
# df2 = pd.read_excel("C:/Users/Devineni/OneDrive - bwedu/0_ESHL/0_ESHL_SUMMER/WZ_Testo/08.07.20 09_33.xlsx")
# 
# df3 = pd.read_excel("C:/Users/Devineni/OneDrive - bwedu/0_ESHL/0_ESHL_SUMMER/WZ_Testo/09.07.20 11_12.xlsx")
# df4 = pd.read_excel("C:/Users/Devineni/OneDrive - bwedu/0_ESHL/0_ESHL_SUMMER/WZ_Testo/10.07.20 10_19.xlsx")
# df5 = pd.read_excel("C:/Users/Devineni/OneDrive - bwedu/0_ESHL/0_ESHL_SUMMER/WZ_Testo/14.07.20 08_27.xlsx")
# 
# df6 = pd.read_excel("C:/Users/Devineni/OneDrive - bwedu/0_ESHL/0_ESHL_SUMMER/WZ_Testo/16.07.20 01_23.xlsx")
# 
# 
# dfs1 = [df,df1,df2,df3,df4,df5,df6]
# dfs = []
# 
# for df in dfs1:
#     df = df.iloc[:,[0,2,3]]
#     df.columns = ["datetime", "hb_m/sec", "hb_°C"]
#     df = df.set_index("datetime")
#     dfs.append(df)
# 
# wz_df = pd.concat(dfs)
# wz_df = wz_df.reset_index()
# wz_df["datetime"] = pd.to_datetime(wz_df["datetime"],format="%d-%m-%Y %H:%M:%S")
# 
# 
# wz_df.to_sql("eshl_summer_wz", con =create_engine("mysql+pymysql://root:Password123@localhost/anemometere",pool_pre_ping=True), if_exists="replace" )
# 
# =============================================================================
#%%
#%% 
df = pd.read_sql_query("SELECT * FROM anemometere.eshl_summer_kt WHERE datetime BETWEEN '{}' AND '{}'".format(t0,tn), con = engine).drop("index", axis = 1).set_index("datetime")
df1 = pd.read_sql_query("SELECT * FROM eshl_summer.1a_testo WHERE datetime BETWEEN '{}' AND '{}'".format(t0,tn), con = engine).drop("index", axis = 1).set_index("datetime")
df2 = pd.read_sql_query("SELECT * FROM eshl_summer.2a_testo WHERE datetime BETWEEN '{}' AND '{}'".format(t0,tn), con = engine).drop("index", axis = 1).set_index("datetime")
df3 = pd.read_sql_query("SELECT * FROM {}.{} WHERE datetime BETWEEN '{}' AND \
                        '{}'".format("weather", "weather_all", t0,\
                            tn), con = engine).set_index("datetime")
    
fig, (ax1,ax2,ax3) = plt.subplots(3,1, sharex=True,figsize=(19,15), sharey=True)
df.plot(y="hb_m/sec", ax = ax1, label = "LR")
df3.plot(y='Wind Speed, m/s', ax=ax1, color = 'silver', label = "windspeed")
ax4 = ax1.twinx()
df.plot(y="hb_°C", ax = ax4, label = "temp",color = "green" )

df1.plot(y="hb_m/sec", ax = ax2,  label = "BD 01")
df3.plot(y='Wind Speed, m/s', ax=ax2, color = 'silver', label = "windspeed")

ax5 = ax2.twinx()
df1.plot(y="hb_°C", ax = ax5, label = "temp",color = "green" )

df2.plot(y="hb_m/sec", ax = ax3, label = "BD 02")
df3.plot(y='Wind Speed, m/s', ax=ax3, color = 'silver', label = "windspeed")

ax6 = ax3.twinx()
df2.plot(y="hb_°C", ax = ax6, label = "temp",color = "green" )

ax1.set_title(folder_name + " Anemometere Data in m/sec")
plt.savefig(folder_name+".png", figsize=(19,15))

#%%
'''
This section deals with taking input selection of the experiment
easygui module was used to create the dialogue boxes for easy input
this is just a more visual way for experiment selection
'''

msg ="Please select a Location/Season you like to analyze"
title = "Season selection"
choices = ["ESHL_summer", "ESHL_winter", "CBo_summer", "CBo_winter"]
database = choicebox(msg, title, choices)


times = pd.read_excel('C:/Users/Devineni/OneDrive - bwedu/MA_Raghavakrishna/0_Evaluation/excel_files/Times_thesis.xlsx', sheet_name= database)

msg ="Please select an experiment you would like to analyse in {database}".format(database = str(database))
title = "Experiment selection"
choices = list(times['short name'])
experiment = choicebox(msg, title, choices)


z = int(times[times['short name'] == experiment].index.values)
Vdot_sheets = {"ESHL_summer":"ESHL_Vdot", "ESHL_winter":"ESHL_Vdot", "CBo_summer":"CBo_Vdot", "CBo_winter":"CBo_Vdot"}


t0 = times.loc[z,"Start"]
tn = times.loc[z,"End"]
folder_name = times.loc[z,"short name"]
#%%

wdf1 = pd.read_sql_query("SELECT * FROM eshl_winter.1a_testo WHERE datetime BETWEEN '{}' AND '{}'".format(t0,tn), con = engine).drop("index", axis = 1).set_index("datetime")
wdf2 = pd.read_sql_query("SELECT * FROM eshl_winter.2a_testo WHERE datetime BETWEEN '{}' AND '{}'".format(t0,tn), con = engine).drop("index", axis = 1).set_index("datetime")


fig, (ax1,ax2) = plt.subplots(2,1, sharex=True,figsize=(19,15))
wdf1.plot(y="hb_m/sec", ax = ax1, label = "BD 01")
ax3 = ax1.twinx()
wdf1.plot(y="hb_°C", ax = ax3, color = "green")


wdf2.plot(y="hb_m/sec", ax = ax2, label = "BD 01")
ax4 = ax2.twinx()
wdf2.plot(y="hb_°C", ax = ax4, color = "green")

ax1.set_title(folder_name + " Anemometere Data in m/sec")
plt.savefig(folder_name+".png", figsize=(19,15))
