# -*- coding: utf-8 -*-
"""
Created on Thu Jun 24 13:28:14 2021

@author: Raghavakrishna
"""


import pandas as pd
from datetime import timedelta
import datetime as dt

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
        self.input = pd.read_excel("master_time_sheet.xlsx", sheet_name = "inputs")
        self.experiment = experiment
        self.sensor_name = sensor_name
        self.column_name = column_name
        # self.engine = create_engine("mysql+pymysql://wojtek:Password#102@wojtek.mysql.database.azure.com/",pool_pre_ping=True)
        self.engine = create_engine("mysql+pymysql://root:Password123@localhost/",pool_pre_ping=True)

        self.database = self.times[self.times["experiment"] == self.experiment].iloc[0,3]
        self.t0 = self.times[self.times["experiment"] == experiment].iloc[0,1]
        self.tn = self.times[self.times["experiment"] == experiment].iloc[0,2]
        self.exclude = self.times[self.times["experiment"] == experiment].iloc[0,4].split(",")
        self.calibration = self.times[self.times["experiment"] == experiment].iloc[0,5]
        
        self.t0_20 = self.t0 - timedelta(minutes = 20)
        self.tn_20 = self.tn + timedelta(minutes = 20)   

        self.tau_nom = self.input.loc[self.input["experiment"] == self.experiment]["tau_nom"].iat[0]                             
        
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
    

    def aussen(self):
        if self.experiment == "S_I_e0_Herdern" or self.experiment == "S_I_e1_Herdern":
            self.Cout = {'meanCO2': 445.1524174626867,
                    'sgm_CO2': 113.06109664245112,
                    'maxCO2': 514.3716999999999,
                    'minCO2': 373.21639999999996}
            self.cout_mean, self.cout_max, self.cout_min = 445.1524174626867, 514.3716999999999, 373.21639999999996
            return self.Cout
        else:
            
            accuracy1 = 50 # it comes from the equation of uncertainity for testo 450 XL
            accuracy2 = 0.02 # ±(50 ppm CO2 ±2% of mv)(0 to 5000 ppm CO2 )
            
            accuracy3 = 50 # the same equation for second testo 450 XL
            accuracy4 = 0.02
            
            accuracy5 = 75 # # the same equation for second testo 480
            accuracy6 = 0.03 # Citavi Title: Testo AG
            
            '''
            The following if esle statement is writtten to import the right data 
            for calibration offset equation
            There are two time periods where calibration was done and this
            '''

            engine1 = create_engine("mysql+pymysql://root:Password123@localhost/{}".format(self.calibration),pool_pre_ping=True)

            
            '''standard syntax to import sql data as dataframe
            self.engine is measurement campagin experimentl data and engine1 is calibration data'''

            '''Calibration data is imported '''
            reg_result = pd.read_sql_table("reg_result", con = engine1).drop("index", axis = 1)
            '''Calibration data for the particular sensor alone is filtered '''
            res = reg_result[reg_result['sensor'].str.lower() == "außen"].reset_index(drop = True)
            
            '''This is to filter the HOBOs from testos, The hobos will have a res variable Testos will not have
            because they dont have experimantal calibration offset'''
            if res.shape[0] == 1:
                ''' The imported sql data is cleaned and columns are renamed to suit to out calculation'''
                self.sensor_df3 = pd.read_sql_query("SELECT * FROM {}.{} WHERE datetime BETWEEN '{}' AND '{}'".format(self.database, "außen", self.t0, self.tn) , self.engine).drop('index', axis =1)
                self.sensor_df3['CO2_ppm_reg'] = self.sensor_df3.eval(res.loc[0, "equation"])    
                self.sensor_df3 = self.sensor_df3.rename(columns = {'CO2_ppm':'CO2_ppm_original', 'CO2_ppm_reg': 'C_CO2 in ppm'})
                self.sensor_df3 = self.sensor_df3.drop_duplicates(subset=['datetime'])
                self.sensor_df3 = self.sensor_df3.loc[:, ["datetime", "C_CO2 in ppm", "CO2_ppm_original"]]
                self.sensor_df3 = self.sensor_df3.dropna()
                '''This is the absolute uncertainity at each point of measurement saved in the
                dataframe at each timestamp Ref: equation D2 in DIN ISO 16000-8:2008-12'''
                
                
                '''For ESHL summer ideally we take mean of all three sensors and also propogate 
                the uncertainities of al three testo sensors, This is not done here at the moment
                But to get the most uncertainity possible we peopogte the uncertainity first'''
                # Why RSE ? https://stats.stackexchange.com/questions/204238/why-divide-rss-by-n-2-to-get-rse
                self.sensor_df3["s_meas"] =  np.sqrt(np.square((self.sensor_df3["C_CO2 in ppm"] * accuracy2)) + np.square(accuracy1) + np.square((self.sensor_df3["C_CO2 in ppm"] * accuracy4)) + np.square(accuracy3) + np.square((self.sensor_df3["C_CO2 in ppm"] * accuracy6)) + np.square(accuracy5)+ np.square(res.loc[0, "rse"])) 
                # Die Messunsicherheit hängt sicher in einem bestimmten Umfang vom Konzentrationsbereich ab.DIN ISO 16000-8:2008-12 (page 36)
        
                x = self.sensor_df3["datetime"][2] - self.sensor_df3["datetime"][1]
                self.sec3 = int(x.total_seconds())
                
                """
                Creating a runtime column with t0 as 0 or centre of the time axes
                """
                t0_cd = self.sensor_df3['datetime'].loc[0]
                
                while not(self.t0 in self.sensor_df3["datetime"].to_list()):
                    self.t0 = self.t0 + dt.timedelta(seconds=1)
                    print(self.t0)
                    
                dtl_t0 = (self.t0 - t0_cd)//dt.timedelta(seconds=1)
                
                """
                Calucates the elapsed time stored in the array x as an interger of seconds
                """
                endpoint = len(self.sensor_df3) * self.sec3 - dtl_t0
                
                """
                Creates an array starting with 0 till endpoint with stepsize sec3.
                """
                x = np.arange(-dtl_t0,endpoint,self.sec3)
                
                self.sensor_df3['runtime'] = x
                
                self.sensor_df2 = self.sensor_df3.set_index('datetime')
                self.rhg = pd.date_range(self.sensor_df2.index[0], self.sensor_df2.index[-1], freq=str(self.sec3)+'S')   
                self.au_mean = self.sensor_df2.reindex(self.rhg).interpolate()
                
                self.au_mean['C_CO2 in ppm_out'] = self.au_mean['C_CO2 in ppm']
                self.cout_max = self.au_mean['C_CO2 in ppm_out'].max()
                self.cout_min = self.au_mean['C_CO2 in ppm_out'].min()
                self.cout_mean = self.au_mean['C_CO2 in ppm_out'].mean()
                
                """
                The default value (499±97)ppm (kp=2) has been calculated as the average CO2-
                concentration of the available outdoor measurement data in 
                ...\CO2-concentration_outdoor\.
                However the value should be setted as a list of datapoints for the natural
                outdoor concentration for a time inverval covering the measurement interval.
                
                In future it would be great to have a dataframe with CO2-concentrations for 
                coresponding time stamps.
                """
                self.Cout = {'meanCO2':self.cout_mean, 
                        'sgm_CO2':self.au_mean["s_meas"].mean(), # More clarification needed on uncertainity
                        'maxCO2':self.cout_max,
                        'minCO2':self.cout_min}
                return self.Cout
        
    
    
    def mean_curve(self, plot = False):
        self.names = pd.read_sql_query('SHOW TABLES FROM {}'.format(self.database), con = self.engine)
        self.names = self.names.iloc[:,0].to_list()
        self.new_names = [x for x in self.names if (x not in self.exclude)]
        
        accuracy1 = 50 # it comes from the equation of uncertainity for testo 450 XL
        accuracy2 = 0.02 # ±(50 ppm CO2 ±2% of mv)(0 to 5000 ppm CO2 )
        
        accuracy3 = 50 # the same equation for second testo 450 XL
        accuracy4 = 0.02
        
        accuracy5 = 75 # # the same equation for second testo 480
        accuracy6 = 0.03 # Citavi Title: Testo AG
        
        
        self.cdf_list, self.df_tau, self.tau_hr  = [], [], []
        for self.table in self.new_names:
            self.cdf1 = pd.read_sql_query("SELECT * FROM {}.{} WHERE datetime BETWEEN '{}' AND '{}'".format(self.database, self.table, self.t0, self.tn), con = self.engine) 
            self.cdf2 = self.cdf1.loc[:,["datetime", "CO2_ppm"]]
            engine1 = create_engine("mysql+pymysql://root:Password123@localhost/{}".format(self.calibration),pool_pre_ping=True)
            self.reg_result = pd.read_sql_table("reg_result", con = engine1).drop("index", axis = 1)
            '''Calibration data for the particular sensor alone is filtered '''
            self.res = self.reg_result[self.reg_result['sensor'].str.lower() == self.table].reset_index(drop = True)
            
            if "testo" not in self.table:
                self.cdf2['CO2_ppm_reg'] = self.cdf2.eval(self.res.loc[0, "equation"]) 
                self.cdf2 = self.cdf2.rename(columns = {'CO2_ppm':'CO2_ppm_original', 'CO2_ppm_reg': 'CO2_ppm'})
                self.cdf2 = self.cdf2.drop_duplicates(subset=['datetime'])
                self.cdf2 = self.cdf2.loc[:, ["datetime", "CO2_ppm"]]
                self.cdf2 = self.cdf2.dropna()
            
            self.cdf2.loc[:,"CO2_ppm"] = self.cdf2.loc[:,"CO2_ppm"] - a.aussen()["meanCO2"]
            
            self.cdf2.columns = ["datetime", str(self.table)]
            self.cdf2["log"] = np.log(self.cdf2[str(self.table)])
            self.diff_sec = (self.cdf2["datetime"][1] - self.cdf2["datetime"][0]).seconds
            self.cdf2["s_meas"] =  np.sqrt(np.square((self.cdf2[str(self.table)] * accuracy2)) 
                                   + np.square(accuracy1) + np.square((self.cdf2[str(self.table)] * accuracy4)) 
                                   + np.square(accuracy3) + np.square((self.cdf2[str(self.table)] * accuracy6)) 
                                   + np.square(accuracy5))
            self.ns_meas = self.cdf2['s_meas'].mean()
            self.n = len(self.cdf2['s_meas'])
            
            ### ISO 16000-8 option to calculate slope (defined to be calculated by Spread-Sheat/Excel)
            self.cdf2["runtime"] = np.arange(0,len(self.cdf2) * self.diff_sec, self.diff_sec)
            
            self.cdf2["t-te"] = self.cdf2["runtime"] - self.cdf2["runtime"][len(self.cdf2)-1]
            
            self.cdf2["lnte/t"] = self.cdf2["log"] - self.cdf2["log"][len(self.cdf2)-1]
            
            self.cdf2["slope"] = self.cdf2["lnte/t"] / self.cdf2["t-te"]
            self.slope_iso = self.cdf2["slope"].mean()
            
            ### More acurate option to calculate the solpe of each (sub-)curve
            self.x1 = self.cdf2["runtime"].values
            self.y1 = self.cdf2["log"].values
            from scipy.stats import linregress
            self.slope = linregress(self.x1,self.y1)[0]
            ###
            self.cdf2.loc[[len(self.cdf2)-1], "slope"] = abs(self.slope)
            
            self.sumconz = self.cdf2[str(self.table)].iloc[1:-1].sum()
            
            self.tail = self.cdf2[str(self.table)][len(self.cdf2)-1]/abs(self.slope)
            
            self.area_sup_1= (self.diff_sec * (self.cdf2[str(self.table)][0]/2 + self.sumconz +self.cdf2[str(self.table)][len(self.cdf2)-1]/2))
            from numpy import trapz
            self.area_sup_2 = trapz(self.cdf2[str(self.table)].values, dx=self.diff_sec) # proof that both methods have same answer
            
            self.a_rest = self.cdf2[str(self.table)].iloc[-1]/abs(self.slope)
            self.a_tot = self.area_sup_2 + self.a_rest
            
            self.sa_num = self.ns_meas * (self.diff_sec) * ((self.n - 1)/np.sqrt(self.n)) # Taken from DIN ISO 16000-8:2008-12, Equation D2 units are cm3.m-3.sec
            self.s_lambda = self.cdf2["slope"][:-1].std()/abs(self.cdf2["slope"][:-1].mean())
            self.s_phi_e = self.cdf2["slope"][:-1].std()/abs(self.cdf2["slope"].iloc[-1])
    
            self.s_rest = np.sqrt(pow(self.s_lambda,2) + pow(self.s_phi_e,2))
            self.sa_rest = self.s_rest * self.a_rest
            self.s_area = np.sqrt(pow(self.sa_num,2) + pow(self.sa_rest,2))/self.a_tot
            self.s_total = np.sqrt(pow(self.s_area,2) + pow(0.05,2))
    
    
            self.tau = (self.area_sup_2 + self.tail)/self.cdf2[str(self.table)][0]
            
            self.tau_hr.append(self.tau/3600)
            self.cdf2["tau_hr"] = self.tau/3600
            self.cdf2.loc[:, "s_total"] = self.s_total

            self.df_tau.append(self.cdf2)
            
            self.cdf3 = self.cdf2.loc[:, ["datetime", str(self.table)]]
            self.cdf3 = self.cdf3.set_index("datetime")
            self.cdf_list.append(self.cdf3)
        self.mega_cdf = pd.concat(self.cdf_list,axis = 1).interpolate(method = "linear")
        # self.mega_cdf.columns = self.new_names
        self.mega_cdf["mean_delta"] = self.mega_cdf.mean(axis = 1)
        # self.mega_cdf = self.mega_cdf.set_index("datetime")
    
        if plot:
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
            #self.df_tau, self.mega_cdfv
        return  np.mean(self.tau_hr) , self.df_tau, self.mega_cdf
    
 
        
        
#%%
""" Inputs needed: 
    1) experiment name (look master_time_sheet.xlsx) 
    2) The name of the sensor (1a_testo, 2a_testo, 3a_testo) 
    
    If data is missing probably it is not available, 
    eitherways if you miss something please notify me
    Cheers
"""

a = DataCleaner("W_H_e0_ESHL" , "2a_testo")
# print(a.wind_velocity_indoor())
# print(a.wind_velocity_outdoor())
b = a.mean_curve(True)



#%%
d = a.mega_cdf.loc[:,["mean_delta"]]
d['mean_delta_norm'] = d["mean_delta"]/d["mean_delta"].iat[0]


d["runtime"] = np.arange(0,len(d) * a.diff_sec, a.diff_sec)

d["min"] = d["runtime"]/(np.mean(a.tau_nom) * 3600)
d["min"] = 1 - d["min"]

slope = 1/(np.mean(a.tau_hr) * 3600)




#%%%

fig, ax = plt.subplots()


def func(x, a, b):
    return a * np.exp(-b * x)


y = func(d["runtime"].values, 1, slope)


slope_50 = 1/(a.tau_nom *3600)
y_50 = func(d["runtime"].values, 1, slope_50)
d["ea_50"] = y_50
d["ea_50_max"] = d[["min", "ea_50"]].max(axis = 1)

d["mean_delta_norm_max"] = d[["min", "mean_delta_norm"]].max(axis = 1)



ax.plot(d["runtime"], d["ea_50_max"].values, label = "50 % efficiency (estimated)")
ax.plot(d["runtime"], d["mean_delta_norm_max"].values, label = "{} % efficiency (measured)".format(round(a.tau_nom/(np.mean(a.tau_hr)*2) * 100) ))
ax.plot(d["runtime"], d["min"].values, label = "maximum effieiency (estimated)")



ax.set_xlabel("time (sec)")
ax.set_ylabel("CO2 (normalized)")

ax.set_title("Decay curves for {}".format(a.experiment))
ax.legend()


#%%






#%%






#%%


