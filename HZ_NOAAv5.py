##############################################
## NOAA API
## Wei Zhang 
## Oct 18 2020 Update for solar radiation 
############################################

import requests
import json
import threading
import selectors
from datetime import datetime, timedelta
from datetime import time as tm 
#from pysolar.solar import *
import time
import pytz
import numpy as np
import pandas as pd
import copy
from math import pi, cos, acos,sin, degrees, radians, exp, pow
from json.decoder import JSONDecodeError

import socket
from collections import deque
from pandas.io.json import json_normalize

###Client thread    
class Connection(object):
    def __init__(self):
        self.host = '127.0.0.1'
        self.port = 65432
        
        self.Latitude = 42.376649
        self.Longitude = -71.112002
        
        # Cambridge elevation + House
        # for solar radiation 
        self.Altitude  = 15
        
        # summer boston time offset to UTC is -4
        # winter boston time offset to UTC is -5
        # for solar position 
        self.timezone_offset=-4
        
###############################################
########### ClientNOAA().weather_DF()##############
############################################    
##from HZ_NOAAv4 import ClientNOAA    
#ClientNOAA().weather_DF()
#ClientNOAA.save_csv()
############################


class ClientNOAA(Connection):
    def __init__(self):
        Connection.__init__(self)
        # message size: configuration of communication
        self.size = 8192
        self.data_storage = None 
        
        columns_name=['date_time', 'temperature', 'windspeed', 'direction','sky_condition','daytime']
        self.DF_weather = pd.DataFrame(columns=columns_name)
        
        
    
    def connectingNOAAserver(self):
        # connect the local NOAA server
        #HOST = '127.0.0.1'  # The server's hostname or IP address
        #PORT = 65432        # The port used by the server
        HOST = self.host  # The server's hostname or IP address
        PORT = self.port        # The port used by the server
    
        Ask = "Hello local NOAA Server"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            s.sendall(json.dumps(Ask).encode())
            data = s.recv(self.size)
        
        return data
    
    def solarI_nowII(self):
    # calculate solar radiation in 15 min series
        cover_DF = self.DF_weather.loc[:,['date_time','sky_condition']]
        cover_DF.set_index('date_time',inplace=True)
        # DF is long enough 
        ActM = datetime.now()
        list1, list2 = list(), list()
        
        # series element number
        H=10
        for k in range(H):
          
          interval = 15*k
          next_moment = ActM.replace(microsecond=0, second=0) + timedelta(minutes=interval)
          list1.append(next_moment)
          # retrieve cloud cover info
          index_location_next = cover_DF.index.get_loc(next_moment, method='nearest')
          nearest_data_next = cover_DF.iloc[index_location_next]
          cover = self.__class__.cloud_coverage(nearest_data_next['sky_condition'])
          # solar 
          solarT, zenith, azimuth = self.solar_position(next_moment)
          dt_month = next_moment.month
          solar = self.solar_radiation(zenith, azimuth, dt_month)
          cloudy_solar_next = solar if cover==0 else self.__class__.cloudy_radiation(solar, cover)
          list2.append(cloudy_solar_next)
          
        DF_s = pd.DataFrame(np.column_stack([list1, list2]), \
                columns=['date_time', 'solar_radiation'])       
        DF_s.set_index('date_time',inplace=True,verify_integrity=True)
        
        return DF_s
   
    
    
            
    def weather_DF(self):
        #Now = datetime.now()         
        # Procedure the infomation from local server thread
        # think about keep the old data
        data = self.connectingNOAAserver()
        flag = False
        
        if not data:
            print("No data from local weather server")
        else:
            try:
                seq1, seq2, seq3, seq4, seq5, seq6, mark = self.__class__.readingdata(data)
            except TypeError as e:
                print("Something wrong with reading data from Json")
                flag = True
                seq1, seq2, seq3, seq4, seq5, seq6, mark = self.__class__.readingdata(self.data_storage)
            
            # if new data can be read, replace old one
            if flag==False: self.data_storage = copy.deepcopy(data) 
            
            lis1, lis22, lis3, lis44, lis5, lis6 = self.__class__.cleanTime(seq1, seq2, seq3, seq4, seq5, seq6, -1)
            
            lis2 = [round(i,2) for i in lis22]
            lis4 = [round(i,2) for i in lis44]
    
            DF = pd.DataFrame(np.column_stack([lis1, lis2, lis4, lis3, lis5, lis6]), \
                columns=['date_time', 'temperature', 'windspeed', 'direction','sky_condition','daytime'])
                        
            DF['zenith'] = 0
            DF['azimuth'] = 0
            DF['ClearSkySolar'] = 0
            DF['Cloud'] = 0
            DF['CloudSolar'] = 0
            DF['SolarRef'] = 0
            print("Weather Forcast is updated by NOAA at:",mark)
            print("--------------------------------------------")
            
            #import pysolar.solar
            for index, row in DF.iterrows():
                #
                #input of solar_position requires naive datetime obj
                solarT, zenith, azimuth = self.solar_position(row['date_time'])
                
                DF.loc[index, 'zenith'] = round(zenith, 2)
                DF.loc[index, 'azimuth'] = round(azimuth, 2)
                #DF.loc[index, 'sky_condition']
                cover = self.__class__.cloud_coverage(DF.loc[index, 'sky_condition'])
                DF.loc[index, 'Cloud'] = round(cover, 3)
                
                dt_month = row['date_time'].month
                solar = self.solar_radiation(zenith, azimuth, dt_month)
                DF.loc[index, 'ClearSkySolar'] = round(solar,1)
                
                cloudy_solar = solar if cover==0 else self.__class__.cloudy_radiation(solar, cover)
                DF.loc[index, 'CloudSolar'] = round(cloudy_solar,1)
                DF.loc[index, 'SolarRef'] = self.pysolar(row['date_time'])
                
            
            if self.DF_weather.empty: self.DF_weather = DF.copy(deep=True)
            else: 
                DF1 = DF.copy(deep=True)
                DF_weather_new = pd.concat([self.DF_weather,DF1])
                DF_weather_new.drop_duplicates(subset=['date_time'], keep='last', inplace = True)
                self.DF_weather = DF_weather_new.copy(deep=True)
    
            return DF
    
    @staticmethod
    def readingdata(data):
        # data is received from server thread
        
        try:
            Data_json = json.loads(data)
        except JSONDecodeError as e:
            print('JSON decoding is failed.')
            # send one TypeError to outside 
            return None
        
    
        Time1 = []
       
        Time_json = Data_json['time']
        Temps_json = Data_json['temps']
        Ws_json = Data_json['windspeed']
        Wd_json = Data_json['winddirection']
        Sky_json = Data_json['sky']
        Dt_json = Data_json['daytime']
        mark = Data_json['mark']
        #check if message is received by client
        #print(Data_json)
        
        for i in Time_json:
        # navie or aware 
            #T1 = time.strptime(i, '%Y-%m-%d %H:%M:%S')
            T1 = datetime.fromisoformat(i)
            #Time1.append(datetime.fromtimestamp(time.mktime(T1))) 
            Time1.append(T1)
        
        return Time1, Temps_json, Wd_json, Ws_json, Sky_json, Dt_json, mark
    
    @staticmethod
    def cleanTime(Time, Temps, Wd, Ws, Sky, Dt, k):
        # clean the previous moment for now
        # output as deque 
        #Now = datetime.now()
        UTC = pytz.timezone('Zulu')
        ET = pytz.timezone('US/Eastern')
        # utcnow to consider daylighting saving time
        now_utc = UTC.localize(datetime.utcnow()+timedelta(hours = k))
        Now = now_utc.astimezone(ET)
        print("Now is one hour ago",Now)
        
        Temps_de = deque(Temps)
        Time_de = deque(Time)
        Wd_de = deque(Wd)
        Ws_de = deque(Ws)
        Sky_de = deque(Sky)
        Dt_de = deque(Dt)
    
        while Time_de[0] < Now:
            Time_de.popleft()
            Temps_de.popleft()
            Wd_de.popleft()
            Ws_de.popleft()
            Sky_de.popleft()
            Dt_de.popleft()
    
        return list(Time_de), list(Temps_de), list(Wd_de), list(Ws_de), list(Sky_de), list(Dt_de)
            
    @staticmethod
    def leap_year(year):
        return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
        
    def solar_position(self, dt):
    # dt must be datetime type
        days = 366 if self.__class__.leap_year(dt.year) else 365
        
        latit = self.Latitude
        longit = self.Longitude
        
        gamma = 2 * pi / days * (dt.timetuple().tm_yday - 1 + float(dt.hour - 12) / 24)
        eqtime = 229.18 * (0.000075 + 0.001868 * cos(gamma) - 0.032077 * sin(gamma) \
             - 0.014615 * cos(2 * gamma) - 0.040849 * sin(2 * gamma))
        decl = 0.006918 - 0.399912 * cos(gamma) + 0.070257 * sin(gamma) \
           - 0.006758 * cos(2 * gamma) + 0.000907 * sin(2 * gamma) \
           - 0.002697 * cos(3 * gamma) + 0.00148 * sin(3 * gamma)
    
        
        time_offset = eqtime + 4 * longit - 60*self.timezone_offset
        #print(time_offset)
        tst = dt.hour * 60 + dt.minute + dt.second / 60 + time_offset
        solar_t = datetime.combine(dt.date(), tm(0)) + timedelta(minutes=tst)
        ha = (tst/4) -180
        #print(ha)
        lat = radians(latit)
        ha_r = radians(ha)
        zenith_cos = sin(lat)*sin(decl) + cos(lat)*cos(decl)*cos(ha_r)
        zenith_r = acos(zenith_cos)
        zenith_dg = degrees(zenith_r)
    
        azimuth_value = degrees(acos((sin(decl)*cos(lat)-cos(decl)*sin(lat)*cos(ha_r))/sin(zenith_r)))
   
    
        azimuth_dg = 360 - azimuth_value if ha_r >0 else azimuth_value

        return solar_t, zenith_dg, azimuth_dg 
    
    def solar_radiation(self, zenith, azimuth, dt_month):
    # Ineichen
        if zenith >90:
            return 0
        else:
            solar_altitude = 90 - zenith
            # cambridge altitude 12 m
            station_altitude = self.Altitude 
    
            a1 = 0.0000509*station_altitude + 0.868
            a2 = 0.0000392*station_altitude + 0.0387
    
            fh1 = exp(-station_altitude/8000)
            fh2 = exp(-station_altitude/1250)
    
            # Gueymard 1993
            #print("cos:",cos(radians(zenith)))
            #print("zenith:", zenith)
            #print("second item:",zenith*pow(94.37515-zenith,-1.21563))
    
    
            AM = 1/(cos(radians(zenith))+0.00176759*zenith*pow(94.37515-zenith,-1.21563))
            I0 = 1.362
    
            month = dt_month
            TL_boston_dic = {'1':2.5, '2':2.7, '3':2.6, '4':3.1, '5':3.4, '6': 3.8, '7': 3.9, '8':4.0,\
                    '9':3.4, '10':3.1, '11':3.1, '12':2.5}
            TL = TL_boston_dic[str(month)]
            #print(TL)
    
            G = a1*I0*sin(radians(solar_altitude))*exp(-a2*AM*(fh1+fh2*(TL-1)))
            # output: w/m2
    
            return G*1000
    
    @staticmethod
    def cloudy_radiation(clear_sky, cover):
        return clear_sky*(1.01-0.734*cover+1.26*pow(cover,2)-1.23*pow(cover,3))
    
    @staticmethod
    def cloud_coverage(sky_string):
    #https://www.weather.gov/bgm/forecast_terms
        sky = str(sky_string).lower()
        if 'mostly clear' in sky:
            return 1/8
        elif 'mostly sunny' in sky:
            return 2/8
        elif 'partly cloudy' in sky:
            return 3/8
        elif 'partly sunny' in sky:
            return 4/8
        elif 'mostly cloudy' in sky or 'patchy fog' in sky or 'rain' in sky:
            return 5/8
        elif 'considerable cloudiness' in sky or 'snow' in sky:
        # update the snow Feb 9, by observation
            return 7/8
        elif 'clear' in sky or 'sunny' in sky:
            return 0
        elif 'cloudy' in sky:
            return 1
        elif 'fair' in sky or 'fog' in sky:
            return 0.3
        else:
            return 0
    
    @classmethod
    def save_csv(cls):
        df = cls().weather_DF()
        now = datetime.now()
        moment = str(now.month) +'_'+ str(now.day) + '_'+str(now.hour) +'h' + '_'+str(now.minute)
        filenom = 'WF'+moment+'.csv'
        df.to_csv(filenom)
        print("Weather forcast has been downloaded.")
    
    # use pysolar to calculate the solar radiation
    def pysolar(self, dt):
        from pysolar.solar import get_altitude, get_azimuth
        from pysolar.radiation import get_radiation_direct 
        latitude_deg = self.Latitude
        longitude_deg = self.Longitude
        alt = get_altitude(latitude_deg, longitude_deg, dt)
        azi = get_azimuth(latitude_deg, longitude_deg, dt)
        return round(get_radiation_direct(dt,alt),1)
    
    
######################################################
# class as server thread###
# from HZ_NOAAv4 import LocalWeatherNOAA
######################################
##############################
#if __name__ == '__main__':
#    LW = LocalWeatherNOAA()
#    LW.run()
##################################
#
class LocalWeatherNOAA(Connection):
    
    def __init__(self):
        
        Connection.__init__(self)
        self.T_list = []
        self.Ti_list = []
        self.Wd_list = []
        self.Ws_list = []
        self.Sky_list = []
        self.DT_list = []
        
        self.mark_updated = None
        
        # Location of local place
        #self.Latitude = 42.3764
        #self.Longitude = -71.1095
        
        # web address for local server
        #self.host = '127.0.0.1'
        #self.port = 65432
    
    #return True if everything is OK
    def check_con_status(self, res):
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError as error1:
            print("Http Error:", error1)
        except requests.exceptions.ConnectionError as error2:
            print("Connection Error:", error2)
        except requests.exceptions.Timeout as error3:
            print("Timout Error:", error3)
        
        return res.status_code == 200            
            
        
    def get_NOAAdata_hourly(self):
        """
        url = 'https://api.weather.gov/points/' + str(self.Latitude) +','+ str(self.Longitude)
        res = requests.get(url)
   
            
        if (self.check_con_status(res)):    
            r = res.json()
            url2 = r['properties']['forecastHourly']
        else:
            # end the process
            print('Connection failed')
            return None
        """
     
        # second url for data retrieve 
        url2 = 'https://api.weather.gov/gridpoints/BOX/69,76/forecast/hourly'
        res2 = requests.get(url2)
       
        if (self.check_con_status(res2)): 
            data = res2.json()
            # retrieve next 49 hours, changable 
            List = data['properties']['periods'][1:49]
            mark_updated = data['properties']['updated']
        else:
            # end the process
            print('Connection failed')
            return None
    
        # get data items: temperature, timestamps ect
        HourlyTemps = []
        HourlyWindSpeed = []
        HourlyWindDir = []
        Timestamps = []
        HourlySky = []
        HourlyDaytime = []
        
        wind_direction_dic = {'N':0, 'E':90, 'S':180, 'W':270, 'NE':45, 'SE': 135, 'NW': 315, 'SW': 225}
        daytime_dic = {True:1, False:0}
        
        for i in range(len(List)):
            T = (float(List[i]['temperature'])-32)/1.8
            Ws = float(List[i]['windSpeed'][0])*0.44704
            Wd = wind_direction_dic.get(List[i]['windDirection'])
            Sky = List[i]['shortForecast']
            Daytime = daytime_dic.get(List[i]['isDaytime'])

            #Time = datetime.strptime(List[i]['startTime'][0:19],"%Y-%m-%dT%H:%M:%S")
            Time_ISO = datetime.fromisoformat(List[i]['startTime'])
            #print("Time:",Time_ISO)
            #Time = float(List[i]['endTime'])
            #Timestamps.append(Time)
            Timestamps.append(Time_ISO)
            HourlyTemps.append(T)
            HourlyWindDir.append(Wd)
            HourlyWindSpeed.append(Ws)
            HourlySky.append(Sky)
            HourlyDaytime.append(Daytime)
        
        return Timestamps, HourlyTemps, HourlyWindSpeed, HourlyWindDir, HourlySky, HourlyDaytime, mark_updated
    
    def process_api(self):
        lock = threading.Lock()
        
        while True:
            lock.acquire()
    
            try:
                Ti_list_a, T_list_a, Ws_list_a, Wd_list_a, Sky_list_a, DT_list_a, mk = self.get_NOAAdata_hourly()
            except (KeyError, TypeError) as e:
                #TypeError: cannot unpack non-iterable NoneType object (return None)
                #KeyError: no item in the list (old issue), 
                #it should be resolved by check_con_status function
                print("Something wrong with NOAA website, try 10 mins later")
                time.sleep(600)
        
                # second try 
                try:
                    Ti_list_a, T_list_a, Ws_list_a, Wd_list_a, Sky_list_a, DT_list_a, mk = self.get_NOAAdata_hourly()
                except (KeyError, TypeError) as e:
                    print("Something wrong with NOAA website again, wait for next updatding")
              
            #print("isDaytime:",DT_list_a)
            if not Ti_list_a:
                print("No info received from NOAA API")
            # do not update in this case
            else:
                # update info
                self.T_list.clear()
                self.Ti_list.clear()
                self.Ws_list.clear()
                self.Wd_list.clear()
                self.Sky_list.clear()
                self.DT_list.clear()
               
            
                self.T_list = T_list_a.copy()
                self.Ti_list = Ti_list_a.copy()
                self.Ws_list = Ws_list_a.copy()
                self.Wd_list = Wd_list_a.copy()
                self.Sky_list = Sky_list_a.copy()
                self.DT_list = DT_list_a.copy()
                self.mark_updated = copy.copy(mk) 
            
            
            lock.release()
            print(self.Ti_list[0:2])

            print("NOAA info received, update again in 2 hours")
            time.sleep(7200)
            
            
    def process_server(self):
        sel = selectors.DefaultSelector()
        #host, port = '127.0.0.1', 65432
    
        def datetimobj(o):
            if isinstance(o, datetime):
                return o.__str__()

        def accept(sock, mask):
            try:
                conn, addr = sock.accept()  # Should be ready
            except ConnectionResetError as e:
                print("connection error, try one more time soon.")
                time.sleep(10)
                try: 
                    conn, addr = sock.accept()
                except ConnectionResetError as e:
                    print("connection error agian")

                
            #print('accepted', conn, 'from', addr)
            conn.setblocking(False)
            sel.register(conn, selectors.EVENT_READ, read)
            # Oct 20: ConnectionResetError: [WinError 10054] 
            # An existing connection was forcibly closed by the remote host

        def read(conn, mask):
            data = conn.recv(1024)  # Should be ready
            if data:
                #print('echoing', repr(data), 'to', conn)
                ask = json.loads(data)
                #print('echoing data is', ask)
                if ask == 'Hello local NOAA Server':
                    #print('Yes, this is Server here')
                
                    Info = {}
                    Info['time'] = self.Ti_list
                    Info['windspeed'] = self.Ws_list
                    Info['winddirection'] = self.Wd_list
                    Info['temps'] = self.T_list
                    Info['sky'] = self.Sky_list
                    Info['daytime'] = self.DT_list
                    Info['mark'] = self.mark_updated
                    message = json.dumps(Info, default = datetimobj).encode()
                    #bt = conn.send(message)  # Hope it won't block
                    #print("The message sent out has:", bt)
                    try:
                        conn.sendall(message)  # Hope it won't block
                        print("The message is sent out.")
                    except socket.error as e:
                        print("No message is not sent out yet.")
                     
                    
                    # message sent to client thread 
            else:
                #print('closing', conn)
                sel.unregister(conn)
                conn.close()

        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
        sock.bind((self.host, self.port))
        sock.listen(10)
        # 10 is the backlog 
        sock.setblocking(False)
        sel.register(sock, selectors.EVENT_READ, accept)

        while True:
            events = sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
                
    def run(self):
        t1 = threading.Thread(target = self.process_api)
        t2 = threading.Thread(target = self.process_server)

        t1.daemon = True
        t2.daemon = True

        t1.start() 
        t2.start() 

        t1.join() 
        t2.join()
        

