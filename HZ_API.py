# HouseZero API
# Python 3.6
# version 1.2
# Sep 20, 2020, by wei zhang 
# wei_zhang@g.harvard.edu
# 
# updated Jan 8, 2021 for occupancy sensor reading and return bool value 
# updated Oct 13, 2024: cover the smart building server URL and authentication information for security
# wei.zhang@alumni.harvard.edu 

import requests
import json
import time
import threading
import pandas as pd
from pandas.io.json import json_normalize
import pytz
from datetime import datetime
from functools import reduce
zulu = pytz.timezone('Etc/Zulu')


import warnings
warnings.filterwarnings('ignore')

#Desigo server information
SERVER_URL = '**********************************' # info masked
requestHeaders={'Content-Type':  'application/json;charset=UTF-8'}
# Headers information, encoding style 
################################
##########Three API functions##
#################################

# Token API
def tokenAPI():
    """
    This function has no input parameter, and it will return a string from Desigo Server as token.
    """
    #authorization info for webservice
    autho = 'grant_type=password&username=*********************************' # info masked
    
    #Prepared Request
    url_t = '' + SERVER_URL + 'token'
    TokenRequest = requests.Request('POST', url_t, headers=requestHeaders, data=autho)
 
    with requests.Session() as session:
        TokenRequestPrepared = session.prepare_request(TokenRequest)
        TokenResponse = session.send(TokenRequestPrepared, verify = False)
        # no SSL certificate in server: no verify = False
        
    
    #more information about errors: requests.exceptions
    try:
        TokenResponse.raise_for_status()
    except requests.exceptions.HTTPError as error1:
        print("Http Error:", error1)
    except requests.exceptions.ConnectionError as error2:
        print("Connection Error:", error2)
    except requests.exceptions.Timeout as error3:
        print("Timout Error:", error3)
        
    #HTTP Status Codes
    if (TokenResponse.status_code == 200):
        Token = json.loads(TokenResponse.text)
        print("Token received.")
        return Token['access_token']
    else:
        print("No token received, connection failed")
        return None
		
# Sensor API
def sensorAPI(token, path, point):
    """
    This API has 3 input parameters: token from tokenAPI(),path as the address to gateway, 
    and point as name of device. This API return the measurement of sensor in float.
    """
    if path == None or point == None or path == "" or point == "":
        print("Path or point is empty")
        return False
    
    gatewayID = __gatewayIO(token, path)
    object = __getDevice(gatewayID, point)
    
    if object == None:
        print("Point: ", point, " does not exists on path: ", path)
        return False
    if object['ObjectId'] != None:
        value = __getValues(token, object['ObjectId'])[0]
        #print(value)
        dataType = value['DataType']
        dataValue = value['Value']
        #check the timestamp of sensor reading
        #print(dataValue)
        sensor_reading = dataValue['Value']
        if isinstance(sensor_reading, str):
            if sensor_reading == 'True' or sensor_reading == 'true':
                return 1
            elif sensor_reading == 'False' or sensor_reading == 'false':
                return 0
            else:
                return float(sensor_reading)
        else: 
            print("Please check the sensor reading!")
            return 0
        
        #return float(dataValue['Value'])
        
        #return {
        #    'dataType': dataType,
        #    'value': dataValue['Value'],
        #    'timeStamp': dataValue['Timestamp']
        #}

# Actuator API
def actuatorAPI(token, path, point, value):
    """
    This API has 4 input parameters: token from tokenAPI(),path as the address to gateway, 
    , point as name of device and value in (0-100) integer as desired position to actuator.
    API will return True, as an indication of successful execuation.
    """
    metaData = __getMetaData(token, path, point)
    gatewayID = __gatewayIO(token, path)
    object = __getDevice(gatewayID, point)
    value = __setValue(token, object['ObjectId'], value, metaData["dataType"], metaData["defaultProperty"])
    return value
    
# Control API
def controlAPI(token, pt, pos):
    """
    This API has 3 input parameters: token from tokenAPI(), pt as the macro of actuator,
    and pos as the desired position to actuator in (0-100) integer.
    """
    userValue = pos
    pt_a = pt.get('action')
    pt_p = pt.get('position')

    value = actuatorAPI(token, pt_a["path"], pt_a["point"], userValue)
    if value == True:
        #value = sensorAPI(token, pt_a["path"], pt_a["point"])
        # this is command value 
        print(pt_a["name"])
        print("Command is sucessfully sent to actuator: ", userValue)
        acc = 3
        while (acc >0):
            pos = sensorAPI(token, pt_p["path"], pt_p["point"])
            print(pt_p["name"])
            print("The actuator",pt_a["name"], "is opening, at: ", pos)
            acc -=1
            time.sleep(5)
    else:
        print("Error, value not updated for actuator")
        
        
def crossNVAPI(token, op1, op2):
    """
    This API has 3 input parameters: token from tokenAPI(), op1 as desired position to south window,
    op2 as desired position to roof skylight window. Both window positions should be in  (0-100) integer.
    No return value.
    """
    th = 2
    jobs = []
    act = [south_window, roof_skylight]
    openning = [op1, op2]

    for i in range(0, th):
        thread = threading.Thread(target = controlAPI, args=(token,act[i],openning[i]))
        jobs.append(thread)

    for j in jobs:
        j.start()
    
    for j in jobs:
        j.join()

# regroup many operations together    

def isolation3_summer(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    This API will realize 3 operations simultaneously: 
    close the south window, roof skylight window, 
    and open completely the skylight shading.
    No return value.
    """
    th = 3
    jobs = []
    act = [south_window, roof_skylight, roof_shading]
    openning = [0, 0, 100]
    
    for i in range(0, th):
        thread = threading.Thread(target = controlAPI, args=(token,act[i],openning[i]))
        jobs.append(thread)

    for j in jobs:
        j.start()
    
    for j in jobs:
        j.join()    
        
def isolation3(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    This API will realize 3 operations simultaneously: 
    close the south window, roof skylight window, 
    and the skylight shading (all to 0).
    No return value.
    """
    th = 3
    jobs = []
    act = [south_window, roof_skylight, roof_shading]
    openning = [0, 0, 0]
    
    for i in range(0, th):
        thread = threading.Thread(target = controlAPI, args=(token,act[i],openning[i]))
        jobs.append(thread)

    for j in jobs:
        j.start()
    
    for j in jobs:
        j.join()  
    
def isolation(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    This API will realize 4 operations simultaneously: 
    close the south window, roof skylight window, heating valve, 
    and open completely thes skylight shading.
    No return value.
    This function is not recommanded any more
    """
    th = 4
    jobs = []
    act = [south_window, roof_skylight, roof_shading, valve]
    openning = [0, 0, 100, 0]
    
    for i in range(0, th):
        thread = threading.Thread(target = controlAPI, args=(token,act[i],openning[i]))
        jobs.append(thread)

    for j in jobs:
        j.start()
    
    for j in jobs:
        j.join()

#################
## the sensors
###################
_sensors = [
    # weather data
    {"name": "Outdoor temperature", "path": "BACnetNetwork1.Hardware.WindowMaster_Gateway.Local_IO", "point": "Outdoor_temperature"},
    {"name": "Wind direction", "path": "BACnetNetwork1.Hardware.WindowMaster_Gateway.Local_IO", "point": "Wind_direction"},
    {"name": "Wind speed", "path": "BACnetNetwork1.Hardware.WindowMaster_Gateway.Local_IO", "point": "Wind_speed"},
    {"name": "z31 Room temperature", "path": "BACnetNetwork1.Hardware.WindowMaster_Gateway.Local_IO", "point": "Z31_Temperature"},
    {"name": "z31 Slab temperature", "path": "BACnetNetwork1.Hardware.WM_BACnet_Gateway.Local_IO", "point": "Z31_Slaptemp"},
    {"name": "z31 CO2", "path": "BACnetNetwork1.Hardware.WindowMaster_Gateway.Local_IO", "point": "Z31_CO2"},
    {"name": "z31 RH", "path": "BACnetNetwork1.Hardware.WindowMaster_Gateway.Local_IO", "point": "Z31_RH"},
    {"name": "Absolute position south", "path": "BACnetNetwork1.Hardware.WindowMaster_Gateway.Local_IO", "point": "WCC07_1-S1X1_APO"},
    {"name": "Absolute position roof", "path": "BACnetNetwork1.Hardware.WindowMaster_Gateway.Local_IO", "point": "WCC07_1-S1X2_APO"},
    {"name": "Skylight shading position", "path": "BACnetNetwork1.Hardware.WM_BACnet_Gateway.Local_IO", "point": "Z31_Sunscreen_APC"},
    {"name": "z31 Occ", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HZ_RM_3_2_OCC_TL__01"},
    {"name": "z31_BTU_en_rate", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.FLN_25101.HVD_ZERO_BTU13", "point":"Energy_Rate"},
    {"name": "z31_BTU_temp_supply", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.FLN_25101.HVD_ZERO_BTU13", "point":"Supply_Temperature"},
    {"name": "z31_BTU_temp_return", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.FLN_25101.HVD_ZERO_BTU13", "point":"Return_Temperature"},
    {"name": "z31_BTU_vol_rate", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.FLN_25101.HVD_ZERO_BTU13", "point":"Volume_Rate"},
    {"name": "z31 Heating Vanne", "path": "BACnetNetwork1.Hardware.WindowMaster_Gateway.Local_IO", "point": "Z31_Heating_valve"}
]

### SensorAPI
def outdoorT(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[0]["path"], _sensors[0]["point"])

def winddirection(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[1]["path"], _sensors[1]["point"])
    
def windspeed(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[2]["path"], _sensors[2]["point"])
    
def z31roomT(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[3]["path"], _sensors[3]["point"])
    
def z31slabT(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[4]["path"], _sensors[4]["point"])    
    
def z31CO2(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[5]["path"], _sensors[5]["point"]) 
    
def z31RH(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[6]["path"], _sensors[6]["point"]) 
    
def z31south(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[7]["path"], _sensors[7]["point"]) 
    
def z31roof(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[8]["path"], _sensors[8]["point"]) 
    
def z31shading(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[9]["path"], _sensors[9]["point"]) 
    
def z31occ(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[10]["path"], _sensors[10]["point"]) 
    
def EnergyRate(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[11]["path"], _sensors[11]["point"]) 
    
def z31supplyT(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[12]["path"], _sensors[12]["point"]) 
    
def z31returnT(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[13]["path"], _sensors[13]["point"]) 
    
def z31vol(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[14]["path"], _sensors[14]["point"]) 
    
def z31valve(token):
    """
    This API has 1 input parameter: token from tokenAPI().
    """
    return sensorAPI(token, _sensors[15]["path"], _sensors[15]["point"]) 

################
## the actuators
################
_LAB_Valve = [
    {"name": "Heating valve", "path": "BACnetNetwork1.Hardware.WM_BACnet_Gateway.Local_IO", "point": "Z31_HEAT_VALVE"},
    {"name": "Heating valve", "path": "BACnetNetwork1.Hardware.WM_BACnet_Gateway.Local_IO", "point": "Z31_HEAT_VALVE#1"}
]
# fake position sensor in valve 
# {"name": "Heating valve", "path": "BACnetNetwork1.Hardware.WM_BACnet_Gateway.Local_IO", "point": "Z31_HEAT_VALVE#1"}

_LAB_Valve_31 = [
    {"name": "Heating valve", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point": "HVD_ZERO_Z31_VLV"},
    {"name": "Heating valve", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point": "HVD_ZERO_Z31_VLV"}
]

_LAB_Valve_32 = [
    {"name": "Heating valve", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point": "HVD_ZERO_Z32_VLV"},
    {"name": "Heating valve", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point": "HVD_ZERO_Z32_VLV"}
]

_LAB_Valve_33 = [
    {"name": "Heating valve", "path": "BACnetNetwork1.Hardware.WM_BACnet_Gateway.Local_IO", "point": "Z33_HEAT_VALVE"},
    {"name": "Heating valve", "path": "BACnetNetwork1.Hardware.WM_BACnet_Gateway.Local_IO", "point": "Z33_HEAT_VALVE"}
]

_LAB_Actuators = [
    #{"name": "South Window", "path": "BACnetNetwork1.Hardware.WM_BACnet_Gateway.Local_IO", "point": "Z31_WINDOW"},
    {"name": "South Window", "path": "BACnetNetwork1.Hardware.WM_BACnet_Gateway.Local_IO", "point": "WCC007_S1_ML_1"},
    {"name": "Skylight", "path": "BACnetNetwork1.Hardware.WM_BACnet_Gateway.Local_IO", "point": "Z31_SKYLIGHT"},
    {"name": "Skylight Shading", "path": "BACnetNetwork1.Hardware.WM_BACnet_Gateway.Local_IO", "point": "Z31_SKYLIGHT_SHADE"}
    
]
_LAB_Actutators_Pos = [
    {"name": "Absolute position south", "path": "BACnetNetwork1.Hardware.WindowMaster_Gateway.Local_IO", "point": "WCC07_1-S1X1_APO"},
    {"name": "Absolute position roof", "path": "BACnetNetwork1.Hardware.WindowMaster_Gateway.Local_IO", "point": "WCC07_1-S1X2_APO"},
    {"name": "Skylight shading position", "path": "BACnetNetwork1.Hardware.WM_BACnet_Gateway.Local_IO", "point": "Z31_Sunscreen_APC"}
]

south_window = {'action': _LAB_Actuators[0], 'position':_LAB_Actutators_Pos[0]}

    
roof_skylight = {'action': _LAB_Actuators[1], 'position':_LAB_Actutators_Pos[1]}
"""
    This Macro has no input and no return value.
    It is used with controlAPI()
"""
roof_shading = {'action': _LAB_Actuators[2], 'position':_LAB_Actutators_Pos[2]}
"""
    This Macro has no input and no return value.
    It is used with controlAPI()
"""
valve = {'action': _LAB_Valve[0],'position':_LAB_Valve[1]}
"""
    This Macro has no input and no return value.
    It is used with controlAPI()
"""
valve31 = {'action': _LAB_Valve_31[0],'position':_LAB_Valve_31[1]}
valve32 = {'action': _LAB_Valve_32[0],'position':_LAB_Valve_32[1]}
valve33 = {'action': _LAB_Valve_33[0],'position':_LAB_Valve_33[1]}
#########################
##support (private) functions:
#########################

def __gatewayIO(token, path_gateway):
    
    headers = requestHeaders
    #Bearer authentication
    headers['Authorization'] = 'Bearer ' + token
    sysBrowser = 'systembrowser'
    sysBrowserMV = 'System1.ManagementView' + ':' + 'ManagementView' + '.FieldNetworks' + '.' + path_gateway
    
    systemId = '1'
    viewId = '9'
    
    url_path = SERVER_URL + sysBrowser + '/' + systemId + '/' + viewId + '/' + sysBrowserMV
    
    rRequest = requests.Request('GET', url_path, headers=headers)
    #rRequestPrepared = rRequest.prepare()
    
    with requests.Session() as session: 
        rRequestPrepared = session.prepare_request(rRequest)
        rResponse = session.send(rRequestPrepared, verify = False)

    try:
        rResponse.raise_for_status()
    except requests.exceptions.HTTPError as error1:
        print("Http Error:", error1)
    except requests.exceptions.ConnectionError as error2:
        print("Connection Error:", error2)
    except requests.exceptions.Timeout as error3:
        print("Timout Error:", error3)    
    
    
    if (rResponse.status_code == 200):
        rResponseData = json.loads(rResponse.text)
        #print(rResponseData)
        return rResponseData
    else:
        return None

def __getDevice(data, name):
    flag = False
    
    # if data is none, will have 
    #TypeError: 'NoneType' object is not iterable
    
    try: 
        for device in data:
            if device['Name'].lower() == name.lower():
                flag = True
                return device
    except TypeError as e:
        print("Lost the sensor point in Desigo: server problem.")
    
    
    if flag == False:
        print("No device in point is founded in gateway.")
    return None

def __getValues(token, objectId):
    headers = requestHeaders
    headers['Authorization'] = 'Bearer ' + token
    url_v = SERVER_URL + 'values' + '/' + objectId
    rRequest = requests.Request('GET', url_v, headers=headers)
    
    with requests.Session() as session: 
        rRequestPrepared = session.prepare_request(rRequest)
        rResponse = session.send(rRequestPrepared, verify = False)    

    try:
        rResponse.raise_for_status()
    except requests.exceptions.HTTPError as error1:
        print("Http Error:", error1)
    except requests.exceptions.ConnectionError as error2:
        print("Connection Error:", error2)
    except requests.exceptions.Timeout as error3:
        print("Timout Error:", error3)   

    if (rResponse.status_code == 200):
        rResponseData = json.loads(rResponse.text)
        return rResponseData
    else: 
        print("connection failed")
        return None
		
def __getCommand(token, objectId, defaultProperty):
    headers = requestHeaders
    headers['Authorization'] = 'Bearer ' + token
    url_c = SERVER_URL + 'commands' + '/' + objectId + '.' + defaultProperty
    rCommandRequest = requests.Request('GET', url_c, headers=headers)
    
    with requests.Session() as session: 
        rRequestPrepared = session.prepare_request(rCommandRequest)
        rResponse = session.send(rRequestPrepared, verify = False)      

    try:
        rResponse.raise_for_status()
    except requests.exceptions.HTTPError as error1:
        print("Http Error:", error1)
    except requests.exceptions.ConnectionError as error2:
        print("Connection Error:", error2)
    except requests.exceptions.Timeout as error3:
        print("Timout Error:", error3)   
    
    if (rResponse.status_code == 200):
        rResponseData = json.loads(rResponse.text)
        #print(rResponseData)
        return rResponseData
    else:
        return None
    
def __getMetaData(token, path, point):
    
    #path
    gatewayID = __gatewayIO(token, path)
    #point
    object = __getDevice(gatewayID, point)
    
    attributes = object['Attributes']
    defaultProperty = attributes['DefaultProperty']

    command = __getCommand(token, object['ObjectId'], defaultProperty)
    commands = command[0]["Commands"][0]
    parameters = commands["Parameters"][0]
    dataType = parameters["DataType"]
    return {
        "dataType": dataType,
        "defaultProperty": defaultProperty
    }

def __setValue(token, objectId, value, dataType, defaultProperty):
    headers = requestHeaders
    headers['Authorization'] = 'Bearer ' + token
    dataObject = {
        "Name": "Value",
        "Value": value,
        "DataType": dataType
    }
    data = [dataObject]
    url_s = SERVER_URL + 'commands' + '/' + objectId + '.' + defaultProperty + '/Write'
    rRequest = requests.Request('POST', url_s, headers=headers, json=data)
    
    with requests.Session() as session: 
        rRequestPrepared = session.prepare_request(rRequest)
        rResponse = session.send(rRequestPrepared, verify = False)  

    try:
        rResponse.raise_for_status()
    except requests.exceptions.HTTPError as error1:
        print("Http Error:", error1)
    except requests.exceptions.ConnectionError as error2:
        print("Connection Error:", error2)
    except requests.exceptions.Timeout as error3:
        print("Timout Error:", error3)   
    
    if (rResponse.status_code == 200):
        return True
    else:
        return False
        
        
######################################
#Trend
####################
# __getDevice

def __getTrendSeriesInfo(token, objectId):
    headers = requestHeaders
    headers['Authorization'] = 'Bearer ' + token
    url = SERVER_URL + 'trendseriesinfo' + '/' + objectId
    rRequest = requests.Request('GET', url, headers=headers)
    rRequestPrepared = rRequest.prepare()
    
    s = requests.Session()
    rResponse = s.send(rRequestPrepared, verify = False)

    if (rResponse.status_code == 200):
        rResponseData = json.loads(rResponse.text)
        return rResponseData
    return None
    
def __getTrendSeries(token, trendSeriesId, fromDate, toDate, isDescription='true'):
    headers = requestHeaders
    headers['Authorization'] = 'Bearer ' + token
    url = SERVER_URL + 'trendseries' + '/' + trendSeriesId + '?from=' + fromDate + '&to=' + toDate + '&description=' + isDescription
    rRequest = requests.Request('GET', url, headers=headers)
    rRequestPrepared = rRequest.prepare()
    s = requests.Session()
    rResponse = s.send(rRequestPrepared, verify = False)

    try:
        rResponse.raise_for_status()
    except requests.exceptions.HTTPError as error1:
        print("Http Error:", error1)
    except requests.exceptions.ConnectionError as error2:
        print("Connection Error:", error2)
    except requests.exceptions.Timeout as error3:
        print("Timout Error:", error3)  

    if (rResponse.status_code == 200):
        rResponseData = json.loads(rResponse.text)
        return rResponseData
    return None
    
def trendAPI_1(token, Start, End, point):

    gateway = __gatewayIO(token, point["path"])
    obj = __getDevice(gateway , point["point"])
    value = __getTrendSeriesInfo(token, obj['ObjectId'])[0]
    #print(value)
    trendseriesId = value['TrendseriesId']
    SS = Start.astimezone(zulu).strftime('%Y-%m-%dT%H:%M:%SZ')
    EE = End.astimezone(zulu).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    trendData = __getTrendSeries(token, trendseriesId, SS, EE, 'true')
    #print(trendData)
    
    series = trendData['Series']
    
    #Data process in dataframe
    name = point["name"]
    Serie_data = json_normalize(trendData['Series'])
    Serie_data['Value'] = pd.to_numeric(Serie_data['Value'])
    Serie_data.drop(columns = ['Quality', 'QualityGood'], inplace = True)
    Serie_data['Timestamp'] = pd.to_datetime(Serie_data['Timestamp']).dt.tz_convert('US/Eastern').dt.to_period('T')
    
    Serie_data.rename(columns={'Value':name}, inplace=True)
    Serie_data.set_index('Timestamp',inplace=True)
    #S_t = Serie_data.loc[Serie_data['QualityGood']==True,['Value']]
    
    return Serie_data

def trendAPI_2(token, Start, End, point):

    gateway = __gatewayIO(token, point["path"])
    obj = __getDevice(gateway , point["point"])

    value = __getTrendSeriesInfo(token, obj['ObjectId'])[0]
    #print(value)
    trendseriesId = value['TrendseriesId']
    SS = Start.astimezone(zulu).strftime('%Y-%m-%dT%H:%M:%SZ')
    EE = End.astimezone(zulu).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    trendData = __getTrendSeries(token, trendseriesId, SS, EE, 'true')
    #print(trendData)
    
    series = trendData['Series']
    
    #Data process in dataframe
    name = point["name"]
    Serie_data = json_normalize(trendData['Series'])
    Serie_data['Value'] = pd.to_numeric(Serie_data['Value'])
    Serie_data.drop(columns = ['Quality', 'QualityGood'], inplace = True)
    Serie_data['Timestamp'] = pd.to_datetime(Serie_data['Timestamp']).dt.tz_convert('US/Eastern').dt.to_period('T')
    Serie_data.rename(columns={'Value':name}, inplace=True)
    
    return Serie_data.groupby(['Timestamp']).mean()
    
## Trend point##
_trendpoint = [
    {"name": "z31_Slab_temperature", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point": "HVD_ZERO_SLAB_Z31_TEMP_TL_1"},
    {"name": "z31_southwall_temperature", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HZ_RM_3_2_IN_WALL_TEMP_TL__01"},
    {"name": "z31_southwindow", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HZ_RM_3_2_WM_WINDOW_TL__01"},
    {"name": "z31_roofwindow", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HZ_RM_3_2_WM_SKYLGT_TL__01"},
    {"name": "z31_CO2", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HZ_RM_3_2_WM_CO2_TL__01"},
    {"name": "OAT", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HVD_ZERO_NOAA_OAT_TL_1"},
    {"name": "OA_Pressure", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HVD_ZERO_PRESS_SOUTH_3_TL_1"},
    {"name": "Wall_temp_ext", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HVD_ZERO_TEMP_SOUTH_3_TL_1"},
    {"name": "wind_direction", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HVD_ZERO_WIND_DIR_TL_1"},
    {"name": "z31_temp_H", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HZ_RM_3_2_HIGH_TMP_TL__01"},
    {"name": "z31_temp_L", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HZ_RM_3_2_LOW_TEMP_TL__01"},
    {"name": "z31_OCC", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HZ_RM_3_2_OCC_TL__01"},
    {"name": "z31_SKY_Shading", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HZ_RM_3_2_WM_SKY_SHD_TL__01"},
    {"name": "z31_Temp", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HZ_RM_3_2_WM_TEMP_TL__01"},  
    {"name": "z31_vlv", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HZ_RM_3_2_WM_VLV_TL__01"},
    {"name": "z31_BTU_vol_rate", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HVD_ZR_BT13_Vlm_Rt_TL_1"},
    {"name": "z31_BTU_en_rate", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HVD_ZR_BT13_nrgy_Rt_TL_1"},
    {"name": "z31_BTU_temp_supply", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HVD_ZERO_BTU13_AI_3_TL_1"},
    {"name": "z31_BTU_temp_return", "path": "BACnetNetwork1.Hardware.HS_ZERO_NODE_01.Local_IO", "point":"HVD_ZERO_BTU13_AI_4_TL_1"}
    
]

def z31SlabT_trend(token, Start, End):
    return trendAPI_1(token, Start, End, _trendpoint[0])
    
def z31CO2_trend(token, Start, End):
    return trendAPI_2(token, Start, End, _trendpoint[4])
    
def OAT_trend(token, Start, End):
    return trendAPI_1(token, Start, End, _trendpoint[5])
    
def OAP_trend(token, Start, End):
    return trendAPI_1(token, Start, End, _trendpoint[6])
    
def wallext_temp_trend(token, Start, End):
    return trendAPI_1(token, Start, End, _trendpoint[7])
    
def z31_tempH_trend(token, Start, End):
    return trendAPI_2(token, Start, End, _trendpoint[9])
    
def z31_tempL_trend(token, Start, End):
    return trendAPI_2(token, Start, End, _trendpoint[10])
    
def z31_temp_trend(token, Start, End):
    return trendAPI_2(token, Start, End, _trendpoint[13])
    
def z31_occ_trend(token, Start, End):
    return trendAPI_2(token, Start, End, _trendpoint[11])
    
def z31southwindow_trend(token, Start, End):
    return trendAPI_2(token, Start, End, _trendpoint[2])
    
def z31roofwindow_trend(token, Start, End):
    return trendAPI_2(token, Start, End, _trendpoint[3])
    
def z31valve_trend(token, Start, End):
    return trendAPI_2(token, Start, End, _trendpoint[14])
    
def z31BTU_vol_trend(token, Start, End):
    return trendAPI_2(token, Start, End, _trendpoint[15])
    
def z31BTU_energy_trend(token, Start, End):
    return trendAPI_2(token, Start, End, _trendpoint[16])
    
def z31BTU_sT_trend(token, Start, End):
    return trendAPI_2(token, Start, End, _trendpoint[17])
    
def z31BTU_rT_trend(token, Start, End):
    return trendAPI_2(token, Start, End, _trendpoint[18])
##########################################################################    
def occupied_verify(tk, start_datetime, end_datetime):
    df_co2 = z31CO2_trend(tk, start_datetime, end_datetime)
    df_south = z31southwindow_trend(tk, start_datetime, end_datetime)
    df_roof = z31roofwindow_trend(tk, start_datetime, end_datetime)
    # use co2, window position
    dfs = [df_co2, df_south, df_roof]
    df_final = reduce(lambda left,right: pd.merge(left, right,left_index=True, right_index=True), dfs)
    # down sampling for 10 min
    df_T = df_final.resample('10T').max()
    df_T.loc[:,'Occupy'] = 0
    #df_T.loc[(df_T['z31_southwindow']>=50)|(df_T['z31_roofwindow']>=50)|(df_T['z31_CO2']>=700),'Occupy'] = 1
    df_T.loc[(df_T['z31_southwindow']>=50)|(df_T['z31_roofwindow']>=50)|((df_T['z31_CO2'].is_monotonic_increasing) & (df_T['z31_CO2']>=500)),'Occupy'] = 1
    
    return df_T.resample('1H').mean()

################ use of trendAPI
#start_datetime = datetime(2020,9,26,20,0,0)
#end_datetime = datetime(2020,10,1,0,0,0)
#occupied_verify(token, start_datetime, end_datetime)

#zulu = pytz.timezone('Etc/Zulu')
#End = start_datetime.astimezone(zulu).strftime('%Y-%m-%dT%H:%M:%SZ')
#Start = end_datetime.astimezone(zulu).strftime('%Y-%m-%dT%H:%M:%SZ')