# Eric Xu, 2017-05

import gc
import pandas as pd
from pandas import DataFrame
import glob
import datetime
from datetime import date
import urllib2
import re
import os
import math
import numpy as np
import requests
import json
import time
import urllib

print 'retrieving latest data'
urllib.urlretrieve ("https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv", "/var/www/104.236.40.7/public_html/nyc/collisions/NYPD_Motor_Vehicle_Collisions.csv")

print 'done retrieving latest data!'
gc.collect()

All_Data = pd.read_csv('/var/www/104.236.40.7/public_html/nyc/collisions/AllData.csv')

NYPD = pd.read_csv('/var/www/104.236.40.7/public_html/nyc/collisions/NYPD_Motor_Vehicle_Collisions.csv')

new_date_format = []
for each_date_to_format in NYPD['DATE']:
    try:
        new_date_format.append(datetime.datetime.strptime(each_date_to_format, "%m/%d/%Y").strftime("%Y-%m-%d"))
    except:
        new_date_format.append(each_date_to_format)
NYPD['DATE'] = new_date_format

print 'filtering NYPD data to last 365 days'
maxDate = max([pd.Timestamp(max(NYPD['DATE'])),pd.Timestamp(max(All_Data['DATE']))])
YearFilter = str(maxDate - datetime.timedelta(days=364))[0:10]

NYPD = NYPD[NYPD['DATE']>=YearFilter]
NYPD = NYPD.reset_index(drop = True)

print 'filtering All_Data data to last 365 days'

All_Data = All_Data[All_Data['DATE']>=YearFilter]
All_Data = All_Data.reset_index(drop = True)



NYPD_keys = set(NYPD['UNIQUE KEY'])
CURRENT_keys = set(All_Data['UNIQUE KEY'])
gc.collect()

NEW_keys = []


for each_nypd_key in NYPD_keys:
    if each_nypd_key not in CURRENT_keys:
        NEW_keys.append(each_nypd_key)

if len(NEW_keys) > 0:
    new_keys_DF = NYPD[NYPD['UNIQUE KEY'].isin(NEW_keys)]
    new_keys_DF = new_keys_DF.reset_index(drop = True)
    Rawdf = new_keys_DF    

    #add new columns
    Rawdf['YEAR']=None
    Rawdf['HOUR']=None
    Rawdf['MONTH']=None
    Rawdf['DAY']=None
    

    
    

    MONTH = []
    DAY = []
    YEAR = []
    HOUR = []

    zipped_array = zip(Rawdf['DATE'], Rawdf['TIME'])


    # ADD MONTH, DAY, YEAR, and HOUR columns

    for index,values in enumerate(zipped_array):
        DATE = str(values[0])
        TIME = values[1]
        if index % 90000 == 1000:
            print str(index) + ' of ' + str(len(zipped_array))
        MONTH.append(DATE[5:7])
        DAY.append(DATE[8:10])
        YEAR.append(DATE[0:4])
        hour_value = int(TIME[0:2].replace(':',''))

        if hour_value==0:
            HOUR.append('12:51 AM')
        elif hour_value==1:
            HOUR.append('1:51 AM')
        elif hour_value==2:
            HOUR.append('2:51 AM')
        elif hour_value==3:
            HOUR.append('3:51 AM')
        elif hour_value==4:
            HOUR.append('4:51 AM')
        elif hour_value==5:
            HOUR.append('5:51 AM')
        elif hour_value==6:
            HOUR.append('6:51 AM')
        elif hour_value==7:
            HOUR.append('7:51 AM')
        elif hour_value==8:
            HOUR.append('8:51 AM')
        elif hour_value==9:
            HOUR.append('9:51 AM')
        elif hour_value==10:
            HOUR.append('10:51 AM')
        elif hour_value==11:
            HOUR.append('11:51 AM')
        elif hour_value==12:
            HOUR.append('12:51 PM')
        elif hour_value==13:
            HOUR.append('1:51 PM')
        elif hour_value==14:
            HOUR.append('2:51 PM')
        elif hour_value==15:
            HOUR.append('3:51 PM')
        elif hour_value==16:
            HOUR.append('4:51 PM')
        elif hour_value==17:
            HOUR.append('5:51 PM')
        elif hour_value==18:
            HOUR.append('6:51 PM')
        elif hour_value==19:
            HOUR.append('7:51 PM')
        elif hour_value==20:
            HOUR.append('8:51 PM')
        elif hour_value==21:
            HOUR.append('9:51 PM')
        elif hour_value==22:
            HOUR.append('10:51 PM')
        elif hour_value==23:
            HOUR.append('11:51 PM')

    Rawdf['YEAR']=YEAR
    Rawdf['HOUR']=HOUR
    Rawdf['MONTH']=MONTH
    Rawdf['DAY']=DAY


    Rawdf['DAY OF WEEK'] = None
    DAY_OF_WEEK = []

    zipped_array = zip(Rawdf['DAY'], Rawdf['MONTH'], Rawdf['YEAR'])

    for index,values in enumerate(zipped_array):
        DAY = int(values[0])
        MONTH = int(values[1])
        YEAR = int(values[2])
        if index % 90000 == 1000:
            print str(index) + ' of ' + str(len(zipped_array))
        if index == 0:
            module_value = date(YEAR, MONTH, DAY).weekday()
            if module_value==0:
                dow_value = 'Monday'
            elif module_value==1:
                dow_value = 'Tuesday'
            elif module_value==2:
                dow_value = 'Wednesday'
            elif module_value==3:
                dow_value = 'Thursday'
            elif module_value==4:
                dow_value = 'Friday'
            elif module_value==5:
                dow_value = 'Saturday'
            elif module_value==6:
                dow_value = 'Sunday'
            else:
                dow_value = 'Unknown'
                print 'unknown day of week'
            DAY_OF_WEEK.append(dow_value)

            Previous_Day = DAY
            Previous_Month = MONTH
            Previous_DOW = dow_value

        # processes this loop when not the first index
        else:
            if (Previous_Day!=DAY) or (Previous_Month!=MONTH):
                module_value = date(YEAR, MONTH, DAY).weekday()
                if module_value==0:
                    dow_value = 'Monday'
                elif module_value==1:
                    dow_value = 'Tuesday'
                elif module_value==2:
                    dow_value = 'Wednesday'
                elif module_value==3:
                    dow_value = 'Thursday'
                elif module_value==4:
                    dow_value = 'Friday'
                elif module_value==5:
                    dow_value = 'Saturday'
                elif module_value==6:
                    dow_value = 'Sunday'
                else:
                    dow_value = 'Unknown'
                    print 'unknown dow'
                DAY_OF_WEEK.append(dow_value)

            else:
                DAY_OF_WEEK.append(Previous_DOW)

            Previous_Day = DAY
            Previous_Month = MONTH
            Previous_DOW = dow_value

    Rawdf['DAY OF WEEK'] = DAY_OF_WEEK

    print '...'
    
    
    
    BRONX=[10453, 10457, 10460, 10458, 10467, 10468, 10451, 10452, 10456, 10454, 10455, 10459, 
           10474, 10463, 10471, 10466, 10469, 10470, 10475, 10461, 10462,10464, 10465, 10472, 
           10473] 

    BROOKLYN=[11212, 11213, 11216, 11233, 11238, 11209, 11214, 11228, 11204, 11218, 11219, 
              11230, 11234, 11236, 11239,11223, 11224, 11229, 11235,11201, 11205, 11215, 
              11217, 11231,11203, 11210, 11225, 11226, 11207, 11208,11211, 11222, 11220, 
              11232, 11206, 11221, 11237] 

    MANHATTAN=[10026, 10027, 10030, 10037, 10039, 10001, 10011, 10018, 10019, 10020, 10036, 
               10029, 10035, 10010, 10016, 10017, 10022, 10012, 10013, 10014, 10004, 10005, 
               10006, 10007, 10038, 10280, 10002, 10003, 10009, 10021, 10028, 10044, 10128, 
               10023, 10024, 10025, 10031, 10032, 10033, 10034, 10040, 10178, 10279, '00083']

    QUEENS=[11361, 11362, 11363, 11364, 11354, 11355, 11356, 11357, 11358, 11359, 11360, 11365, 
            11366, 11367,11412, 11423, 11432, 11433, 11434, 11435, 11436, 11101, 11102, 11103, 
            11104, 11105, 11106, 11374, 11375, 11379, 11385, 11691, 11692, 11693, 11694, 11695, 
            11697, 11004, 11005, 11411, 11413, 11422, 11426, 11427, 11428, 11429, 11414, 11415, 
            11416, 11417, 11418, 11419, 11420, 11421, 11368, 11369, 11370, 11372, 11373, 11377, 
            11378]

    STATENISLAND=[10302, 10303, 10310, 10306, 10307, 10308, 10309, 10312, 10301, 10304, 
                  10305, 10314]

    Link1='https://maps.googleapis.com/maps/api/geocode/json?address='
    Link3 = '&key=AIzaSyD0nxCTgbmoDA-vyRKOjwduqi-XV2BpbF8'

    # creating a dictinary of boroughs to match each nyc zip code to a borough
    zip2borough = {}
    for borough in ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATENISLAND"]:
        for zipCode in eval(borough):
            zip2borough[str(zipCode)] = borough


    # finding which indices are valid for the following columns
    S1NN=Rawdf['ON STREET NAME'].notnull()
    S2NN=Rawdf['CROSS STREET NAME'].notnull()
    S3NN=Rawdf['OFF STREET NAME'].notnull()


    zipped_array = zip(Rawdf['ON STREET NAME'], Rawdf['CROSS STREET NAME'], 
                       Rawdf['OFF STREET NAME'], Rawdf['ZIP CODE'], S1NN, S2NN, S3NN, 
                       Rawdf['BOROUGH'])

    Rawdf['ZIP CODE'] = None
    Rawdf['BOROUGH'] = None
    NEW_ZIPS = []
    NEW_BOROUGHS = []
    OverLimit = 0

    for index,values in enumerate(zipped_array):  
        # valid zip code or 'Unknown' : Continue = 0
        # invalid zip code: Continue = 1
        Continue=0
        try:
            # valid zip code found
            try_me = int(float(values[3]))
            Continue = 0
        except:
            if str(values[3])=='Unknown':
                Continue = 0
            else:
                Continue = 1

        if (Continue == 0):
            if str(values[3])=='':
                Continue = 1

        # not over limit and no valid zip code
        if (Continue ==1) and (OverLimit == 0):
            print '-----------------'
            print ''.join(['working on ', str(index), ' of ', str(len(zipped_array))])
            Link2=''
            # street names present
            if True in [values[4], values[5], values[6]]:
                if values[4]==True:
                    Link2=values[0]
                    Link2=Link2.replace(' ','+')
                else:
                    Link2=''
                if values[5]==True:
                    if Link2=='':
                        Link2=values[1]
                        Link2=Link2.replace(' ','+')
                    else:
                        Link2=Link2+'+and+'+values[1]
                        Link2=Link2.replace(' ','+')
                else:
                    Link2=Link2+''
                if values[6]==True:
                    if Link2=='':
                        Link2=values[2]
                        Link2=Link2.replace(' ','+')
                    else:
                        Link2=Link2+'+and+'+values[2]
                        Link2=Link2.replace(' ','+')
                else:
                    Link2=Link2+''
                Link2='ny+'+Link2


                # go forward and call the google maps api only if cross streets were found
                successful = 0
                totalLink = Link1 + Link2 + Link3
                # call google maps api
                response = requests.get(totalLink) 
                if (response.text).find('OVER_QUERY_LIMIT')>0:
                    OverLimit = 1
                    print 'over query limit'

                if (response.text).count('ZERO_RESULTS') > 0:
                    # add ny+(ny)
                    Link2 = 'ny+' + Link2
                    totalLink = Link1 + Link2 + Link3
                    # call google maps api
                    print 'calling: ' + str(totalLink)
                    response = requests.get(totalLink)
                    if (response.text).count('ZERO_RESULTS') > 0:
                        pass
                    elif (response.text).find('OVER_QUERY_LIMIT')>0:
                        OverLimit = 1
                        print 'over query limit'
                        break
                    else:
                        successful = 1
                else:
                    successful = 1                    

                if successful == 1:
                    zipValue = None
                    boroughValue = None
                    for zipCode in BRONX:
                        FoundZip = (response.text).find(str(zipCode))
                        if FoundZip > 0:
                            boroughValue = 'BRONX'
                            zipValue = zipCode
                            break
                    if FoundZip == -1:
                        for zipCode in BROOKLYN:
                            FoundZip = (response.text).find(str(zipCode))
                            if FoundZip > 0:
                                boroughValue = 'BROOKLYN'
                                zipValue = zipCode
                                break
                    if FoundZip == -1:
                        for zipCode in MANHATTAN:
                            FoundZip = (response.text).find(str(zipCode))
                            if FoundZip > 0:
                                boroughValue = 'MANHATTAN'
                                zipValue = zipCode
                                break

                    if FoundZip == -1:
                        for zipCode in QUEENS:
                            FoundZip = (response.text).find(str(zipCode))
                            if FoundZip > 0:
                                boroughValue = 'QUEENS'
                                zipValue = zipCode
                                break

                    if FoundZip == -1:
                        for zipCode in STATENISLAND:
                            FoundZip = (response.text).find(str(zipCode))
                            if FoundZip > 0:
                                boroughValue = 'STATEN ISLAND'
                                zipValue = zipCode
                                break


                    if boroughValue!=None:
                        NEW_ZIPS.append(zipValue)
                        NEW_BOROUGHS.append(boroughValue)
                        print 'SUCCESS!!'
                        time.sleep(.3)
                        if index % 500 == 2:
                            print '...'
                    # can't find borough  
                    else:
                        NEW_ZIPS.append('Unknown')
                        NEW_BOROUGHS.append('Unknown')
                # if unsuccessful
                else:
                    NEW_ZIPS.append('Unknown')
                    NEW_BOROUGHS.append('Unknown')
            # no valid streets
            else:
                NEW_ZIPS.append('Unknown')
                NEW_BOROUGHS.append('Unknown')
        # no valid zip code found but overlimit
        elif (Continue == 1) and (OverLimit==1):
            NEW_ZIPS.append(values[3])
            NEW_BOROUGHS.append(values[7])

        # zip code exists
        elif Continue == 0:
            NEW_ZIPS.append(values[3])
            NEW_BOROUGHS.append(values[7])
        # do nothing
        elif OverLimit == 0:
            NEW_ZIPS.append(values[3])
            NEW_BOROUGHS.append(values[7])




    if OverLimit == 1:
        Rawdf['ZIP CODE'] = NEW_ZIPS
        Rawdf['BOROUGH'] = NEW_BOROUGHS
        print 'over limit -- trying second key'
        run_second_key = 1
    else:
        run_second_key = 0
        Rawdf['ZIP CODE'] = NEW_ZIPS
        Rawdf['BOROUGH'] = NEW_BOROUGHS



    if run_second_key == 1:
	gc.collect()
        # finding which indices are valid for the following columns
        S1NN=Rawdf['ON STREET NAME'].notnull()
        S2NN=Rawdf['CROSS STREET NAME'].notnull()
        S3NN=Rawdf['OFF STREET NAME'].notnull()


        zipped_array = zip(Rawdf['ON STREET NAME'], Rawdf['CROSS STREET NAME'], 
                           Rawdf['OFF STREET NAME'], Rawdf['ZIP CODE'], S1NN, S2NN, S3NN, 
                           Rawdf['BOROUGH'])

        Rawdf['BOROUGH'] = None
        NEW_ZIPS = []
        NEW_BOROUGHS = []
        OverLimit = 0
        # new key
        Link3='&key=AIzaSyDcnwSKtZ4LFOTT16pU8OW2I3YZKrRlA24'
        for index,values in enumerate(zipped_array):  
            # valid zip code or 'Unknown' : Continue = 0
            # invalid zip code: Continue = 1
            Continue=0
            try:
                # valid zip code found
                try_me = int(float(values[3]))
                Continue = 0
            except:
                if str(values[3])=='Unknown':
                    Continue = 0
                else:
                    Continue = 1

            if (Continue == 0):
                if str(values[3])=='':
                    Continue = 1

            # not over limit and no valid zip code
            if (Continue ==1) and (OverLimit == 0):
                print '-----------------'
                print ''.join(['working on ', str(index), ' of ', str(len(zipped_array))])
                Link2=''
                # street names present
                if True in [values[4], values[5], values[6]]:
                    if values[4]==True:
                        Link2=values[0]
                        Link2=Link2.replace(' ','+')
                    else:
                        Link2=''
                    if values[5]==True:
                        if Link2=='':
                            Link2=values[1]
                            Link2=Link2.replace(' ','+')
                        else:
                            Link2=Link2+'+and+'+values[1]
                            Link2=Link2.replace(' ','+')
                    else:
                        Link2=Link2+''
                    if values[6]==True:
                        if Link2=='':
                            Link2=values[2]
                            Link2=Link2.replace(' ','+')
                        else:
                            Link2=Link2+'+and+'+values[2]
                            Link2=Link2.replace(' ','+')
                    else:
                        Link2=Link2+''
                    Link2='ny+'+Link2


                    # go forward and call the google maps api only if cross streets were found
                    successful = 0
                    totalLink = Link1 + Link2 + Link3
                    # call google maps api
                    response = requests.get(totalLink) 
                    if (response.text).find('OVER_QUERY_LIMIT')>0:
                        OverLimit = 1
                        print 'over query limit'

                    if (response.text).count('ZERO_RESULTS') > 0:
                        # add ny+(ny)
                        Link2 = 'ny+' + Link2
                        totalLink = Link1 + Link2 + Link3
                        # call google maps api
                        print 'calling: ' + str(totalLink)
                        response = requests.get(totalLink)
                        if (response.text).count('ZERO_RESULTS') > 0:
                            pass
                        elif (response.text).find('OVER_QUERY_LIMIT')>0:
                            OverLimit = 1
                            print 'over query limit'
                            break
                        else:
                            successful = 1
                    else:
                        successful = 1                    

                    if successful == 1:
                        zipValue = None
                        boroughValue = None
                        for zipCode in BRONX:
                            FoundZip = (response.text).find(str(zipCode))
                            if FoundZip > 0:
                                boroughValue = 'BRONX'
                                zipValue = zipCode
                                break
                        if FoundZip == -1:
                            for zipCode in BROOKLYN:
                                FoundZip = (response.text).find(str(zipCode))
                                if FoundZip > 0:
                                    boroughValue = 'BROOKLYN'
                                    zipValue = zipCode
                                    break
                        if FoundZip == -1:
                            for zipCode in MANHATTAN:
                                FoundZip = (response.text).find(str(zipCode))
                                if FoundZip > 0:
                                    boroughValue = 'MANHATTAN'
                                    zipValue = zipCode
                                    break

                        if FoundZip == -1:
                            for zipCode in QUEENS:
                                FoundZip = (response.text).find(str(zipCode))
                                if FoundZip > 0:
                                    boroughValue = 'QUEENS'
                                    zipValue = zipCode
                                    break

                        if FoundZip == -1:
                            for zipCode in STATENISLAND:
                                FoundZip = (response.text).find(str(zipCode))
                                if FoundZip > 0:
                                    boroughValue = 'STATEN ISLAND'
                                    zipValue = zipCode
                                    break


                        if boroughValue!=None:
                            NEW_ZIPS.append(zipValue)
                            NEW_BOROUGHS.append(boroughValue)
                            print 'SUCCESS!!'
                            time.sleep(.3)
                            if index % 500 == 2:
                                print '...'
                        # can't find borough  
                        else:
                            NEW_ZIPS.append('Unknown')
                            NEW_BOROUGHS.append('Unknown')
                    # if unsuccessful
                    else:
                        NEW_ZIPS.append('Unknown')
                        NEW_BOROUGHS.append('Unknown')
                # no valid streets
                else:
                    NEW_ZIPS.append('Unknown')
                    NEW_BOROUGHS.append('Unknown')
            # no valid zip code found but overlimit
            elif (Continue == 1) and (OverLimit==1):
                NEW_ZIPS.append(values[3])
                NEW_BOROUGHS.append(values[7])

            # zip code exists
            elif Continue == 0:
                NEW_ZIPS.append(values[3])
                NEW_BOROUGHS.append(values[7])
            # do nothing
            elif OverLimit == 0:
                NEW_ZIPS.append(values[3])
                NEW_BOROUGHS.append(values[7])




        if OverLimit == 1:
            Rawdf['ZIP CODE'] = NEW_ZIPS
            Rawdf['BOROUGH'] = NEW_BOROUGHS
            print 'over limit again -- may need to be re-run'
        else:
            Rawdf['ZIP CODE'] = NEW_ZIPS
            Rawdf['BOROUGH'] = NEW_BOROUGHS


    print 'done using google maps api'

    print 'All_Data'
    each_new_list = []
    for each in list(All_Data.columns):
        print each
        each_new = each.replace(' ','_')
        each_new_list.append(each_new)
        # print 'executing: ' + ''.join([each_new,'=[]'])
        exec(''.join([each_new,'=[]']))
        
        for each2 in All_Data[each]:
            exec(''.join([each_new,'.append(each2)']))
    print 'Rawdf'        
    for each in list(Rawdf.columns):
        print each
        each_new = each.replace(' ','_')
        for each2 in Rawdf[each]:
            exec(''.join([each_new,'.append(each2)']))
                
    print '...' 


    gc.collect()
    long_string = 'data_all=pd.DataFrame(zip(' 

    for ind_, each in enumerate(each_new_list):
        if ind_ == 0:
            long_string = long_string + str(each)
        else:
            long_string = long_string + ',' + str(each)
            
    long_string = long_string + '))'

    exec(long_string)
    gc.collect()

    data_all.columns = list(All_Data.columns)
    data_all = data_all.drop_duplicates(['UNIQUE KEY'])
    data_all = data_all.reset_index(drop = True)
    data_all.to_csv('/var/www/104.236.40.7/public_html/nyc/collisions/AllData.csv', index = False)
    gc.collect()
    print 'done'
    '''
    # concatenating data
    frames = [All_Data, Rawdf]
    data_all1 = pd.concat(frames)
    data_all = data_all1.reset_index(drop = True)
    print 'finished concatenating data'
    gc.collect()
    data_all.to_csv('/var/www/104.236.40.7/public_html/nyc/collisions/AllData.csv', index = False)
    gc.collect()
    print 'done'
    '''
