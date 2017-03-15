
# Eric Xu , 2017-02, 2017-03

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
from datetime import datetime
from datetime import timedelta

# importing data
all_data = pd.read_csv('/var/www/104.236.40.7/public_html/boston/crime/AllData.csv',
                       dtype={'zip': 'str'})

ZipDF = pd.read_csv('/var/www/104.236.40.7/public_html/boston/crime/ZipDataBase.csv',
                    dtype={'zip': 'str'})

# del ZipDF['population']
html_text_all = (requests.get('http://www.universalhub.com/crime/index.html')).text

html_text = html_text_all[html_text_all.find('<tbody>'):html_text_all.find('</tbody>')]

# print html_text



find_index_list = []
start_find_index = 0
find_index = 0
while find_index != -1:
    find_index = html_text.find('views-field views-field-field-crime-date',start_find_index)
    if find_index != -1:
        find_index_list.append(find_index)
        start_find_index = find_index+20
    
print '...'

datetime_list = []
title_list = []
address_list = []
crime_type_list = []

for index_ in range(0,len(find_index_list)):
    start_index_ = find_index_list[index_]
    try:
        end_index_ = find_index_list[index_+1] 
        text_to_work_with = html_text[start_index_:end_index_]
    except:
        text_to_work_with = html_text[start_index_:]
        
    
    # datetime_list
    find1 = text_to_work_with.find('content="')
    find2 = text_to_work_with.find('"',find1+12)
    datetime_list.append(text_to_work_with[find1+9:find2])
    
    # title_list
    find1 = text_to_work_with.find('<a href="')
    find2 = text_to_work_with.find('">',find1)
    find3 = text_to_work_with.find('</a>',find1)
    title_list.append(text_to_work_with[find2+2:find3])
    
    # crime_type_list
    find1 = text_to_work_with.find('views-field-name')
    find2 = text_to_work_with.find('</td>', find1)
    crime_type_list.append(text_to_work_with[find1+19:find2].strip())
    
    
    # address_list
    find1 = text_to_work_with.find('views-field-street')
    find2 = text_to_work_with.find('</td>', find1)
    address_list.append(text_to_work_with[find1+21:find2].strip())


new_data = pd.DataFrame(zip(datetime_list,title_list,crime_type_list,address_list))

new_data.columns = ['datetime', 'title', 'crime_type', 'address']
maxDateNew = max(new_data['datetime'])



maxDate = (max(all_data['datetime']))

print maxDateNew
print maxDate

if maxDateNew[0:10] > maxDate[0:10]:

    all_data['old'] = 1
    
    mergedDF = DataFrame.merge(new_data, all_data, how='left', on=['datetime',
                                                                    'title',
                                                                    'crime_type',
                                                                    'address'])
    
    del all_data['old']

    mergedDF = mergedDF.reset_index(drop = True)
    
    mergedDF_new = mergedDF[mergedDF['old']!=1]
    
    mergedDF_new = mergedDF_new.reset_index(drop = True)
    
    del mergedDF_new['zip']

    del mergedDF_new['old']

    print mergedDF_new

    mergedDF_new.to_csv('/var/www/104.236.40.7/public_html/boston/crime/new.csv',dtype={'zip': 'str'}, index = False)
    
    # call google maps API
    Link1 = 'https://maps.googleapis.com/maps/api/geocode/json?address='
    Link3 = '&key=AIzaSyD0nxCTgbmoDA-vyRKOjwduqi-XV2BpbF8'

    # second API key
    # Link3='&key=AIzaSyDcnwSKtZ4LFOTT16pU8OW2I3YZKrRlA24'

    zip_candidates = ['02109','02111','02113','02114','02115','02116','02118','02119','02120',
                      '02121','02122','02124','02125','02126','02127','02128','02129','02130',
                      '02131','02132','02134','02135','02136','02151','02199','02203','02210',
                      '02215','02108','02110','02201']

    zip_list = []

    for progress,address in enumerate(mergedDF_new['address']):
        if (progress % 10) == 1:
            print '#################'
            print str(progress) + ' of ' + str(len(mergedDF_new))
            print '#################'

        parsed_string = str(address).replace(' ','+')
        Link2='boston+ma+' + parsed_string
        successful = 0
        totalLink = Link1 + Link2 + Link3
        # call google maps api
        response = requests.get(totalLink) 
        first_boundary = (response.text).find('formatted_address')
        second_boundary = (response.text).find('geometry',first_boundary)
        parsed_text = (response.text)[first_boundary:second_boundary]
        successful = 0
        for zip_candidate in zip_candidates:
            if parsed_text.find(zip_candidate)>-1:
                zip_list.append(zip_candidate)
                successful = 1
                print 'found ' + str(zip_candidate) + '!'
                break
        if successful == 0:
            print 'unsuccessful'
            zip_list.append(None)

    print 'finished google maps api'
    mergedDF_new['zip'] = None
    mergedDF_new['zip'] = zip_list
    
    frames = [mergedDF_new, all_data]
    all_data = pd.concat(frames)

    all_data = all_data.reset_index(drop = True)
    print 'saving all_data'
    all_data.to_csv('/var/www/104.236.40.7/public_html/boston/crime/AllData.csv', dtype={'zip': 'str'}, index = False)
    print 'finished saving all data'
    
    # filtering all_date to last 366 days in dataset
    maxDate = pd.Timestamp(max(all_data['datetime']))
    DateFilter = str(maxDate - timedelta(days=365))
    all_data = all_data[all_data['datetime']>=DateFilter]
    all_data = all_data.reset_index(drop = True)



    # crime type lists
    PERSONAL_CRIMES = ['Assault and battery with a dangerous weapon',
    'Assault and battery',
    'Indecent assault and battery',
    'Assault with a dangerous weapon',
    'Assaul',
    'Sexual attack',
    'Rape',
    'Sexual Assault',
    'Attempted rape',
    'Murder',
    'Armed robbery',
    'Shooting',
    'Stabbing',
    'Hit and run',
    'Armed robbery',
    'Armed Robbery',
    'Robbery',
    'Armed home invasion',
    'Hit and run',
    'Unarmed robbery',
    'Sexual assault',
    'Manslaughter',
    'Leaving the scene of a personal-injury accident',
    'Home invasion',
    'Kidnapping'
    ]

    PERSONAL_CRIMES_Sexual_Offenses = ['Sexual attack',
    'Rape',
    'Sexual Assault',
    'Sexual assault',
    'Attempted rape',
    'Indecent assault and battery']

    PROPERTY_CRIMES = ['Armed robbery','Bank robbery','Armed home invasion',
    'Carjacking','Armed Robbery','Unarmed robbery','Break in','Burglary','Robbery',
    'Home invasion'
    ]

    GUNFIRE_REPORTS = ['Gunfire']


    L_PERSONAL_CRIMES = []
    L_PERSONAL_CRIMES_Sexual_Offenses = []
    L_PROPERTY_CRIMES = []
    L_GUNFIRE_REPORTS = []


    for crime_type in all_data['crime_type']:
        if crime_type in PERSONAL_CRIMES:
            L_PERSONAL_CRIMES.append(1)
        else:
            L_PERSONAL_CRIMES.append(0)

        if crime_type in PERSONAL_CRIMES_Sexual_Offenses:
            L_PERSONAL_CRIMES_Sexual_Offenses.append(1)
        else:
            L_PERSONAL_CRIMES_Sexual_Offenses.append(0)

        if crime_type in PROPERTY_CRIMES:
            L_PROPERTY_CRIMES.append(1)
        else:
            L_PROPERTY_CRIMES.append(0)

        if crime_type in GUNFIRE_REPORTS:
            L_GUNFIRE_REPORTS.append(1)
        else:
            L_GUNFIRE_REPORTS.append(0)


    all_data['PERSONAL_CRIMES'] = L_PERSONAL_CRIMES
    all_data['PERSONAL_CRIMES_Sexual_Offenses'] = L_PERSONAL_CRIMES_Sexual_Offenses
    all_data['PROPERTY_CRIMES'] = L_PROPERTY_CRIMES
    all_data['GUNFIRE_REPORTS'] = L_GUNFIRE_REPORTS


    ## ALL CRIMES
    CRIMES_DF = all_data.copy()
    CRIMES_DF = CRIMES_DF.reset_index(drop = True)

    newDF = pd.DataFrame(zip(CRIMES_DF['zip'].value_counts().sort_index().index,
                             CRIMES_DF['zip'].value_counts().sort_index().values))

    newDF.columns = ['zip','value']

    MergedDF = DataFrame.merge(ZipDF,newDF,on=['zip'],how='left')
    MergedDF = MergedDF.fillna(0)

    value2 = []
    value3 = []

    MergedDF['value2'] = MergedDF['value']/MergedDF['sqmiles']
    MergedDF['value3'] =  MergedDF['value']/MergedDF['population']

    DF1 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value']))
    DF2 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value2']))
    DF3 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value3']))

    DF1.columns = ['zip','value']
    DF2.columns = ['zip','value']
    DF3.columns = ['zip','value']

    DF1.to_csv('/var/www/104.236.40.7/public_html/boston/crime/ZipData.csv',dtype={'zip': 'str'},float_format='%.f', index = False)
    DF2.to_csv('/var/www/104.236.40.7/public_html/boston/crime/normalized.csv',dtype={'zip': 'str'}, float_format='%.2f',index = False)
    DF3.to_csv('/var/www/104.236.40.7/public_html/boston/crime/normalized_pop.csv',dtype={'zip': 'str'},float_format='%.2f', index = False)
    print 'finished ALL CRIMES'



    ## PERSONAL_CRIMES
    CRIMES_DF = all_data[all_data['PERSONAL_CRIMES']==1]
    CRIMES_DF = CRIMES_DF.reset_index(drop = True)

    newDF = pd.DataFrame(zip(CRIMES_DF['zip'].value_counts().sort_index().index,
                             CRIMES_DF['zip'].value_counts().sort_index().values))

    newDF.columns = ['zip','value']

    MergedDF = DataFrame.merge(ZipDF,newDF,on=['zip'],how='left')
    MergedDF = MergedDF.fillna(0)

    value2 = []
    value3 = []

    MergedDF['value2'] = MergedDF['value']/MergedDF['sqmiles']
    MergedDF['value3'] =  MergedDF['value']/MergedDF['population']

    DF1 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value']))
    DF2 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value2']))
    DF3 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value3']))

    DF1.columns = ['zip','value']
    DF2.columns = ['zip','value']
    DF3.columns = ['zip','value']

    DF2.to_csv('/var/www/104.236.40.7/public_html/boston/crime/personal_n.csv',dtype={'zip': 'str'}, float_format='%.2f',index = False)
    DF1.to_csv('/var/www/104.236.40.7/public_html/boston/crime/personal.csv',dtype={'zip': 'str'},float_format='%.f', index = False)
    DF3.to_csv('/var/www/104.236.40.7/public_html/boston/crime/personal_pop.csv',dtype={'zip': 'str'},float_format='%.2f', index = False)
    print 'finished PERSONAL CRIMES'




    ## PERSONAL_SEXUAL_CRIMES
    CRIMES_DF = all_data[all_data['PERSONAL_CRIMES_Sexual_Offenses']==1]
    CRIMES_DF = CRIMES_DF.reset_index(drop = True)

    newDF = pd.DataFrame(zip(CRIMES_DF['zip'].value_counts().sort_index().index,
                             CRIMES_DF['zip'].value_counts().sort_index().values))

    newDF.columns = ['zip','value']

    MergedDF = DataFrame.merge(ZipDF,newDF,on=['zip'],how='left')
    MergedDF = MergedDF.fillna(0)

    value2 = []
    value3 = []

    MergedDF['value2'] = MergedDF['value']/MergedDF['sqmiles']
    MergedDF['value3'] =  MergedDF['value']/MergedDF['population']

    DF1 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value']))
    DF2 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value2']))
    DF3 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value3']))

    DF1.columns = ['zip','value']
    DF2.columns = ['zip','value']
    DF3.columns = ['zip','value']

    DF3.to_csv('/var/www/104.236.40.7/public_html/boston/crime/personal_sexual_pop.csv',dtype={'zip': 'str'}, float_format='%.2f',index = False)
    DF2.to_csv('/var/www/104.236.40.7/public_html/boston/crime/personal_sexual_n.csv',dtype={'zip': 'str'}, float_format='%.2f',index = False)
    DF1.to_csv('/var/www/104.236.40.7/public_html/boston/crime/personal_sexual.csv',dtype={'zip': 'str'},float_format='%.f', index = False)
    print 'finished PERSONAL SEXUAL CRIMES'


    ## PROPERTY_CRIMES
    CRIMES_DF = all_data[all_data['PROPERTY_CRIMES']==1]
    CRIMES_DF = CRIMES_DF.reset_index(drop = True)

    newDF = pd.DataFrame(zip(CRIMES_DF['zip'].value_counts().sort_index().index,
                             CRIMES_DF['zip'].value_counts().sort_index().values))

    newDF.columns = ['zip','value']

    MergedDF = DataFrame.merge(ZipDF,newDF,on=['zip'],how='left')
    MergedDF = MergedDF.fillna(0)

    value2 = []
    value3 = []

    MergedDF['value2'] = MergedDF['value']/MergedDF['sqmiles']
    MergedDF['value3'] =  MergedDF['value']/MergedDF['population']

    DF1 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value']))
    DF2 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value2']))
    DF3 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value3']))

    DF1.columns = ['zip','value']
    DF2.columns = ['zip','value']
    DF3.columns = ['zip','value']

    DF3.to_csv('/var/www/104.236.40.7/public_html/boston/crime/property_pop.csv',dtype={'zip': 'str'}, float_format='%.2f',index = False)
    DF2.to_csv('/var/www/104.236.40.7/public_html/boston/crime/property_n.csv',dtype={'zip': 'str'}, float_format='%.2f',index = False)
    DF1.to_csv('/var/www/104.236.40.7/public_html/boston/crime/property.csv',dtype={'zip': 'str'},float_format='%.f', index = False)
    print 'finished PROPERTY CRIMES'





    ## GUNFIRE CRIMES
    CRIMES_DF = all_data[all_data['GUNFIRE_REPORTS']==1]
    CRIMES_DF = CRIMES_DF.reset_index(drop = True)

    newDF = pd.DataFrame(zip(CRIMES_DF['zip'].value_counts().sort_index().index,
                             CRIMES_DF['zip'].value_counts().sort_index().values))

    newDF.columns = ['zip','value']

    MergedDF = DataFrame.merge(ZipDF,newDF,on=['zip'],how='left')
    MergedDF = MergedDF.fillna(0)

    value2 = []
    value3 = []

    MergedDF['value2'] = MergedDF['value']/MergedDF['sqmiles']
    MergedDF['value3'] =  MergedDF['value']/MergedDF['population']

    DF1 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value']))
    DF2 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value2']))
    DF3 = pd.DataFrame(zip(MergedDF['zip'], MergedDF['value3']))

    DF1.columns = ['zip','value']
    DF2.columns = ['zip','value']
    DF3.columns = ['zip','value']
    DF3.to_csv('/var/www/104.236.40.7/public_html/boston/crime/gunfire_pop.csv',dtype={'zip': 'str'}, float_format='%.2f',index = False)
    DF2.to_csv('/var/www/104.236.40.7/public_html/boston/crime/gunfire_n.csv',dtype={'zip': 'str'}, float_format='%.2f',index = False)
    DF1.to_csv('/var/www/104.236.40.7/public_html/boston/crime/gunfire.csv',dtype={'zip': 'str'},float_format='%.f', index = False)
    print 'finished GUNFIRE CRIMES'

else:
    print 'no updates found'

    
# filtering all_date to last 366 days in dataset
maxDate = pd.Timestamp(max(all_data['datetime']))
DateFilter = str(maxDate - timedelta(days=365))
all_data = all_data[all_data['datetime']>=DateFilter]
all_data = all_data.reset_index(drop = True)



# updating dates and update time
date_min_list = []
date_max_list = []
updatetime_list = []

date_min_list.append(str(min(all_data['datetime']))[0:10])
date_max_list.append(str(max(all_data['datetime']))[0:10])
string_value = time.strftime('%l:%M%p ' + '(EST) ' + 'on %b %d, %Y')
updatetime_list.append(string_value)


dateDF = pd.DataFrame(zip(date_min_list,date_max_list,updatetime_list))
dateDF.columns = ['mindate','maxdate','updatetime']
dateDF.to_csv('/var/www/104.236.40.7/public_html/boston/crime/displaydates_boston.csv', index = False)

print 'made it to the bitter end!'

