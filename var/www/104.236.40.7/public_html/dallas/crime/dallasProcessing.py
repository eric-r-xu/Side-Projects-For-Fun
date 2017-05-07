# Eric Xu, 2017-04, 2017-05

import glob
import gc
import pandas as pd
from pandas import DataFrame
from datetime import datetime
import numpy as np
import urllib
import time
from datetime import timedelta
'''for each in glob.glob('*'):
    print each'''

print 'retrieving latest data'
urllib.urlretrieve ("https://www.dallasopendata.com/api/views/tbnj-w5hb/rows.csv", "/var/www/104.236.40.7/public_html/dallas/crime/Police_Incidents.csv")
print 'finished retrieving latest data'


data = pd.read_csv('/var/www/104.236.40.7/public_html/dallas/crime/Police_Incidents.csv')
pop_area = pd.read_csv('/var/www/104.236.40.7/public_html/dallas/crime/ZipDataBase.csv')

newDates = []

# filtering out data with no dates or zip
data = data.fillna(0)

data = data[data['Date of Report']!=0]
data = data.reset_index(drop = True)

data = data[data['Zip Code']!=0]
data = data.reset_index(drop = True)


for each in data['Date of Report']:
    newDates.append(datetime.strptime(each[0:10], "%m/%d/%Y").strftime("%Y-%m-%d"))
 

data['Date of Report'] = newDates


data_abridged = data[['Date of Report', 'Incident Number w/ Year', 'Zip Code', 'UCR Offense Description']]

data_abridged = data_abridged.reset_index(drop = True)




data_abridged = data_abridged.sort_values(['Date of Report'], ascending = True)

data_abridged = data_abridged.reset_index(drop = True)


curdate = time.strftime("%Y-%m-%d")


data_abridged = data_abridged[data_abridged['Date of Report']<=curdate]
data_abridged = data_abridged.reset_index(drop = True)




data_all = data_abridged.drop_duplicates(['Incident Number w/ Year'])


data_all = data_all.reset_index(drop = True)
data_all.to_csv('/var/www/104.236.40.7/public_html/dallas/crime/data_abridged.csv', index = False)


maxDate = pd.Timestamp(max(data_all['Date of Report']))
DateFilter365 = str(maxDate - timedelta(days=365))[0:10]
DateFilter30 = str(maxDate - timedelta(days=30))[0:10]



newDF = pd.DataFrame(zip(data_all['Zip Code'].value_counts().index, data_all['Zip Code'].value_counts().values))
newDF.columns = ['zip','value']

newzip = []
for each in newDF['zip']:
    newzip.append(int(str(each)[0:5]))
    
    
newDF['zip'] = newzip
newDF.to_csv('/var/www/104.236.40.7/public_html/dallas/crime/ZipData.csv', index = False)


# normalization for area and population
mergedDF = DataFrame.merge(pop_area, newDF, how='inner', on=['zip'])
mergedDF = mergedDF.reset_index(drop = True)
mergedDF['area_norm_value'] = mergedDF['value']/mergedDF['area']
mergedDF['pop_norm_value'] = mergedDF['value']/mergedDF['population']

DF1 = pd.DataFrame(zip(mergedDF['zip'], mergedDF['area_norm_value']))
DF2 = pd.DataFrame(zip(mergedDF['zip'], mergedDF['pop_norm_value']))

DF1.columns = ['zip','value']
DF2.columns = ['zip','value']

DF1.to_csv('/var/www/104.236.40.7/public_html/dallas/crime/ZipData_Norm_Area.csv', index = False)
DF2.to_csv('/var/www/104.236.40.7/public_html/dallas/crime/ZipData_Norm_Pop.csv', index = False)


data_365 = data_all[data_all['Date of Report']>DateFilter365]
newDF = pd.DataFrame(zip(data_365['Zip Code'].value_counts().index, data_365['Zip Code'].value_counts().values))

newDF.columns = ['zip','value']

newzip = []
for each in newDF['zip']:
    newzip.append(int(str(each)[0:5]))
    
newDF['zip'] = newzip
newDF.to_csv('/var/www/104.236.40.7/public_html/dallas/crime/ZipData365.csv', index = False)

# normalization for area and population
mergedDF = DataFrame.merge(pop_area, newDF, how='inner', on=['zip'])
mergedDF = mergedDF.reset_index(drop = True)
mergedDF['area_norm_value'] = mergedDF['value']/mergedDF['area']
mergedDF['pop_norm_value'] = mergedDF['value']/mergedDF['population']

DF1 = pd.DataFrame(zip(mergedDF['zip'], mergedDF['area_norm_value']))
DF2 = pd.DataFrame(zip(mergedDF['zip'], mergedDF['pop_norm_value']))

DF1.columns = ['zip','value']
DF2.columns = ['zip','value']

DF1.to_csv('/var/www/104.236.40.7/public_html/dallas/crime/ZipData365_Norm_Area.csv', index = False)
DF2.to_csv('/var/www/104.236.40.7/public_html/dallas/crime/ZipData365_Norm_Pop.csv', index = False)






data_30 = data_all[data_all['Date of Report']>DateFilter30]
newDF = pd.DataFrame(zip(data_30['Zip Code'].value_counts().index, data_30['Zip Code'].value_counts().values))

newDF.columns = ['zip','value']

newzip = []
for each in newDF['zip']:
    newzip.append(int(str(each)[0:5]))
    
newDF['zip'] = newzip
newDF.to_csv('/var/www/104.236.40.7/public_html/dallas/crime/ZipData30.csv', index = False)

# normalization for area and population
mergedDF = DataFrame.merge(pop_area, newDF, how='inner', on=['zip'])
mergedDF = mergedDF.reset_index(drop = True)
mergedDF['area_norm_value'] = mergedDF['value']/mergedDF['area']
mergedDF['pop_norm_value'] = mergedDF['value']/mergedDF['population']

DF1 = pd.DataFrame(zip(mergedDF['zip'], mergedDF['area_norm_value']))
DF2 = pd.DataFrame(zip(mergedDF['zip'], mergedDF['pop_norm_value']))

DF1.columns = ['zip','value']
DF2.columns = ['zip','value']

DF1.to_csv('/var/www/104.236.40.7/public_html/dallas/crime/ZipData30_Norm_Area.csv', index = False)
DF2.to_csv('/var/www/104.236.40.7/public_html/dallas/crime/ZipData30_Norm_Pop.csv', index = False)





# updating dates and update time
date_min_list = []
date_max_list = []
updatetime_list = []
date30_min_list = []
date30_max_list = []
date365_min_list = []
date365_max_list = []

date_min_list.append(str(min(data_all['Date of Report']))[0:10])
date_max_list.append(str(max(data_all['Date of Report']))[0:10])
string_value = time.strftime('%l:%M%p ' + '(EST) ' + 'on %b %d, %Y')

date30_min_list.append(str(min(data_30['Date of Report']))[0:10])
date30_max_list.append(str(max(data_30['Date of Report']))[0:10])

date365_min_list.append(str(min(data_365['Date of Report']))[0:10])
date365_max_list.append(str(max(data_365['Date of Report']))[0:10])




updatetime_list.append(string_value)


dateDF = pd.DataFrame(zip(date_min_list,date_max_list,updatetime_list,date30_min_list,date30_max_list,date365_min_list,date365_max_list))
dateDF.columns = ['mindate','maxdate','updatetime','mindate30', 'maxdate30', 'mindate365', 'maxdate365']
dateDF.to_csv('/var/www/104.236.40.7/public_html/dallas/crime/displaydates_dallas.csv', index = False)





print 'done'

