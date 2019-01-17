# Jiong Chen, 2017-03
# Eric Xu, 2017-04, 2017-05, 2017-06, 2017-07, 2017-09, 2018-01, 2018-03, 2018-12
import sys
sys.path.insert(0, '/home/exu/analytics-framework/')
# daily crontab (5-10 min):
#    0 13 * * * /home/exu/analytics-framework/scripts/MapReduce/Analytics/env/bin/python /home/exu/analytics-framework/python/DCT/Daily-RTM-Campaign/Daily_MSO_DCT.py > /home/exu/output/cronrun 2>&1

################## IMPORTS ###################################
# importing python packages and passwords, etc.
import pandas as pd
import datetime
import pymysql
import numpy as np
import time
import matplotlib
matplotlib.use('Agg')
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib import pyplot as plt
plt.style.use('ggplot')
from local_settings import *

################ FUNCTIONS ###################################
# function to initiate mysql connection 
def getSQLConn(host, port, user, password, database):
	#print host, port, user, password, database 
	return pymysql.connect(host=host,
                        user=user,
                        password=password,
                        db=database,
                        port=port,
                        local_infile=True)

# function to execute mysql query
def executeSQLQuery(conn, query):
    cur = conn.cursor()
    #Execute query
    print query
    cur.execute(query)

    return cur.fetchall()

# function to remove mysql data based on criteria
def sqlDelete (curr, deleteQuery):
    rc = -1
    try:
        rc = curr.execute(deleteQuery)
        print "%s Deleted %d rows" % (deleteQuery, rc)
    except :
        print "Error %s < %s >" % (sys.exc_info()[0], deleteQuery)
    return rc

# function to remove and load mysql data 
def sqlLoadFlatFileClean(curr, table_name, f_name, deleteQuery):
   # Delete tblFact data for dateStart and dateEnd just in case 
   sqlDelete(curr, deleteQuery)   
   # Load data transformed from DFP into stage table 
   statement="LOAD DATA LOCAL INFILE " + "\'"  + f_name + "\'" + ' INTO TABLE '+ table_name + \
           " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n';"
   print statement
   try:
       curr.execute(statement)
       rc=curr.rowcount
       print "Loaded: %d row(s)" % rc
   except :
       print "Error %s" % (sys.exc_info()[0])
       return 1
   return 0

# function to retrieve rtm mso campaign data via mysql: 
#   idorder, idAli, idConversionSegmentGroup, idAdnetConversionSegments, sSegmentPartner, sValue, 
#   sKey, idsegmentpartner
def get_mso_ali(conn):
  query = '''
        select abc.*, d.idSegmentPartner
        from
            (select  
                  a.idorder, a.idAdvertiserLineItem, b.idConversionSegmentGroup, 
                  c.idAdnetSegment, c.sSegmentPartner, c.sValue, c.sKey, 
                  case when c.sKey regexp 'id_' then 'product_id' when c.sKey regexp 'brand_' then 'brand' 
                  else 'title' end label
                  from
                      (select idorder, idAdvertiserLineItem
                      from WAREHOUSE_HADOOP.tblDimMostLineItem
                      where (sALIStatus = 'ACTIVE' or datediff(curdate(), dateALIEnd) < 45) 
                      group by idorder, idAdvertiserLineItem) a
            join (select idAdvertiserLineItem, idConversionSegmentGroup
                  from WAREHOUSE_HADOOP.tblDimAliAdnetSegment) b
            on a.idAdvertiserLineItem = b.idAdvertiserLineItem
            join (select idAdnetSegmentGroup, idAdnetSegment, sSegmentPartner,sValue, sKey  
                  from MOST_LIVE.tblAdnetSegmentGroupRelation 
                  where abs(idAdnetSegmentGroup) > 0
                        and ssegmenttype = 'CUSTOM'
                        and sValue is not null
                        and (sKey regexp 'id_' or sKey regexp 'brand_' or sKey regexp 'title_')) c
            on abs(b.idConversionSegmentGroup) = abs(c.idAdnetSegmentGroup)) abc
        join (select idSegmentPartner, sSegmentPartner
              from WAREHOUSE_HADOOP.tblDimSegmentPartner) d
        on abc.sSegmentPartner = d.sSegmentPartner
  '''
  print query
  result = executeSQLQuery(conn, query)
  df = pd.DataFrame(list(result), columns = ['idorder', 'idAli', 'idConversionSegmentGroup', 
					     'idAdnetConversionSegments', 'sSegmentPartner', 
					     'sValue', 'sKey', 'label', 'idsegmentpartner'])
  df['idAli'] = df['idAli'].astype(str)
  df['idsegmentpartner'] = df['idsegmentpartner'].astype(str)
  df['sValue'] = df['sValue'].str.lower()
  return df

# function to retrieve cube9 data via mysql for the rtm mso alis:
#   dateFact, idAli, Spend, OIQImpression
def getcube09(conn, Ali_list, start_date, end_date):
  query = '''
    select a.dateFact, cast(a.idAdvertiserLineItem as char) idAdvertiserLineItem,
           a.revenue, a.imp
    from  
      (select dateFact, idAdvertiserLineItem, sum(fTotalRevenue) revenue, sum(iSpendImps) imp 
       from tblFactNetworkAdCLISummaryByDateAdSizeExchange 
       where idAdvertiserLineItem in (%s)
             and dateFact between '%s' and '%s'
       group by idAdvertiserLineItem, dateFact ) a
    join (select idAdvertiserLineItem
          from tblDimMostLineItem
          where idAdvertiserLineItem in (%s)
          group by idAdvertiserLineItem
        ) b
    on a.idAdvertiserLineItem = b.idAdvertiserLineItem
  ''' % ("'{}'".format("','".join(Ali_list)), start_date, end_date, "'{}'".format("','".join(Ali_list)))
  print query
  result = executeSQLQuery(conn, query)

  df = pd.DataFrame(list(result), columns = ['dateFact', 'idAli', 'Spend', 'OIQImpression'])
  df['dateFact'] = df['dateFact'].astype(str)
  df['OIQImpression'] = df['OIQImpression'].astype(int)
  df['idAli'] =df['idAli'].astype(str)
  
  return(df)

# function to retrieve daily dct attributed data via mysql for the rtm mso alis:
#   dateFact, idAli, idsegmentpartner, attributed_orders, attributed_units, attributed_revenue
def get_mso_attributed(conn, Ali_list, start_date, end_date, sProductID_list, sTitle_list, sBrand_list):

  new_query = '''
    select d.dateFact, d.idSegmentPartner, d.order_id, d.product_id, d.product_id sValue, max(cast(d.quantity as decimal(14,0))), 
    max(cast(d.price as decimal(14,2))), d.label, d.idadvertiserlineitem
    from
        (select *, 'product_id' label from WAREHOUSE_HADOOP.tblFactAttributedDCTTransactionItem
          where dateFact between '%s' and '%s'
              and order_id is not null
              and order_id not in ('value','undefined', 'Not Available')
              and cast(quantity as decimal) > 0
              and quantity not regexp 'NaN'
              and price not regexp 'NaN'
              and cast(price as decimal) > 0
              and lower(product_id) in (%s) 
	      and idadvertiserlineitem in (%s)) d
    group by 
    d.dateFact, 
    d.idSegmentPartner, 
    d.order_id, 
    d.product_id, 
    d.idadvertiserlineitem
    
    union all
    
    select d1.dateFact, d1.idSegmentPartner, d1.order_id, d1.product_id, lower(d1.title) sValue, 
    max(cast(d1.quantity as decimal(14,0))), max(cast(d1.price as decimal(14,2))), d1.label, d1.idadvertiserlineitem
    from
        (select *, 'title' label from WAREHOUSE_HADOOP.tblFactAttributedDCTTransactionItem
          where dateFact between '%s' and '%s'
              and order_id is not null
              and order_id not in ('value','undefined', 'Not Available')
              and cast(quantity as decimal) > 0
              and quantity not regexp 'NaN'
              and price not regexp 'NaN'
              and cast(price as decimal) > 0
              and lower(title) in (%s)
	      and idadvertiserlineitem in (%s)) d1
    group by d1.dateFact, d1.idSegmentPartner, d1.order_id, d1.product_id, lower(d1.title), d1.idadvertiserlineitem
    
    union all
    
        select d2.dateFact, d2.idSegmentPartner, d2.order_id, d2.product_id, d2.brand sValue, max(cast(d2.quantity as decimal(14,0))), 
	max(cast(d2.price as decimal(14,2))), d2.label, d2.idadvertiserlineitem
    from
        (select *, 'brand' label from WAREHOUSE_HADOOP.tblFactAttributedDCTTransactionItem
          where dateFact between '%s' and '%s'
              and order_id is not null
              and order_id not in ('value','undefined', 'Not Available')
              and cast(quantity as decimal) > 0
              and quantity not regexp 'NaN'
              and price not regexp 'NaN'
              and cast(price as decimal) > 0
              and lower(brand) in (%s) 
	      and idadvertiserlineitem in (%s)) d2
    group by d2.dateFact, d2.idSegmentPartner, d2.order_id, d2.product_id, d2.brand, d2.idadvertiserlineitem
  ''' % (start_date, end_date, "'{}'".format("','".join(sProductID_list)), "'{}'".format("','".join(Ali_list)),
    start_date, end_date, '''"{}"'''.format('''","'''.join(sTitle_list)), "'{}'".format("','".join(Ali_list)),
    start_date, end_date, '''"{}"'''.format('''","'''.join(sBrand_list)), "'{}'".format("','".join(Ali_list)))


  print new_query

  result = executeSQLQuery(conn, new_query)
	
  df = pd.DataFrame(list(result), columns = ['dateFact', 'idsegmentpartner', 'order_id', 'product_id', 'sValue', 'quantity', 'price', 'label', 'idAli'])
  df['dateFact'] = df['dateFact'].astype(str)
  df['sValue'] = df['sValue'].astype(str)
  df['idsegmentpartner'] = df['idsegmentpartner'].astype(str)
  df['sValue'] = df['sValue'].str.lower()
  df['idAli'] = df['idAli'].astype(str)
  

  return(df)

# function to retrieve item level data for all orders/units (attributed + unattributed) 
# for share of voice calculations (attributed/total) for rtm mso campaigns via mysql:
#   dateFact, idsegmentpartner, order_id, sValue, quantity, price
def get_total(conn, start_date, end_date, sProductID_list, sTitle_list, sBrand_list):
  query = '''
    select d.dateFact, d.idSegmentPartner, d.order_id, d.product_id, d.product_id sValue, max(cast(d.quantity as decimal(14,0))), 
    max(cast(d.price as decimal(14,2))), d.label
    from
        (select *, 'product_id' label from WAREHOUSE_HADOOP.tblFactDCTTransactionItem
          where dateFact between '%s' and '%s'
              and order_id is not null
              and order_id not in ('value','undefined', 'Not Available')
              and cast(quantity as decimal) > 0
              and quantity not regexp 'NaN'
              and price not regexp 'NaN'
              and cast(price as decimal) > 0
              and lower(product_id) in (%s)) d
    group by d.dateFact, d.idSegmentPartner, d.order_id, d.product_id
    
    union all
    
    select d1.dateFact, d1.idSegmentPartner, d1.order_id, d1.product_id, lower(d1.title) sValue, max(cast(d1.quantity as decimal(14,0))), max(cast(d1.price as decimal(14,2))), d1.label
    from
        (select *, 'title' label from WAREHOUSE_HADOOP.tblFactDCTTransactionItem
          where dateFact between '%s' and '%s'
              and order_id is not null
              and order_id not in ('value','undefined', 'Not Available')
              and cast(quantity as decimal) > 0
              and quantity not regexp 'NaN'
              and price not regexp 'NaN'
              and cast(price as decimal) > 0
              and lower(title) in (%s)) d1
    group by d1.dateFact, d1.idSegmentPartner, d1.order_id, d1.product_id, lower(d1.title)
    
    union all
    
    select d2.dateFact, d2.idSegmentPartner, d2.order_id, d2.product_id, d2.brand sValue, max(cast(d2.quantity as decimal(14,0))), max(cast(d2.price as decimal(14,2))), d2.label
    from
        (select *, 'brand' label from WAREHOUSE_HADOOP.tblFactDCTTransactionItem
          where dateFact between '%s' and '%s'
              and order_id is not null
              and order_id not in ('value','undefined', 'Not Available')
              and cast(quantity as decimal) > 0
              and quantity not regexp 'NaN'
              and price not regexp 'NaN'
              and cast(price as decimal) > 0
              and lower(brand) in (%s)) d2
    group by d2.dateFact, d2.idSegmentPartner, d2.order_id, d2.product_id, d2.brand
  ''' % (start_date, end_date, "'{}'".format("','".join(sProductID_list)), 
    start_date, end_date, '''"{}"'''.format('''","'''.join(sTitle_list)), 
    start_date, end_date, '''"{}"'''.format('''","'''.join(sBrand_list)))
  print query
  result = executeSQLQuery(conn, query)

  df = pd.DataFrame(list(result), columns = ['dateFact', 'idsegmentpartner', 'order_id', 'product_id', 'sValue', 'quantity', 'price', 'label'])
  df['dateFact'] = df['dateFact'].astype(str)
  df['sValue'] = df['sValue'].astype(str)
  df['idsegmentpartner'] = df['idsegmentpartner'].astype(str)
  df['sValue'] = df['sValue'].str.lower()

  return(df)

# function to retrieve cumulative distribution data for rtm mso alis (with spend>$1):
#   idAli, sov, roi
def get_cdf_data(conn, cdf_start, cdf_end):

  query = '''
    select  
	b.idorder,
	b.sov,
	b.roi
    from
	(select 
	   idOrder
	from  
	   WAREHOUSE_HADOOP.tblDimMostLineItem 
	where 
	   curdate()<date_add(dateOrderEnd, interval 30 day)
	group by 
	   idOrder) a 
	join
	(select 
	   idorder, 
	   sum(attributed_orders)/sum(total_orders) sov, 
	   (sum(attributed_revenue) - sum(spend))/sum(spend) roi
	from 
	   test.tblFactRetailShopperTransactionMSO
	where 
	   dateFact between '%s' and '%s'
	group by 
	    idorder
	having 
	    sov is not null and 
	    roi is not null and 
	    sum(spend)>1) b 
	on 
	    a.idOrder=b.idorder
  ''' % (cdf_start, cdf_end)

  result = executeSQLQuery(conn, query)
  
  df = pd.DataFrame(list(result), columns = ['idorder', 'sov', 'roi'])

  df['sov'] = df['sov'].astype(float)
  df['roi'] = df['roi'].astype(float)

  return(df)

def get_summary_data(conn):

  query = '''
	select 
		dateFact, 
    		mean_sov, 
    		median_sov,
		mean_roi,
		median_roi
	from 
		test.tblFactRetailShopperTransactionMSO_Summary
	where
		dateFact>=date_sub(CURRENT_DATE, interval 30 day)
	group by 
		dateFact,
    		mean_sov,
    		median_sov,
		mean_roi,
		median_roi
	order by 
		dateFact asc'''

  result = executeSQLQuery(conn, query)
  
  df = pd.DataFrame(list(result), columns = ['dateFact', 'mean_sov', 'median_sov', 'mean_roi', 'median_roi'])

  df['mean_sov'] = df['mean_sov'].astype(float)
  df['median_sov'] = df['median_sov'].astype(float)
  df['mean_roi'] = df['mean_roi'].astype(float)
  df['median_roi'] = df['median_roi'].astype(float)

  return(df)
################ END OF FUNCTIONS ############################


# username & passwords for mysql
user = DB_AUTH['db_user']
password = DB_AUTH['db_password']
# ib_conn = getSQLConn(DB_AUTH['db_host'], DB_AUTH['db_ib_port'], user, password, 'WAREHOUSE_IB')
wh_conn = getSQLConn(DB_AUTH['db_host'], DB_AUTH['db_mysql_port'], user, password, 'WAREHOUSE_HADOOP')
adnet_live_conn = getSQLConn(DB_AUTH['db_host'], DB_AUTH['db_mysql_port'], user, password, 'ADNET_LIVE')
most_live_conn = getSQLConn(DB_AUTH['db_host'], DB_AUTH['db_mysql_port'], user, password, 'MOST_LIVE')
test_conn = getSQLConn(DB_AUTH['db_host'], DB_AUTH['db_mysql_port'], user, password, 'test')
curr = test_conn.cursor()

# get date range for data loading and cumulative distributions
start_date = (datetime.date.today() - datetime.timedelta(7)).strftime("%Y-%m-%d")
end_date = (datetime.date.today() - datetime.timedelta(1)).strftime("%Y-%m-%d")
cdf_start = (datetime.date.today() - datetime.timedelta(30)).strftime("%Y-%m-%d")
cdf_end = end_date

# execute queries and aggregate data
mso_ali = get_mso_ali(wh_conn)
Ali_list = list(mso_ali['idAli'].unique())
cube09 = getcube09(wh_conn, Ali_list, start_date, end_date)
cube09 = pd.merge(cube09, mso_ali[['idsegmentpartner', 'idAli']].drop_duplicates(), on = ['idAli'])

# make sure all sValues are lower case and %20 turned into spaces
product_id_sValue_list = list(mso_ali[mso_ali['sKey'].str.contains('id_')]['sValue'].unique())
title_sValue_list = list(mso_ali[mso_ali['sKey'].str.contains('title_')]['sValue'].unique())
brand_sValue_list = list(mso_ali[mso_ali['sKey'].str.contains('brand_')]['sValue'].unique())
product_id_sValue_list_new = []
title_sValue_list_new = []
brand_sValue_list_new = []

for each in product_id_sValue_list:
	product_id_sValue_list_new.append(each.lower())
	if each.find('%20') != -1:
		product_id_sValue_list_new.append(each.lower().replace('%20',' '))
		
for each in title_sValue_list:
	title_sValue_list_new.append(each.lower())
	if each.find('%20') != -1:
		title_sValue_list_new.append(each.lower().replace('%20',' '))
		
for each in brand_sValue_list:
	brand_sValue_list_new.append(each.lower())
	if each.find('%20') != -1:
		brand_sValue_list_new.append(each.lower().replace('%20',' '))
		
if product_id_sValue_list_new == []:
	product_id_sValue_list_new.append('improbable_filler_string')

if title_sValue_list_new == []:
	title_sValue_list_new.append('improbable_filler_string')

if brand_sValue_list_new == []:
	brand_sValue_list_new.append('improbable_filler_string')
#################################################################
total = get_total(wh_conn, start_date, end_date, product_id_sValue_list_new, title_sValue_list_new, brand_sValue_list_new)
print total.head()
df_total = pd.merge(mso_ali[['idAli', 'sValue', 'idsegmentpartner', 'label']], total, on = ['sValue', 'idsegmentpartner', 'label'])
print df_total.head()
df_total = df_total.drop_duplicates()
df_total = df_total.reset_index(drop = True)
df_total = df_total.drop_duplicates(['idAli', 'idsegmentpartner', 'order_id', 'product_id'])
df_total = df_total.reset_index(drop = True)
df_total['quantity'] = df_total['quantity'].astype(int)
df_total['price'] = df_total['price'].astype(float)
df_total['quantity_price_product'] = df_total['quantity']*df_total['price']
print df_total.head()
df_total_grouped = df_total.groupby(['idAli', 'dateFact']).agg({'quantity_price_product': np.sum, 'quantity': np.sum, 
								'order_id': lambda x: len(set(x))})
print df_total_grouped.head()
df_total_grouped['dateFact'] = df_total_grouped.index.get_level_values('dateFact')
df_total_grouped['idAli'] = df_total_grouped.index.get_level_values('idAli')
df_total_grouped = df_total_grouped.reset_index(drop = True)

attributed = get_mso_attributed(wh_conn, Ali_list, start_date, end_date, product_id_sValue_list_new, title_sValue_list_new, brand_sValue_list_new)

# attributed before: ['dateFact', 'idAli', 'idsegmentpartner', 'attributed_orders', 'attributed_units', 'attributed_revenue']
# attributed after: ['dateFact', 'idsegmentpartner', 'order_id', 'product_id', 'sValue', 'quantity', 'price', 'label', 'idAli']

df_attributed = pd.merge(mso_ali[['idAli', 'sValue', 'idsegmentpartner', 'label']], attributed, on = ['sValue', 'idsegmentpartner', 'label', 'idAli'])
df_attributed = df_attributed.drop_duplicates()
df_attributed = df_attributed.reset_index(drop = True)
df_attributed = df_attributed.drop_duplicates(['idAli', 'idsegmentpartner', 'order_id', 'product_id'])
df_attributed = df_attributed.reset_index(drop = True)
df_attributed['attributed_units'] = df_attributed['quantity'].astype(int)
df_attributed['price'] = df_attributed['price'].astype(float)
df_attributed['attributed_revenue'] = df_attributed['attributed_units']*df_attributed['price']
df_attributed_grouped = df_attributed.groupby(['idAli', 'dateFact', 'idsegmentpartner']).agg({'attributed_revenue': np.sum, 
									  'attributed_units': np.sum, 
									  'order_id': lambda x: len(set(x))})
df_attributed_grouped['dateFact'] = df_attributed_grouped.index.get_level_values('dateFact')
df_attributed_grouped['idAli'] = df_attributed_grouped.index.get_level_values('idAli')
df_attributed_grouped['idsegmentpartner'] = df_attributed_grouped.index.get_level_values('idsegmentpartner')
df_attributed_grouped = df_attributed_grouped.reset_index(drop = True)
df_attributed_grouped = df_attributed_grouped.rename(columns={'order_id':'attributed_orders'})
attributed = df_attributed_grouped[['dateFact', 'idAli', 'idsegmentpartner', 'attributed_orders', 
				    'attributed_units', 'attributed_revenue']]

attributed.to_csv('/tmp/attributed.csv', index = False)
total.to_csv('/tmp/total.csv', index = False)

# combine cube9 data with dct data and prepare csv for data loading
df1 = pd.merge(attributed, df_total_grouped, on = ['dateFact', 'idAli'], how = 'outer')
df1 = df1.fillna(0)
df2 = pd.merge(df1[['dateFact', 'idAli', 'attributed_orders', 'attributed_units', 'attributed_revenue', 'order_id', 'quantity_price_product', 'quantity']], cube09, on = ['dateFact', 'idAli'], how = 'outer')
final = pd.merge(df2, mso_ali[['idorder', 'idAli', 'idsegmentpartner']].drop_duplicates(), on = ['idAli', 'idsegmentpartner'])
final = final.rename(columns={'quantity_price_product':'total_revenue'})
final = final.rename(columns={'quantity':'total_units'})
final = final.rename(columns={'order_id':'total_number_of_orders'})
final['SOV'] = final['attributed_orders']/final['total_number_of_orders']
final['ROI'] = (final['attributed_revenue'] - final['Spend'])/final['Spend']
final['ROAS'] = final['attributed_revenue']/final['Spend']
final = final.replace([np.inf, -np.inf], 0)
final = final.fillna(0)
final['attributed_units'] = final['attributed_units'].astype(int)
final['total_units'] = final['total_units'].astype(int)
final['attributed_orders'] = final['attributed_orders'].astype(int)
final['total_number_of_orders'] = final['total_number_of_orders'].astype(int)
columns = ['idorder', 'idAli', 'dateFact', 'OIQImpression', 'Spend', 'attributed_units', 
'total_units', 'attributed_revenue', 'total_revenue', 'attributed_orders', 'total_number_of_orders', 'SOV', 'ROI', 'ROAS']
final = final[columns]
final = final.sort_values(by = ['idorder', 'idAli', 'dateFact'])
final = final.fillna(0)
file_name = '/tmp/' + 'Daily_MSO_Campaign_Reporting_' + str(start_date) + '_' + str(end_date) + '_' + str(time.time()).split('.')[0] + '.csv'
final.to_csv(file_name, encoding = 'utf-8', header = False, index = False)
final.to_csv('/tmp/finalfinal.csv', index = False)

# loading csv data to test.tblFactRetailShopperTransactionMSO
deleteQuery = "delete from tblFactRetailShopperTransactionMSO where dateFact between '%s' and '%s'" % (start_date, end_date)
rc = sqlLoadFlatFileClean(curr,'tblFactRetailShopperTransactionMSO' , file_name, deleteQuery)
if rc <0:
  print "ERROR: Load infile %s failed" %file_name

# graph the cumulative distributions for roi and sov with a 30 day lookback as a pdf to be emailed
cdf_data = get_cdf_data(test_conn, cdf_start, cdf_end)
sov_series = cdf_data.sov
roi_series = cdf_data.roi
summary_data = get_summary_data(test_conn)
Report = '/tmp/Daily_MSO_Campaign_Reporting_' + str(cdf_start) + '_' + str(cdf_end) + '_' + str(time.time()).split('.')[0] + '_' + 'cdf.pdf'
with PdfPages(Report) as pdf:
	# Cumulative Distribution of Share of Voice of Transactions by Campaign idOrder Across Last 30 days
	fig = plt.figure(figsize=(15, 22))
	ax1 = fig.add_subplot(411)
	sov_series.hist(cumulative=True, normed=1, bins=3000, histtype='step', linewidth=1.5)
	plt.title('Latest 30-Day Cumulative Distribution of Share of Voice of Transactions by Campaign (idOrder)', fontsize = 12)
	ax1.get_xaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: str(x*100) + '%'))
	ax1.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: str(x*100) + '%'))
	plt.ylabel('Percentile of Total Number of Campaigns (idOrder)')
	plt.xlabel('Share of Voice of Transactions (%)', fontsize=10)
	plt.yticks(np.arange(0,1+0.05,0.1))
	
	# Cumulative Distribution of ROI by Campaign idOrder Across Last 30 days
	ax2 = fig.add_subplot(412)
	roi_series.hist(cumulative=True, normed=1, bins=3000, histtype='step', linewidth=1.5)
	plt.title('Latest 30-Day Cumulative Distribution of ROI (%) by Campaign (idOrder)', fontsize = 12)
	ax2.get_xaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: str(x*100) + '%'))
	ax2.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: str(x*100) + '%'))
	plt.ylabel('Percentile of Total Number of Campaigns (idOrder)')
	plt.xlabel('ROI (%)', fontsize=10)
	plt.yticks(np.arange(0,1+0.05,0.1))
	
	# Mean/Median 30-Day Share of Voice of Transactions Across All Campaigns (idOrders) by Date Across Last Month
	ax3 = fig.add_subplot(413)
	plt.plot(summary_data.dateFact, summary_data.mean_sov, 'r-', label = 'mean')
	plt.plot(summary_data.dateFact, summary_data.median_sov, 'b-', label = 'median')
	plt.legend()
	plt.title('30-Day Share of Voice of Transactions Across Campaigns (idOrders) by Analysis End Date across Last Month', fontsize = 12)
	plt.ylabel('Share of Voice of Transactions (%)')
	plt.xlabel('Analysis End Date', fontsize=9)
	
	# Mean/Median 30-Day ROI Across All Campaigns (idOrders) by Date Across Last Month
	ax4 = fig.add_subplot(414)
	plt.plot(summary_data.dateFact, summary_data.mean_roi, 'r-', label = 'mean')
	plt.plot(summary_data.dateFact, summary_data.median_roi, 'b-', label = 'median')
	plt.legend()
	plt.title('30-Day Return on Investment (ROI) Across Campaigns (idOrders) by Analysis End Date across Last Month', fontsize = 12)
	ax4.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: str(x*100) + '%'))
	plt.ylabel('ROI (%)')
	plt.xlabel('Analysis End Date', fontsize=9)
	
	
	
	pdf.savefig()
	plt.close()
	
# prepare csv data to load mean/median roi and sov for rtm mso alis with a 30 day lookback
# into test.tblFactRetailShopperTransactionMSO_Summary mysql table
median_sov = ''.join([str(round(100*(np.median(sov_series)),1)),' %'])
median_roi = ''.join([str(round((np.median(roi_series)),1))])
median_sov_list = [round(100*(np.median(sov_series)),1)]
median_roi_list = [round((np.median(roi_series)),1)]
mean_sov_list = [round(100*(np.mean(sov_series)),1)]
mean_roi_list = [round((np.mean(roi_series)),1)]
dateFact_list = [(datetime.date.today() - datetime.timedelta(1)).strftime("%Y-%m-%d")]
startDate_list = [(datetime.date.today() - datetime.timedelta(31)).strftime("%Y-%m-%d")]
final_Summary = pd.DataFrame(zip(dateFact_list, startDate_list, mean_sov_list, mean_roi_list, median_sov_list, median_roi_list))
final_Summary.columns = ['a','b','c','d','e','f']
final_Summary = final_Summary.fillna(0)
file_name_Summary = '/tmp/' + 'Daily_MSO_Campaign_Reporting_Summary_' + str(start_date) + '_' + str(end_date) + '_' + str(time.time()).split('.')[0] + '.csv'
final_Summary.to_csv(file_name_Summary, encoding = 'utf-8', header = False, index = False)
deleteQuery_Summary = "delete from tblFactRetailShopperTransactionMSO_Summary where dateFact='%s' and startDate='%s'" % (end_date, cdf_start)
rc = sqlLoadFlatFileClean(curr,'tblFactRetailShopperTransactionMSO_Summary' , file_name_Summary, deleteQuery_Summary)
if rc <0:
  print "ERROR: Load infile %s failed" %file_name

# emailing recipients with median sov/roi of rtm mso alis as well as cumulative distribution pdf 
Subject = 'RTM.MSO.Campaign.Health.Report--' + str(cdf_start) + '.to.' + str(cdf_end) + '.INTERNAL.ONLY.'
os.system('''echo "See attached for the sov and roi stats for RTM campaigns with MSO segment group attribution.  Please note that the data underlying this report is for INTERNAL USE ONLY.  Please go through Client Analytics for any external reporting. 
 
                            share of voice *sov* {range 0 to 100%} = (attributed orders) / (total orders)
                            return on investment *roi* {range: -100% to inf%} = 100*((revenue - spend) / (spend))
 	                   -----------------------------------------------------------------------------------------
 	                   median sov across RTM MSO campaigns = ''' + str(median_sov) + '''
 	         	   median roi across RTM MSO campaigns = ''' + str(int(float(float(100)*float(median_roi)))) + '%' + '''
 		           -----------------------------------------------------------------------------------------
 			   
 			   https://github.com/owneriq-dev/analytics-framework/blob/master/python/DCT/Daily-RTM-Campaign/Daily_MSO_DCT.py
 " | mutt -s %s -b wh-report-admin@owneriq.com -b gloftus@owneriq.com -b clientanalytics@owneriq.com -b agrudic@owneriq.com -b mdoucette@owneriq.com -b teamdelivery@owneriq.com -b exu@owneriq.com -a %s;''' % (Subject, Report))
