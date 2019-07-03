from pyhydrotel import get_sites_mtypes, get_ts_data, get_mtypes

server = 'sql2012prod04'
database = 'hydrotel'


workdir = r'C:\Active\Projects\Rakaia\Data\Lake_Coleridge_Trustpower\IMS\Example_SWR_CSVs'

# #-Coleridge site id
# id = ['6852602']
# mtypes = ['non-hydro release lc', 'Rakaia FH modified']
# mtypes = ['Rakaia FH modified']
# #tsdata = get_ts_data(server, database, mtypes, id, resample_code='T', period=15)#, from_date='2018-01-01', to_date='2018-01-10')
# tsdata = get_ts_data(server, database, mtypes, id, resample_code='D')
# print tsdata
# exit(0)


#-Fighting Hill site id
sites = ['168526']
mtypes = ['Flow']#, 'Wilcos Flow']
tsdata = get_ts_data(server, database, mtypes, sites, resample_code='T', period=15)
print(tsdata)





