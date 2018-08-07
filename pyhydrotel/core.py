# -*- coding: utf-8 -*-
"""
Created on Tue Jan 02 10:05:15 2018

@author: MichaelEK
Functions to read hydrotel data.
"""
import numpy as np
import pandas as pd
from pdsql.mssql import rd_sql, rd_sql_ts

######################################
### Parameters
## mtypes dict
ob_dict = {'flow': 'Flow',
               'gwl': 'Water Level',
               'precip': 'Rainfall',
               'swl': 'Water Level'}
mtypes_list = ['Barometric Pressure', 'Conductivity', 'Flow Rate', 'Groundwater level', 'Rainfall Depth', 'Solar Radiation', 'Temperature', 'Turbidity', 'Water Level', 'Water Temperature', 'Wind Speed', 'Air Temperature']
resample_dict = {'flow': 'mean',
               'gwl': 'mean',
               'precip': 'sum',
               'swl': 'mean',
               'sw_temp': 'mean'}

## Database parameters
data_tab = 'Samples'
points_tab = 'Points'
objects_tab = 'Objects'
mtypes_tab = 'ObjectVariants'
sites_tab = 'Sites'

data_col = ['Point', 'DT', 'SampleValue']
points_col = ['Point', 'Object']
objects_col = ['Object', 'Site', 'ObjectVariant', 'Name']
mtypes_col = ['ObjectVariant', 'Name']
sites_col = ['Site', 'Name', 'ExtSysId']


def get_sites_mtypes(server, database, mtypes=None, sites=None):
    """
    Function to determine the available sites and associates measurement types in the Hydrotel database.

    Parameters
    ----------
    server : str
        The server where the Hydrotel database lays.
    database : str
        The name of the Hydrotel database.
    mtypes : str, list of str, or None
        The measurement type(s) of the sites that should be returned. None returns all mtypes.
    sites : list of str or None
        The list of sites that should be returned. None returns all sites.

    Returns
    -------
    DataFrame
        ExtSysID, MType, Site, Object, ObjectVariant
    """

    if mtypes is None:
        mtypes = list(ob_dict.keys())
    elif isinstance(mtypes, str):
        mtypes = [mtypes]
    elif not isinstance(mtypes, list):
        raise TypeError('mtypes must be either a str, a list of str, or None')

    ## Extract hydrotel site numbers for all ECan sites
    sites1 = rd_sql(server, database, sites_tab, sites_col)
    sites1['ExtSysId'] = sites1['ExtSysId'].str.strip()
    sites1.rename(columns={'ExtSysId': 'ExtSysID'}, inplace=True)
    ob1 = rd_sql(server, database, objects_tab, ['Site', 'ExtSysID']).dropna()

    # GW
    names_len_bool = sites1.Name.str.upper().str.contains('[A-Z]+\d+/\d+')
    gw_sites = sites1[names_len_bool].copy()
    gw_sites.ExtSysID = gw_sites.Name.str.findall('[A-Z]+\d+/\d+').apply(lambda x: x[0])
    gw_sites['MType'] = 'gwl'

    # Others
    sites2 = sites1[sites1.ExtSysID.str.contains('\d+', na=False)].drop('Name', axis=1)
    sites3 = ob1[ob1.ExtSysID.str.contains('\d+', na=False)]

    # Combine and remove duplicates
    sites_all = pd.concat([gw_sites.drop('Name', axis=1), sites2, sites3])
    sites_all = sites_all.drop_duplicates('ExtSysID')

    if isinstance(sites, list):
        sites_all = sites_all[sites_all.ExtSysID.isin(sites)]

    ## objects
    objects1 = rd_sql(server, database, objects_tab, objects_col, {'Site': sites_all.Site.tolist(), 'Name': list(set(ob_dict[i] for i in ob_dict if i in mtypes))})

    # Join Sites to Objects
    sites_ob1 = pd.merge(sites_all, objects1, on='Site')
    sites_ob1.Name = sites_ob1.Name.str.lower()
    sites_ob1.loc[sites_ob1.MType.isnull() & (sites_ob1.Name == 'water level'), 'MType'] = 'swl'
    sites_ob1.loc[sites_ob1.Name == 'flow', 'MType'] = 'flow'
    sites_ob1.loc[sites_ob1.Name == 'rainfall', 'MType'] = 'precip'

    return sites_ob1.drop('Name', axis=1)


def get_ts_data(server, database, mtypes, sites, from_date=None, to_date=None, resample_code='D', period=1, val_round=3, min_count=None, pivot=False):
    """
    Function to extract time series data from the hydrotel database.

    Parameters
    ----------
    server : str
        The server where the Hydrotel database lays.
    database : str
        The name of the Hydrotel database.
    mtypes : str or list of str
        The measurement type(s) of the sites that should be returned. Possible options include swl, flow, gwl, and precip.
    sites : list of str
        The list of sites that should be returned.
    from_date : str or None
        The start date in the format '2000-01-01'.
    to_date : str or None
        The end date in the format '2000-01-01'.
    resample_code : str
        The Pandas time series resampling code. e.g. 'D' for day, 'W' for week, 'M' for month, etc.
    period : int
        The number of resampling periods. e.g. period = 2 and resample = 'D' would be to resample the values over a 2 day period.
    val_round : int
        The number of decimals to round the values.
    pivot : bool
        Should the output be pivotted into wide format?

    Returns
    -------
    Series or DataFrame
        A MultiIndex Pandas Series if pivot is False and a DataFrame if True
    """
    ### Import data and select the correct sites
    object_val = get_sites_mtypes(server, database, mtypes, sites)

    ### Import object/point data
    point_val = rd_sql(server, database, points_tab, points_col, where_col='Object', where_val=object_val.Object.tolist())

    ### Merge
    site_point = pd.merge(object_val[['ExtSysID', 'MType', 'Object']], point_val, on='Object').drop('Object', axis=1)

    ### Pull out the ts data
    tsdata_list = []

    mtypes1 = site_point.MType.unique()

    for m in mtypes1:
        sel = site_point[site_point.MType == m]
        points = sel.Point.astype(int).tolist()

        data1 = rd_sql_ts(server, database, data_tab, 'Point', 'DT', 'SampleValue', resample_code, period, resample_dict[m], val_round, {'Point': points}, from_date=from_date, to_date=to_date, min_count=min_count).reset_index()

        data1.rename(columns={'DT': 'DateTime', 'SampleValue': 'Value'}, inplace=True)
        data2 = pd.merge(sel, data1, on='Point').drop('Point', axis=1).set_index(['ExtSysID', 'MType', 'DateTime']).Value
        tsdata_list.append(data2)

    tsdata = pd.concat(tsdata_list)

    if pivot:
        tsdata = tsdata.unstack([0, 1])

    return tsdata
