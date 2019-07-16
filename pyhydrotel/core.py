# -*- coding: utf-8 -*-
"""
Created on Tue Jan 02 10:05:15 2018

@author: MichaelEK
Functions to read hydrotel data.
"""
import pandas as pd
from pdsql.mssql import rd_sql, rd_sql_ts, to_mssql

######################################
### Parameters
## mtypes dict
#mtypes = ['flow', 'water level', 'rainfall', 'non-hydro release lc', 'Rakaia FH modified']
#mtypes_list = ['Barometric Pressure', 'Conductivity', 'Flow Rate', 'Groundwater level', 'Rainfall Depth', 'Solar Radiation', 'Temperature', 'Turbidity', 'Water Level', 'Water Temperature', 'Wind Speed', 'Air Temperature']
resample_dict = {'rainfall': 'sum'}

## Database parameters
data_tab = 'Samples'
points_tab = 'Points'
objects_tab = 'Objects'
mtypes_tab = 'ObjectVariants'
sites_tab = 'Sites'

data_col = ['Point', 'DT', 'SampleValue']
points_col = ['Point', 'Object']
objects_col = ['Object', 'Site', 'ObjectVariant', 'Name', 'ExtSysID']
mtypes_col = ['ObjectVariant', 'Name']
sites_col = ['Site', 'Name', 'ExtSysId']


def get_mtypes(server, database):
    """
    Function to return a Series of measurement types that can be passed to get_sites_mtypes and get_ts_data. Returns with a count of the frequency the values exist in the database and is sorted by the count.

    Remember, SQL is not case sensitive. The MTypes returned will have different cases, but these differences do not matter for the other functions.

    Parameters
    ----------
    server : str
        The server where the Hydrotel database lays.
    database : str
        The name of the Hydrotel database.
    mtypes : str or list of str
        The measurement type(s) of the sites that should be returned.
    sites : list of str or None
        The list of sites that should be returned. None returns all sites.

    Returns
    -------
    Series
        MType (index), count
    """
    objects1 = rd_sql(server, database, objects_tab, objects_col)
    objects2 = objects1.groupby('Name').Site.count().sort_values(ascending=False)
    objects2.name = 'count'
    objects2.index.name = 'MType'

    return objects2


def get_sites_mtypes(server, database, mtypes=None, sites=None):
    """
    Function to determine the available sites and associated measurement types in the Hydrotel database.

    Parameters
    ----------
    server : str
        The server where the Hydrotel database lays.
    database : str
        The name of the Hydrotel database.
    mtypes : str, list of str, or None
        The measurement type(s) of the sites that should be returned.
    sites : str, list of str, or None
        The list of sites that should be returned. None returns all sites.

    Returns
    -------
    DataFrame
        ExtSysID, MType, Site, Object, ObjectVariant
    """
    if isinstance(sites, str):
        sites = [sites]

    if isinstance(mtypes, str):
        mtype_dict = {'Name': [mtypes]}
    elif isinstance(mtypes, list):
        mtype_dict = {'Name': mtypes}
    elif mtypes is None:
        mtype_dict = None
    else:
        raise TypeError('mtypes must be either a str, a list of str, or None')

    ## Extract hydrotel site numbers for all ECan sites
    sites1 = rd_sql(server, database, sites_tab, sites_col)
    sites1['ExtSysId'] = sites1['ExtSysId'].str.strip()
    sites1 = sites1[sites1.ExtSysId != '']
#    sites1.rename(columns={'ExtSysId': 'ExtSysID'}, inplace=True)

    # GW
    names_len_bool = sites1.Name.str.upper().str.match('[A-Z]+\d+/\d+')
    gw_sites = sites1[names_len_bool].copy()
    gw_sites.ExtSysId = gw_sites.Name.str.findall('[A-Z]+\d+/\d+').apply(lambda x: x[0])
#    gw_sites['MType'] = 'gwl'

    # Others
    sites2 = sites1[sites1.ExtSysId.str.match('\d+', na=False)].drop('Name', axis=1)
#    sites3 = ob1[ob1.ExtSysID.str.contains('\d+', na=False)]

    # Combine and remove duplicates
    sites3 = pd.concat([gw_sites.drop('Name', axis=1), sites2])
    sites3 = sites3.drop_duplicates('ExtSysId')

    ## objects
    objects1 = rd_sql(server, database, objects_tab, objects_col, mtype_dict)
    objects1.ExtSysID = objects1.ExtSysID.str.strip()
    objects1.loc[objects1.ExtSysID == '', 'ExtSysID'] = None

    ## Combine objects with sites
    sites_ob1 = pd.merge(objects1, sites3, on='Site', how='left')
    sites_ob1.loc[sites_ob1.ExtSysID.isnull(), 'ExtSysID'] = sites_ob1.loc[sites_ob1.ExtSysID.isnull(), 'ExtSysId']
    sites_ob1 = sites_ob1.dropna(subset=['ExtSysID']).drop('ExtSysId', axis=1)

    if isinstance(sites, list):
        sites_ob1 = sites_ob1[sites_ob1.ExtSysID.isin(sites)]

    sites_ob1.Name = sites_ob1.Name.str.lower()
    sites_ob1.rename(columns={'Name': 'MType'}, inplace=True)

    ## Import object/point data
    point_val = rd_sql(server, database, points_tab, points_col, where_in={'Object': sites_ob1.Object.astype(int).tolist()})

    # Merge
    site_point = pd.merge(sites_ob1, point_val, on='Object')

    ## Get from and to dates
    sql_stmt = """select Point, min(DT) as FromDate, max(DT) as ToDate
                from Hydrotel.dbo.Samples
                where Point in ({sites})
                group by Point""".format(sites=str(site_point.Point.astype(int).tolist())[1:-1])
    min_max_point = rd_sql(server, database, stmt=sql_stmt)

    ## Combine all together
    site_summ = pd.merge(site_point, min_max_point, on='Point', how='left')
    site_summ.rename(columns={'ExtSysID': 'ExtSiteID'}, inplace=True)
    site_summ.set_index(['ExtSiteID', 'MType'], inplace=True)

    return site_summ


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
    site_point = get_sites_mtypes(server, database, mtypes, sites).reset_index()

    ### Select rows within time period
    if isinstance(from_date, str):
        site_point = site_point[site_point.ToDate > from_date]
    if isinstance(to_date, str):
        site_point = site_point[site_point.FromDate < to_date]

    if site_point.empty:
        return pd.DataFrame()

    ### Pull out the ts data
    site_point1 = site_point[['ExtSiteID', 'MType', 'Point']].copy()

    tsdata_list = []

    mtypes1 = site_point.MType.unique()

    for m in mtypes1:
        if m in resample_dict:
            res_val = resample_dict[m]
        else:
            res_val = 'mean'
        sel = site_point1[site_point1.MType == m]
        points = sel.Point.astype(int).tolist()

        data1 = rd_sql_ts(server, database, data_tab, 'Point', 'DT', 'SampleValue', resample_code, period, res_val, val_round, {'Point': points}, from_date=from_date, to_date=to_date, min_count=min_count).reset_index()

        data1.rename(columns={'DT': 'DateTime', 'SampleValue': 'Value'}, inplace=True)
        data2 = pd.merge(sel, data1, on='Point').drop('Point', axis=1).set_index(['ExtSiteID', 'MType', 'DateTime']).Value
        tsdata_list.append(data2)

    tsdata = pd.concat(tsdata_list)

    if pivot:
        tsdata = tsdata.unstack([0, 1])

    return tsdata


def create_site_mtype(server, database, site, ref_point, new_mtype):
    """
    Function to create a new mtype for a specific site. A reference point number of an existing mtype of the same site must be used for creation. Run get_sites_mtypes to find a good reference point.

    Parameters
    ----------
    server : str
        The server where the Hydrotel database lays.
    database : str
        The name of the Hydrotel database.
    site : str
        The site to create the new mtype on.
    ref_point : int
        The reference point from another mtype on the same site.
    new_mtype : str
        The new mtype name. Must be unique for the associated site.

    Returns
    -------
    DataFrame
        New object and point values extracted by the get_sites_mtypes function.
    """
    ## Checks
    site_mtypes = get_sites_mtypes(server, database, sites=site).reset_index()

    if not (site_mtypes.Point == ref_point).any():
        raise ValueError('model_point must be a Point that exists within the mtypes of the site')
    if (site_mtypes.MType == new_mtype.lower()).any():
        raise ValueError('new_name already exists as an mtype, please use a different name')

    ## Import object/point data
    point_val = rd_sql(server, database, points_tab, where_in={'Point': [ref_point]})
    obj_val = rd_sql(server, database, objects_tab, where_in={'Object': point_val.Object.tolist()})

    treeindex1 = int(rd_sql(server, database, stmt='select max(TreeIndex) from {tab} where Site = {site}'.format(tab=objects_tab, site=int(obj_val.Site))).iloc[0])

    ## Assign new object data
    obj_val2 = obj_val.drop('Object', axis=1).copy()
    obj_val2['Name'] = new_mtype
    obj_val2['TreeIndex'] = treeindex1 + 1

    to_mssql(obj_val2, server, database, objects_tab)

    ## Find out what the new object value is
    new_obj = int(rd_sql(server, database, objects_tab, where_in={'Site': obj_val.Site.tolist(), 'Name': [new_mtype]}).Object)

    ## Assign new point data
    point_val2 = point_val.drop('Point', axis=1).copy()
    point_val2['Name'] = new_mtype
    point_val2['Object'] = new_obj

    to_mssql(point_val2, server, database, points_tab)

    ## Return new values
    site_mtypes = get_sites_mtypes(server, database, sites=site, mtypes=new_mtype)

    return site_mtypes

