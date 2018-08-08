# -*- coding: utf-8 -*-
"""
Created on Wed May  9 15:12:14 2018

@author: MichaelEK
"""
import pytest
import pandas as pd
import os
from pyhydrotel import get_sites_mtypes, get_ts_data

###############################
### Parameters

_module_path = os.path.dirname(__file__)
sites_mtypes_ex1_csv = 'sites_mtypes_ex1.csv'
ts_results_csv = 'ts_results_ex1.csv'

server = 'SQL2012PROD04'
database = 'Hydrotel'

mtypes = ['flow', 'water level', 'rainfall']
sites = ['69607', '70105', 'L37/0024']

sites_mtypes_ex1 = os.path.join(_module_path, sites_mtypes_ex1_csv)
ts_results1 = os.path.join(_module_path, ts_results_csv)

###############################
### Tests

sites_mtypes1 = pd.read_csv(sites_mtypes_ex1)
sites_mtypes1.ExtSysID = sites_mtypes1.ExtSysID.astype(str)

ts_results = pd.read_csv(ts_results1)
ts_results.DateTime = pd.to_datetime(ts_results.DateTime)
ts_results.ExtSysID = ts_results.ExtSysID.astype(str)


def test_get_sites_mtypes():
    sites_mtypes = get_sites_mtypes(server, database, mtypes, sites)

    assert all(sites_mtypes == sites_mtypes1)


def test_get_ts_data():
    tsdata = get_ts_data(server, database, mtypes, sites).reset_index()

    assert all(tsdata == ts_results)

