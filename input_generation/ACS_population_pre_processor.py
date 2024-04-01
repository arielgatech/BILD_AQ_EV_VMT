#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 23 14:40:59 2022

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv

path_to_prj = '/Volumes/GoogleDrive/My Drive/GEMS/BILD-AQ/data'
os.chdir(path_to_prj)

ACS_household_by_income = read_csv('ACS2017/ACS2017_income.csv')
ACS_household_by_income.loc[:, 'GEOID'] = ACS_household_by_income.loc[:, 'id'].str.split('US').str[-1]


low_inc_attr = ['Estimate!!Total!!Less than $10,000', 'Estimate!!Total!!$10,000 to $14,999',
                'Estimate!!Total!!$15,000 to $19,999', 'Estimate!!Total!!$20,000 to $24,999',
                'Estimate!!Total!!$25,000 to $29,999', 'Estimate!!Total!!$30,000 to $34,999',
                'Estimate!!Total!!$35,000 to $39,999', 'Estimate!!Total!!$40,000 to $44,999',
                'Estimate!!Total!!$45,000 to $49,999']

med_inc_attr = ['Estimate!!Total!!$50,000 to $59,999', 'Estimate!!Total!!$60,000 to $74,999',
                'Estimate!!Total!!$75,000 to $99,999', 'Estimate!!Total!!$100,000 to $124,999']

high_inc_attr = ['Estimate!!Total!!$125,000 to $149,999', 'Estimate!!Total!!$150,000 to $199,999',
                 'Estimate!!Total!!$200,000 or more']
ACS_household_by_income.loc[:, 'hh_low_income'] = ACS_household_by_income.loc[:, low_inc_attr].sum(axis = 1)
ACS_household_by_income.loc[:, 'hh_med_income'] = ACS_household_by_income.loc[:, med_inc_attr].sum(axis = 1)
ACS_household_by_income.loc[:, 'hh_high_income'] = ACS_household_by_income.loc[:, high_inc_attr].sum(axis = 1)

ACS_household_by_income_out = ACS_household_by_income.loc[:, ['GEOID', 'Geographic Area Name', 'hh_low_income',
                                                              'hh_med_income', 'hh_high_income']]

# ACS_household_by_income_out.loc[:, 'sum_hh'] = ACS_household_by_income.loc[:, ['hh_low_income',
#                                                               'hh_med_income', 'hh_high_income']].sum(axis = 1)

print(ACS_household_by_income_out.head(5))
ACS_household_by_income_out.to_csv('Input/ACS_household_by_tracts.csv', index = False)