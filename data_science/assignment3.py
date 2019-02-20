import array
import base64
import codecs
import csv
import numpy as np
import os
import pandas as pd
import struct


def answer_one():
    energy = pd.read_excel(
            io='./documents/Energy Indicators.xls',
            sheet_name='Energy',
            skiprows=18,
            skip_footer=38,
            usecols='C,D,E,F',
            header=None,
            names=['Country', 'Energy Supply', 'Energy Supply per Capita', '% Renewable']
        ).replace(to_replace='...', value=np.nan)
    energy = energy.replace(to_replace='Republic of Korea', value='South Korea')
    energy = energy.replace(to_replace='United States of America', value='United States')
    energy = energy.replace(to_replace='United Kingdom of Great Britain and Northern Ireland', value='United Kingdom')
    energy = energy.replace(to_replace='China, Hong Kong Special Administrative Region', value='Hong Kong')
    energy['Energy Supply'] = energy.loc[:,'Energy Supply'] * 1000000
    energy['Country'] = energy['Country'].str.replace('\d+', '')
    energy['Country'] = energy['Country'].str.replace('\(([^()]*)\)', '')
    
