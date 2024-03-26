# %% documentation
"""
Created on Wed Mar 22 18:40:27 2023

@author: pschicchi
"""

# %% imports

import os
import numpy as np
import pandas as pd
import locale
locale.setlocale(locale.LC_ALL,'es_AR.UTF-8')

# %% globals

# adjust directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# DATA_DIR = os.path.join(BASE_DIR, '..', '..', 'data', 'raw')
OUTPUT_DIR = os.path.join(BASE_DIR, '..', 'data', 'processed')

filename = os.path.join(OUTPUT_DIR, 'ar_crush_monthly_magyp.csv')

# %% functions

def read_raw_data(url):
    # get dataframes in page
    df_list = pd.read_html(url, thousands='.', decimal=',', header=None)
    
    # filter only dataframes with more than 30 rows
    df_list = [df.T.reset_index().T.reset_index(drop=True) for df in df_list if df.shape[1] > 12]
    
    return df_list

def split_and_concat(df):
    # split
    mask = df.isnull().all(axis=1).cumsum().astype(bool)
    df1 = df[~mask].dropna(how='all').reset_index(drop=True)
    df2 = df[mask].dropna(how='all').reset_index(drop=True)

    # concat and transpose
    df = pd.concat([df1, df2], axis=1, ignore_index=True).T

    return df

def fix_headers(df):
    # get new columns
    new_cols = df.iloc[0].to_list()
    new_cols[:3] = ['commodity_type','commodity','date']
    
    # fix headers
    df.columns = new_cols
    df = df.iloc[1:]
    
    return df

def reshape_dataframe(df):
    # melt columns
    df = df.melt(id_vars=['commodity_type','commodity','date'], var_name='month').copy()
    
    # concat year and month
    df['date'] = df['date'] + '-' + df['month'].str[:3].str.lower() + '.'
    df = df.drop('month',axis=1)
    
    return df
    
def convert_column_types(df):
    df['date'] = pd.to_datetime(df['date'], format='%Y-%b', errors='coerce')
    df['value'] = pd.to_numeric(df['value'], errors='coerce').replace(0,np.nan)
    return df.dropna()

def change_names(df):
    # replacement dictionaries
    comm_typ = {'G R A N O S O L E A G I N O S O S':'grains',
                'A C E I T E S':'oil',
                'P E L L E T S':'pellets',
                'E X P E L L E R S':'expellers'}
    comm = {'SOJA':'soybean',
            'GIRASOL':'sunflower',
            'LINO':'linen',
            'MANI':'peanut',
            'ALGODON':'cotton',
            'CARTAMO':'safflower',
            'CANOLA':'canola'}

    # replace
    df['commodity_type'] = df['commodity_type'].map(comm_typ)
    df['commodity'] = df['commodity'].map(comm)
    
    # add suffixes to commodity name
    suffixes = ('_' + df['commodity_type']).where(df['commodity_type'] != 'grains', '')
    df['commodity'] = df['commodity'] + suffixes
    
    # add variables
    df['variable'] = df['commodity_type'].map({'grains':'crush'}).fillna('production')

    return df


def get_crush():
    # get all dfs in url
    url = 'https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/areas/granos/_archivos/000058_Estad%C3%ADsticas/000032_Evolucion%20de%20la%20Molienda%20(Cereales%20y%20Oleaginosas)/000002_Evoluci%C3%B3n%20de%20la%20Molienda%20Mensual%20-%20Oleaginosas/000002_Evoluci%C3%B3n%20de%20la%20Molienda%20Mensual%20-%20Oleaginosas.php'
    dfs = read_raw_data(url)
    
    # for each df in list of dfs
    df = pd.DataFrame()
    for temp in dfs:
        # format temporary df
        temp = split_and_concat(temp)
        temp = fix_headers(temp)
        temp = reshape_dataframe(temp)
        temp = convert_column_types(temp)
        temp = change_names(temp)
        
        # store results
        df = pd.concat([df,temp])
        
    return df

# %% get previous

try:
    previous = pd.read_csv(filename, parse_dates=['date'], dtype={'value':'float64'})
except:
    previous = pd.DataFrame()

# %% get new data and concat

# get new data
latest = get_crush()

# merge and drop dups
df = pd.concat([previous, latest]).drop_duplicates(subset=['commodity_type', 'commodity', 'date'], keep='last')

# %% output

df.to_csv(filename, index=False)
