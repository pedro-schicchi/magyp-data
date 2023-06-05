# built-in libraries
import os
# third-party libraries
import numpy as np
import pandas as pd
# custom libraries
from .utils import std_str_series#, numeric_converter


class ProcessedFarmerSellingData:
    
    possible_buyers = {
        'industria':'industry',
        'exportador':'exporters',
        'total':'total'
    }
    commodities = {
        'trigo':'wheat',
        'maíz':'corn',
        'sorgo':'sorghum',
        'cebada cervecera':'barley',
        'cebada forrajera':'barley_feed',
        'soja':'soybean',
        'girasol':'sunflower'
    }
            
    def get_filenames(self):
        if self.year:
            return [os.path.join(self.data_dir, fn) for fn in os.listdir(self.data_dir) if f'_{self.year}-' in fn]
        else:
            return [os.path.join(self.data_dir, fn) for fn in os.listdir(self.data_dir)]
    
    def __init__(self, data_dir, year=None):
        self.data_dir = data_dir
        self.year = year
        self.filenames = self.get_filenames()

    def fix_buyers_names(self, series):
        new_series = series
        for key,value in self.possible_buyers.items():
            new_series = new_series.where(~new_series.str.contains(key), value)
        return new_series
    
    def fix_commodities_names(self, series):
        new_series = series.map(self.commodities)
        return new_series

    def load_data(self):
        df = pd.concat([pd.read_parquet(fn) for fn in self.filenames]).reset_index(drop=True)
        self.raw_data = df
        return df
    
    def process_data(self):
        df = self.raw_data
        
        # rename dict and find value cols
        idx_cols = {'producto':'commodity', 'date':'date', 'compras':'buyer', 'cosecha':'crop'}
        df = df.rename(columns=idx_cols)
        
        # find rows that refer to y-1
        mask = (df.groupby(['commodity','buyer','date'])['crop'].shift(1) == df['crop']).fillna(False)
        df['ref_period'] = pd.Series(np.where(mask, 'y-1', 'y'))
        
        # define idx and columns values for later reshaping 
        idx_cols = list(idx_cols.values()) + ['ref_period']
        # value_cols = [col for col in df.columns if col not in idx_cols]
        
        # standardize string columns
        df[['commodity', 'buyer']] = df[['commodity', 'buyer']].apply(std_str_series)
        df['buyer'] = self.fix_buyers_names(df['buyer'])
        df['commodity'] = self.fix_commodities_names(df['commodity'])
        
        # fillnas based on the following equations
        # # total_comprado = total_acumulado
        # # total_comprado = total_precio_hecho + total_a_fijar
        # # total_a_fijar = total_fijado + saldo_a_fijar
        df['total_comprado'] = df['total_comprado'].fillna(df['total_acumulado'])
        df['total_comprado'] = df['total_comprado'].fillna(df['total_precio_hecho'] + df['total_a_fijar'])
        df['total_precio_hecho'] = df['total_precio_hecho'].fillna(df['total_comprado'] - df['total_a_fijar'])
        df['saldo_a_fijar'] = df['saldo_a_fijar'].fillna(df['total_a_fijar'] - df['total_fijado'])
        df['total_a_fijar'] = df['total_a_fijar'].fillna(df['total_fijado'] + df['saldo_a_fijar'])
        
        # drop redundant columns
        df = df.drop('total_acumulado', axis=1)
        
        # melt columns
        df = df.melt(id_vars=idx_cols, var_name='attribute',  value_name='value')
        
        # add new attribute
        self.processed_data = df
        return df
    
    def save_data(self, out_dir):
        print('saving files')
        self.processed_data.to_csv(os.path.join(out_dir,'ar_farmerselling.csv'), index=False)
    
if __name__ == '__main__':
    # fix folders
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR,'..','..','data','raw','farmer_selling')
    OUTPUT_DIR = os.path.join(BASE_DIR,'..','..','data','processed')
    
    # get data from raw
    data = ProcessedFarmerSellingData(DATA_DIR)
    data.load_data()
    data.process_data()
    
    # save into csv for easy consumption
    data.save_data()