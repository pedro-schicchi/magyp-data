import os
import pandas as pd
from data.farmerselling_collect import RawFarmerSellingData
from data.farmerselling_cleaning import ProcessedFarmerSellingData

# fix folders
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR,'..','data','raw','farmer_selling')
OUTPUT_DIR = os.path.join(BASE_DIR,'..','data','processed')

# find last date available
def last_date(data_dir):
    fns = [fn[3:13] for fn in os.listdir(data_dir)]
    fns = pd.to_datetime(fns)
    return fns.max()
    

if __name__ == '__main__':
    
    # input from user
    get_last = input('Get last date [y] or reload entire year [n]?') == 'y'
    
    # find last available
    new_date = last_date(DATA_DIR) + pd.DateOffset(days=7)
    year = new_date.year
    
    print('---')
    print('getting raw data')
    if get_last:
        query = RawFarmerSellingData(dates=new_date)
    else:
        query = RawFarmerSellingData(year=year)
    query.get_data()
    query.save_data(DATA_DIR)
            
    print('---')
    print('processing raw data')
    data = ProcessedFarmerSellingData(DATA_DIR)
    data.load_data()
    data.process_data()
    data.save_data(OUTPUT_DIR)
    