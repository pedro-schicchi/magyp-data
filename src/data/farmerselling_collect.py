# built-in libraries
import os
# third-party libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
# custom libraries
from .utils import std_str_series, numeric_converter


def get_report_dates_in_year(year):        
    # read dfs in page
    url = f'https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/areas/granos/_archivos/000058_Estad%C3%ADsticas/_compras_historicos/{year}/{year}.php'
    dfs = {df.size:df for df in pd.read_html(url, thousands='.', decimal=',')}
    
    # find largest df
    df = dfs[max(dfs.keys())]
    
    # get only dates
    dates = df.melt(value_name='dates')['dates']
    dates = pd.to_datetime(dates, format='%d/%m/%Y', errors='coerce')
    dates = dates.sort_values()
    dates = dates.dropna().reset_index(drop=True)
    
    return dates

def format_colnames(columns):
    # removes numbers and dates at the end of columns
    new_columns = columns.str.split(' y| \(', n=1, expand=True).get_level_values(0)
    new_columns = new_columns.str.replace('*','', regex=True)
    # makes everything lower case and removes whitespaces
    new_columns = std_str_series(new_columns)
    return new_columns

class RawFarmerSellingData:
    
    def __init__(self, dates=None, year=None):
        """
        Given a  year or a date gets Argentina's Farmer Selling data from MAGyP website
        Instance has to be given a date or a year. Dates will be favored over year.
        
        """
        
        # Preprocess inputs to make sure a list of dates is being passed
        if not dates:
            try:
                dates = get_report_dates_in_year(year)
            except:
                raise ValueError(r'One of the two "date" or "year" has to be passed. ')
        
        elif isinstance(dates, str) or isinstance(dates, pd.Timestamp):
            # Convert a single string into a list of a single element
            dates = [dates]
        
        elif isinstance(dates, list):
            # Do nothing, as it is already a list
            pass
        
        else:
            raise ValueError("Invalid input type. Expected dates to be either a string or list.")

        
        # Now that it was made sure that we have a list of dates, convert it to pandas datetime format
        dates = pd.to_datetime(dates)

        # Assigns the attributes
        self.dates = dates

    def get_single_week(self, date):
        # get date str and prin to user
        date_str = date.strftime('%Y-%m-%d')
        print(f'getting {date_str}')
        
        # get html file
        url = ('https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/areas/granos/_archivos/'
               f'000058_Estad%C3%ADsticas/_compras_historicos/{date_str[:4]}/01_embarque_{date_str}.php')
        response = requests.get(url)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            # get commodities names
            commodities = [comm.text for comm in soup.findAll('li', {'class':'TabbedPanelsTab'})]

            # read 1 table as example - to get number of columns
            example = pd.read_html(response.text, match='Compras y DJVE', attrs={'class':'tabla'})[0]
            num_cols = len(example.columns)

            # get tables
            dfs = pd.read_html(
                response.text,
                match='Compras y DJVE',
                attrs={'class':'tabla'},
                thousands=None,
                converters={c:numeric_converter for c in range(2,num_cols)}
            )

            # fix column names (has to be done before merging all into one)
            dfs = [df.set_axis(format_colnames(df.columns), axis=1) for df in dfs]
            
            # merge in single df
            df = pd.concat({comm:df for comm,df in zip(commodities, dfs)})
            df = df.reset_index(level=0,names='producto').reset_index(drop=True)
            
            df['date'] = date

            return df
        
        else:
            return pd.DataFrame()
    
    def get_data(self):
        # gets all data
        date_data = {date:self.get_single_week(date) for date in self.dates}
        # eliminates empty df's (unavailable data)
        results = {k:v for k,v in date_data.items() if len(v)}
        self.results = results
        return results
    
    def save_data(self, out_dir):
        print('saving files')
        for date, df in self.results.items():
            # feedback to user
            date_str = date.strftime('%Y-%m-%d')

            # saves into parquet files
            df.to_parquet(os.path.join(out_dir,f'fs_{date_str}.parquet'))

if __name__ == '__main__':
    # fix folders
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR,'..','..','data','raw','farmer_selling')
    
    # gets the data
    for y in range(2018,2024):
        print('getting data')
        query = RawFarmerSellingData(year=y)
        results = query.get_data()
        results = query.save_data(DATA_DIR)
        
        # print('\nsaving files')
        # for date, df in results.items():
        #     # feedback to user
        #     date_str = date.strftime('%Y-%m-%d')

        #     # saves into parquet files
        #     df.to_parquet(os.path.join(OUTPUT_DIR,f'fs_{date_str}.parquet'))
        
        # print('\n')
        # # to test
        # print(df.head())
    