import os
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd

def numeric_converter(string):
    # Remove non-numeric characters from the column
    cleaned_string = re.sub(r'[^0-9,]', '', string)
    cleaned_string = re.sub(r',', r'.', cleaned_string)
    # Apply pd.to_numeric() to convert the cleaned column to numeric format
    numeric = pd.to_numeric(cleaned_string, errors='coerce')
    return numeric

def fix_column_names(col_names):
    return 

class ArgentinaFarmerSelling:
    def __init__(self, date):
        # if a something other than a pandas timestamp is passed, transform into timestamp
        if type(date) != pd.Timestamp:
            date = pd.Timestamp(date)

        self.date = date
        self.date_str = date.strftime('%Y-%m-%d')

    def url_single_week(self):
        url = ('https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/areas/granos/_archivos/'
               f'000058_Estad%C3%ADsticas/_compras_historicos/{self.date_str[:4]}/01_embarque_{self.date_str}.php')
        return url

    def data_single_week(self):
        url = self.url_single_week()

        # get html file
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # get commodities names
        commodities = [comm.text for comm in soup.findAll('li', {'class':'TabbedPanelsTab'})]

        # read 1 table as example - to get number of columns
        example = pd.read_html(
            response.text,
            match='Compras y DJVE',
            attrs={'class':'tabla'}
        )[0]
        num_cols = len(example.columns)

        # get tables
        dfs = pd.read_html(
            response.text,
            match='Compras y DJVE',
            attrs={'class':'tabla'},
            thousands=None,
            converters={c:numeric_converter for c in range(2,num_cols)}
        )

        # fix columns
        

        return dfs


if __name__ == '__main__':
    query = ArgentinaFarmerSelling('2018-01-03')
    print(query.data_single_week())