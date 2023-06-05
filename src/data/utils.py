import re
import requests
import pandas as pd

def is_link_active(url):
    try:
        response = requests.head(url)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def numeric_converter(string):
    # Remove non-numeric characters from the column
    cleaned_string = re.sub(r'[^0-9,]', '', string)
    cleaned_string = re.sub(r',', r'.', cleaned_string)
    # Apply pd.to_numeric() to convert the cleaned column to numeric format
    numeric = pd.to_numeric(cleaned_string, errors='coerce')
    return numeric

def std_str_series(series):
    # makes everything lower case and removes whitespaces
    new_series = series.str.lower()
    new_series = new_series.str.strip()
    new_series = new_series.str.replace(r'\s+', ' ', regex=True)
    new_series = new_series.str.replace(' ','_')
    return new_series