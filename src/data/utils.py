import re

def remove_non_numeric(string):
    # Remove non-numeric characters from the column
    cleaned_string = re.sub(r'[^0-9,]', '', string)
    cleaned_string = re.sub(r',', r'.', cleaned_string)
    # # Apply pd.to_numeric() to convert the cleaned column to numeric format
    # numeric = pd.to_numeric(cleaned_string, errors='coerce')
    return numeric