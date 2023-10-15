from lookups import InputTypes, SourceFiles, ErrorHandling
from logging_handler import show_error_message
from db_handler import read_data_as_dataframe 
import pandas as pd
import numpy as np

def cleaned_business_dataframe():
    df_business = None
    try:
        df_business = read_data_as_dataframe(InputTypes.CSV, SourceFiles.BUSINESS.value)
        df_business['address'].fillna("No address available", inplace=True)
        df_business['postal_code'].fillna("No postal code available", inplace=True)
        df_business['categories'] = df_business['categories'].str.split(', ')
        df_business = df_business.explode('categories', ignore_index=True)
        df_business['categorie_id'] = pd.factorize(df_business['categories'])[0] + 1
        df_business = df_business.drop(["attributes", "hours"], axis=1)
    except Exception as e:
        show_error_message(ErrorHandling.ERROR_BUSINESS_CLEANING.value, str(e))
    finally:
        return df_business

def cleaned_attributes_dataframe():
    df_business = None
    try:
        df_business = read_data_as_dataframe(InputTypes.CSV, SourceFiles.BUSINESS.value)
        df_business = df_business.dropna(subset=['attributes'])
        # Remove single quotes, curly braces, and spaces from 'attributes' column
        df_business['attributes'] = df_business['attributes'].str.replace(r"['{}]", "").str.replace('"', "").str.replace(" ", "")
        df_business['attributes'] = df_business['attributes'].str.split(',')
        df_business = df_business.explode('attributes', ignore_index=True)
        df_business = df_business[['business_id','attributes']]
        # Extract and filter attributes with ":True"
        df_business['filtered_attributes'] = df_business['attributes'].apply(lambda x: x.split(':')[0] if ':True' in x else None)
        df_business = df_business.dropna()
        # Clean the attribute names
        df_business['filtered_attributes'] = df_business['filtered_attributes'].apply(lambda x: x.replace('_', '').capitalize())
        # Drop the original 'attributes' column if needed
        df_business = df_business.drop(columns=['attributes'])
        df_business = df_business.reset_index(drop=True)
        df_business['attributes_id'] = pd.factorize(df_business['filtered_attributes'])[0] + 1

    except Exception as e:
        show_error_message(ErrorHandling.ERROR_ATTRIBUTES_CLEANING.value, str(e))
    finally:
        return df_business
    
def cleaned_checkin_dataframe():
    df_checkin = None
    try:
        df_checkin = read_data_as_dataframe(InputTypes.CSV, SourceFiles.CHECKIN.value)
        # Split the 'date' column into a list and explode it into separate rows
        df_checkin = df_checkin.assign(splited_date=df_checkin['date'].str.split(', ')).explode('splited_date')
        # Extract the 'business_id' and 'splited_date' columns
        df_checkin = df_checkin[['business_id', 'splited_date']]

    except Exception as e:
        show_error_message(ErrorHandling.ERROR_CHECKIN_CLEANING.value, str(e))
    finally:
        return df_checkin
    
def cleaned_user_dataframe():
    df_user = None
    try:
        df_user = read_data_as_dataframe(InputTypes.CSV, SourceFiles.USER.value)
        df_user['nb_friends'] = df_user['friends'].apply(lambda x: len(x.split(', ')))
        df_user = df_user.drop('friends', axis=1)
        # Replace 'elite' column with the number of elite years or 0 for NaN values
        df_user['elite'] = df_user['elite'].apply(lambda x: len(str(x).split(',')) if not pd.isna(x) else 0)

    except Exception as e:
        show_error_message(ErrorHandling.ERROR_USER_CLEANING.value, str(e))
    finally:
        return df_user
    
def cleaned_elite_user_dataframe():
    df_elite_user = None
    try:
        df_elite_user = read_data_as_dataframe(InputTypes.CSV, SourceFiles.USER.value)
        elite_years = df_elite_user['elite'][~df_elite_user['elite'].isna()].str.split(',').explode().reset_index(drop=True)
        # Combine the new 'elite_years' DataFrame with 'user_id' and 'yelping_since'
        df_elite_years = pd.concat([df_elite_user['user_id'].iloc[elite_years.index], elite_years, df_elite_user['yelping_since'].iloc[elite_years.index]], axis=1)
        # Rename columns for clarity
        df_elite_years.columns = ['user_id', 'elite_year', 'yelping_since']
        df_elite_user = df_elite_years
        df_elite_user['elite_year'] = df_elite_user['elite_year'].replace("20", "2020")
    except Exception as e:
        show_error_message(ErrorHandling.ERROR_ELITE_user_CLEANING.value, str(e))
    finally:
        return df_elite_user
    
def cleaned_dataframes_dict():
    dataframes_dict = {}
    try:
        # Clean and store each DataFrame in the dictionary
        dataframes_dict['business'] = cleaned_business_dataframe()
        dataframes_dict['attributes'] = cleaned_attributes_dataframe()
        dataframes_dict['checkin'] = cleaned_checkin_dataframe()
        dataframes_dict['user'] = cleaned_user_dataframe()
        dataframes_dict['elite_user'] = cleaned_elite_user_dataframe()
    except Exception as e:
        show_error_message(ErrorHandling.ERROR_DICT.value, str(e))
    finally:
        return dataframes_dict