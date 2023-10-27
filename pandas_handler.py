from lookups import InputTypes, SourceFiles, ErrorHandling
from logging_handler import show_error_message
from db_handler import read_data_as_dataframe 
import pandas as pd
import numpy as np
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Define the assign_numeric_id function
numeric_id_counter = 0
business_id_to_numeric_id = {}

def assign_numeric_id(business_id):
    global numeric_id_counter
    if business_id not in business_id_to_numeric_id:
        numeric_id_counter += 1
        business_id_to_numeric_id[business_id] = numeric_id_counter
    return business_id_to_numeric_id[business_id]

def cleaned_business_dataframe():
    df_business = None
    try:
        df_business = read_data_as_dataframe(InputTypes.CSV, SourceFiles.BUSINESS.value)
        df_business['address'].fillna("No address available", inplace=True)
        df_business['postal_code'].fillna("No postal code available", inplace=True)
        df_business = df_business.drop(["attributes", "hours","categories"], axis=1)
        df_business['numeric_id'] = df_business['business_id'].apply(assign_numeric_id)
    except Exception as e:
        show_error_message(ErrorHandling.ERROR_BUSINESS_CLEANING.value, str(e))
    finally:
        return df_business

def cleaned_business_categories_dataframe():
    df_business = None
    try:
        df_business = read_data_as_dataframe(InputTypes.CSV, SourceFiles.BUSINESS.value)
        df_business['address'].fillna("No address available", inplace=True)
        df_business['postal_code'].fillna("No postal code available", inplace=True)
        df_business['categories'] = df_business['categories'].str.split(', ')
        df_business = df_business.explode('categories', ignore_index=True)
        df_business['categorie_id'] = pd.factorize(df_business['categories'])[0] + 1
        df_business = df_business.drop(["attributes", "hours"], axis=1)
        category_list = [
                        "Restaurants", "Nightlife", "Bars", "Food", "Cafes", "Fast Food", "Italian",
                        "British", "Coffee & Tea", "Indian", "Pizza", "Breakfast & Brunch", "Chinese",
                        "Sandwiches", "Japanese", "Burgers", "Pubs", "Mediterranean", "Thai",
                        "Cocktail Bars", "French", "Wine Bars", "Fish & Chips", "Pakistani", "Middle Eastern",
                        "Modern European", "Turkish", "Sushi Bars", "Delis", "American (Traditional)", "Seafood",
                        "Asian Fusion", "Steakhouses", "Gastropubs", "Mexican", "Vietnamese", "Event Planning & Services",
                        "Chicken Shop", "Spanish", "Bakeries", "Caribbean", "Vegetarian", "Greek", "Salad",
                        "Vegan", "Halal", "Arts & Entertainment", "Chicken Wings", "Food Stands", "Tapas Bars",
                        "Juice Bars & Smoothies", "Venues & Event Spaces", "American (New)", "Lebanese",
                        "Barbeque", "Tapas/Small Plates", "Korean", "African", "Brasseries", "Specialty Food",
                        "Diners", "Desserts", "Persian/Iranian", "Delicatessen", "Lounges", "Dim Sum", "Latin American",
                        "Bistros", "Food Delivery Services", "Street Vendors", "Malaysian", "Portuguese", "Gluten-Free",
                        "Brazilian", "Shopping", "Kebab", "Noodles", "Creperies", "Bangladeshi", "Music Venues",
                        "Patisserie/Cake Shop", "Argentine", "Falafel", "Ramen"
                        ] #got it from yelp website
        df_business = df_business[df_business['categories'].isin(category_list)]

        df_business['numeric_id'] = df_business['business_id'].apply(assign_numeric_id)
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
        df_business['numeric_id'] = df_business['business_id'].apply(assign_numeric_id)

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
        df_checkin.rename(columns={'splited_date': 'date'}, inplace=True)
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
        df_user.rename(columns={'yelping_since': 'date'}, inplace=True)
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
        df_elite_user.rename(columns={'yelping_since': 'date'}, inplace=True)
        df_elite_user['elite_year_id'] = pd.factorize(df_elite_user['elite_year'])[0] + 1
    except Exception as e:
        show_error_message(ErrorHandling.ERROR_ELITE_user_CLEANING.value, str(e))
    finally:
        return df_elite_user
    
def cleaned_top_10_resto():
        df_top_10_resto = None
        try:
            df_top_10_resto= read_data_as_dataframe(InputTypes.CSV, SourceFiles.Top10Business.value)
            df_top_10_resto['numeric_id'] = df_top_10_resto['business_id'].apply(assign_numeric_id)
        except Exception as e:
            show_error_message(ErrorHandling.ERROR_TOP_10_CLEANNING.value, str(e))
        finally:
            return df_top_10_resto
    
def cleaned_bottom_10_resto():
    df_bottom_10_resto = None
    try:
        df_bottom_10_resto  = read_data_as_dataframe(InputTypes.CSV, SourceFiles.Bottom10Business.value)
        df_bottom_10_resto['numeric_id'] = df_bottom_10_resto['business_id'].apply(assign_numeric_id)
    except Exception as e:
        show_error_message(ErrorHandling.ERRO_BOTTOM_10_CLEANNING.value, str(e))
    finally:
        return df_bottom_10_resto
    
#SENTIMENT ANALYSIS
# def cleaned_review_dataframe():
#     df_review = None
#     try:
#         df_review = read_data_as_dataframe(InputTypes.CSV, SourceFiles.REVIEW.value)
#         analyzer = SentimentIntensityAnalyzer()
#         sentiments = []
#         for text in df_review['text']:
#             sentiment = analyzer.polarity_scores(text)
#             compound_score = sentiment['compound']
#             if compound_score >= 0.05:
#                 sentiment_label = 'Positive'
#             elif compound_score <= -0.05:
#                 sentiment_label = 'Negative'
#             else:
#                 sentiment_label = 'Neutral'
#             sentiments.append({'Sentiment': sentiment_label, 'Score': compound_score})
#         # Convert the list of sentiment results into a DataFrame
#         sentiments_df = pd.DataFrame(sentiments)
#         # Concatenate the original DataFrame 'review' with the 'sentiments_df'
#         df_review = pd.concat([df_review, sentiments_df], axis=1)
#     except Exception as e:
#         show_error_message(ErrorHandling.ERROR_IN_SENTIMENT_ANALYSIS.value, str(e))
#     finally:
#         return df_review
    
def cleaned_dataframes_dict():
    dataframes_dict = {}
    try:
        # Clean and store each DataFrame in the dictionary
        dataframes_dict['business'] = cleaned_business_dataframe()
        dataframes_dict['attributes'] = cleaned_attributes_dataframe()
        dataframes_dict['categories'] = cleaned_business_categories_dataframe() 
        dataframes_dict['checkin'] = cleaned_checkin_dataframe()
        dataframes_dict['checkin']['date'] = pd.to_datetime(dataframes_dict['checkin']['date'], format='%Y-%m-%d %H:%M:%S')
        dataframes_dict['user'] = cleaned_user_dataframe()
        dataframes_dict['user']['date'] = pd.to_datetime(dataframes_dict['user']['date'], format='%Y-%m-%d %H:%M:%S')
        dataframes_dict['elite_user'] = cleaned_elite_user_dataframe()
        dataframes_dict['elite_user']['date'] = pd.to_datetime(dataframes_dict['elite_user']['date'], format='%Y-%m-%d %H:%M:%S')
        dataframes_dict['review'] =  read_data_as_dataframe(InputTypes.CSV, SourceFiles.REVIEW.value)
        dataframes_dict['review']['date'] = pd.to_datetime(dataframes_dict['review']['date'], format='%Y-%m-%d %H:%M:%S')
        dataframes_dict['top_10_resto']  = cleaned_top_10_resto()
        dataframes_dict['bottom_10_resto']  = cleaned_bottom_10_resto()
    except Exception as e:
        show_error_message(ErrorHandling.ERROR_DICT.value, str(e))
    finally:
        return dataframes_dict

