import pandas as pd
from stages.extract import extract

def transform_index(dataframes):
    
    extracted_warehouse_df = extract()[0]
    extracted_datalake_df = extract()[1]
    extracted_local_df = extract()[2]
    
    dataframes= [extracted_warehouse_df, extracted_datalake_df, extracted_local_df]

    for df in dataframes:
        if "index" in df.columns:
            df.set_index('index', inplace=True)
    
    return dataframes

def transform_merge(dataframes):

   final_df = pd.merge(pd.merge(transform_index(dataframes)[0], transform_index(dataframes)[1], left_index= True, right_index= True), transform_index(dataframes)[2], left_index= True, right_index= True)
   
   return final_df

def generate_report(final_df):

    report_df = transform_merge(final_df).groupby('Genre').agg({'Revenue_Millions': 'sum', 'Votes': 'sum', 'Genre': 'count'}).rename(columns = {'Genre': 'Movie_count'}).sort_values(by = 'Revenue_Millions', ascending = False)
    report_df.insert(0, 'Rank', range(1, 1 + len(report_df)))

    return report_df






