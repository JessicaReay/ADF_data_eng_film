import pandas as pd
from stages.extract import extract
from stages.transform import transform_merge, generate_report


'''
Five pytests:
- test_all_three_files_have_same_len: checks that all three extracted dataframes have the same number of rows before to confirm if ready for merge.
- test_merge_were_successful: checks that after merging on the index that all the titles match and therefore the join was successful for each title. 
- test_genre_are_unique: checks that the group by was successful (and no duplicates or missing genres) by comparing the number of rows for the group by genres by unique of the genres in the merged dataframe. 
- test_rank_insert: checks that the ranking for the genres have correcting be assigned. 
'''

def test_all_three_files_have_same_len():
    #Arrange
    extracted_warehouse_df = extract()[0]
    extracted_datalake_df = extract()[1]
    extracted_local_df = extract()[2]

    # Assert
    assert len(extracted_warehouse_df) == len(extracted_datalake_df)
    assert len(extracted_datalake_df) == len(extracted_local_df)
    assert len(extracted_warehouse_df) == len(extracted_local_df)

def test_merge_were_successful():
    dataframes = extract()
    final_df= transform_merge(dataframes)

    #Arrange
    check_column_1 = final_df['Title_x']
    check_column_2 = final_df['Title_y']
    check_column_3 = final_df['Title']

    #Assert
    assert check_column_1.equals(check_column_2)
    assert check_column_2.equals(check_column_3)
    assert check_column_1.equals(check_column_3)

def test_genre_are_unique():
    dataframes = extract()
    final_df= transform_merge(dataframes)
    
    #Arrange
    report_df= generate_report(final_df)
    unique_df = pd.DataFrame(final_df.Genre.unique())

    #Assert
    assert len(report_df) == len(unique_df)

def test_rank_insert():
    dataframes = extract()

    #Arrange
    final_df= transform_merge(dataframes)
    report_df= generate_report(final_df)

    #Assert
    assert max(report_df.Rank) == len(report_df)




