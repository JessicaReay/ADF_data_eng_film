import pandas as pd
from stages.extract import extract
from stages.transform import transform_merge, generate_report
from stages.reporting import table

def test_all_three_files_have_same_len():
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

def test_rank_insert():
    dataframes = extract()
    final_df= transform_merge(dataframes)
    report_df= generate_report(final_df)

    assert max(report_df.Rank) == len(report_df)

def test_genre():
    dataframes = extract()
    final_df= transform_merge(dataframes)
    report_df= generate_report(final_df)
    unique_df = pd.DataFrame(final_df.Genre.unique())

    assert len(report_df) == len(unique_df)

def test_top():
    dataframes = extract()
    final_df= transform_merge(dataframes)
    report_df= generate_report(final_df)
    table(report_df)

    top1_table = table(report_df)[0][2]
    max_rev = final_df.groupby('Genre').agg({'Revenue_Millions': 'sum'}).max()[0]
        
    assert top1_table == max_rev




