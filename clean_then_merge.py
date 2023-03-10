import pandas as pd
import glob
from pathlib import Path
import os

class DataMergeAndClean():
    def __init__(self):
        path = 'csv_files/'
        self.all_files = glob.glob(path + '*.csv')
        self.df_list = []
        self.cleaned_df_list = []

    def create_df_list(self):
        #remove_columns = ['CancellationCode', 'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay']
        for csv_file in self.all_files:
            df = pd.read_csv(csv_file)
            head = df.head(1)
            print(f'Header: {head}')
            print(type(df))
            self.df_list.append(df)

        print(type(self.df_list))

    def clean_data(self):
        self.create_df_list()
        for df in self.df_list:
    #removes columns that have null values in all dataframes
            nan_columns_removed_df = df.drop(df.columns[[22, 24, 25, 26, 27, 28]], axis=1)
            head = nan_columns_removed_df.head(1)
            print(head)
    #replaces null values with 0
            cleaned_df = nan_columns_removed_df.fillna(0)
            self.cleaned_df_list.append(cleaned_df)

    def merge_dataframes(self):
        self.clean_data()
        self.master_df = pd.concat(self.cleaned_df_list, ignore_index=True)
        master_size = self.master_df.shape
        print(f'size of cleaned master dataframe: {master_size}')
        #self.cleaned_master_df.info()
        nan_count = self.master_df.isna().sum()
        print(f'Number of NaNs in master_df:\n{nan_count}')

    def write_master_to_csv(self):
        self.merge_dataframes()
        self.path = "csv_files/combined_data.csv"
        if not os.path.exists(self.path):
            self.master_df.to_csv(self.path)
            print('combined_data.csv has been created')
        else:
            print('combined_data.csv already exists.')
        

merge_files = DataMergeAndClean()
merge_files.write_master_to_csv()




