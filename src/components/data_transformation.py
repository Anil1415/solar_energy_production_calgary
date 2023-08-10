import pandas as pd
import os
from dataclasses import dataclass
# from src.components.data_ingestion import DataIngestion

@dataclass
class DataTransformationConfig:

    transformed_file_path = os.path.join('artifacts','clean_data.csv')

class DataTransformation:
    def __init__(self):
        self.data_transformation_config = DataTransformationConfig()

    def initiate_data_transformation(self):
        try:
            df_raw = pd.read_csv(os.path.join('artifacts','raw.csv'))
            df_raw.drop(columns=['id','address','public_url','uid'], axis=1, inplace=True)
            df_raw['year'] = pd.to_datetime(df_raw['date']).dt.year
            df_raw['month'] = pd.to_datetime(df_raw['date']).dt.month
            df_raw['day'] = pd.to_datetime(df_raw['date']).dt.day
            df_raw['time'] = pd.to_datetime(df_raw['date']).dt.time
            df_raw['time'] = df_raw['time'].astype(str)
            df_raw['time_hour'] = df_raw['time'].apply(lambda x:x.split(':')[0])
            df_raw['time_min'] = df_raw['time'].apply(lambda x:x.split(':')[1])
            df_raw['time_hour']  = df_raw['time_hour'].astype(int)
            df_raw['time_min']  = df_raw['time_min'].astype(int)
            df_raw.drop(labels=['date','time'], axis=1, inplace= True)
            df_raw['installationDate'] = pd.to_datetime(df_raw['installationDate'])

            # df_grouped = df_dropped.groupby(['city_id','year','month','day','hour']).first()
            os.makedirs(os.path.dirname(self.data_transformation_config.transformed_file_path), exist_ok=True)

            df_raw.to_csv(self.data_transformation_config.transformed_file_path, index=False)
            print(df_raw.head())
            return self.data_transformation_config.transformed_file_path

            
        except Exception as e:
            print("an error occured",e)


if __name__ == '__main__':
    # obj = DataIngestion()
    # ingestion_path = obj.initiate_data_ingestion()
    data_transformation = DataTransformation()
    obje_tran = data_transformation.initiate_data_transformation()

