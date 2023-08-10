import pandas as pd
import os
from dataclasses import dataclass

from src.components.data_transformation import DataTransformation

# initialise the data ingestion 

@dataclass
class DataIngestionConfig:
    raw_data_path:str = os.path.join('artifacts','raw.csv')

class DataIngestion:
    def __init__(self) -> None:
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self):

        
        try:
            df = pd.read_csv(os.path.join('notebooks','data','Solar_Energy_Production.csv'))
            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path), exist_ok=True)
            df.to_csv(self.ingestion_config.raw_data_path, index=False)
            return self.ingestion_config.raw_data_path
        
            
        except Exception as e:
            print('Error occured at ingestion stage',e)



if __name__ == '__main__':
    obj = DataIngestion()
    ingestion_path = obj.initiate_data_ingestion()
    data_transformation = DataTransformation()
    obje_tran = data_transformation.initiate_data_transformation()
