import os
import datetime
from logger import setup_logger

logger = setup_logger()


def create_folder_if_not_exists(data_folder_path):   
    created_folder = 'Data_extracted/'+ data_folder_path + '/tech_year='+str(datetime.date.today()).split('-')[0]+'/tech_month='+str(datetime.date.today()).split('-')[1]+ '/tech_day='+str(datetime.date.today())+'/'
    try:
        os.makedirs(created_folder, exist_ok=True)
        print('created folder ', created_folder)
    except :
        pass 
    return created_folder
        

def save_data_to_csv(data_folder_path, data_df):
    created_folder = create_folder_if_not_exists(data_folder_path)
    data_df.to_csv(os.path.join(created_folder, data_folder_path+'_5.csv'), index=False)
    