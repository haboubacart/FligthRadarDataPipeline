import os
import json
import datetime
from logger import setup_logger

logger = setup_logger()
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-8]

def create_folder_if_not_exists(data_folder_path, data_layer):   
    created_folder = 'Data_extracted/'+ data_folder_path + '/' + data_layer +'/tech_year='+str(datetime.date.today()).split('-')[0]+'/tech_month='+str(datetime.date.today()).split('-')[1]+ '/tech_day='+str(datetime.date.today())+'/'
    try:
        os.makedirs(created_folder, exist_ok=True)
        print('created folder ', created_folder)
    except :
        pass 
    return created_folder

def save_data_txt(data, filename, data_layer):
    data_folder = create_folder_if_not_exists(filename, data_layer)
    with open(data_folder + filename + timestamp + ".txt", "w") as file:
        for item in data:
            file.write(str(item)+"\n")

def save_data_json(data, filename, data_layer):
    data_folder = create_folder_if_not_exists(filename, data_layer)
    with open(data_folder + filename + timestamp + ".json", "w") as file:
        json.dump(data, file, indent=2)
    