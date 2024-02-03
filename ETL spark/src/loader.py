import os
from io import BytesIO
import json
import datetime
from logger import setup_logger

logger = setup_logger()
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-8]

def create_folder_if_not_exists(data_layer, data_folder_path, local=True):
    """_summary_

    Args:
        data_folder_path (_type_): _description_
        data_layer (_type_): _description_

    Returns:
        _type_: _description_
    """
    if (local):
        created_folder = data_layer + '/' + data_folder_path +'/tech_year='+str(datetime.date.today()).split('-')[0]+'/tech_month='+str(datetime.date.today()).split('-')[1]+ '/tech_day='+str(datetime.date.today())+'/'
    else:
        created_folder = data_folder_path +'/tech_year='+str(datetime.date.today()).split('-')[0]+'/tech_month='+str(datetime.date.today()).split('-')[1]+ '/tech_day='+str(datetime.date.today())+'/'
    try:
        os.makedirs(created_folder, exist_ok=True)
        print('created folder ', created_folder)
    except :
        pass 
    return created_folder



def save_data_to_local(data, filename, data_layer, file_type):
    """_summary_

    Args:
        data (_type_): _description_
        filename (_type_): _description_
        data_layer (_type_): _description_
        file_type (_type_): _description_
    """
    data_folder = create_folder_if_not_exists(data_layer, filename, True)
    if file_type == "txt":
        with open(data_folder + filename + timestamp + ".txt", "w") as file:
            for item in data:
                file.write(str(item)+"\n")
    elif file_type == "json":
        with open(data_folder + filename + timestamp + ".json", "w") as file:
            json.dump(data, file, indent=2)


def save_data_to_minio(minio_client, bucket_name, filename, data, file_type):
    """_summary_

    Args:
        minio_client (_type_): _description_
        bucket_name (_type_): _description_
        data_destination_path (_type_): _description_
        data (_type_): _description_
    """  
    if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

    data_folder = create_folder_if_not_exists(bucket_name, filename, False)
    data_destination_path = data_folder + filename + timestamp + "." + file_type
    if file_type == "txt":
        minio_client.put_object(bucket_name, data_destination_path, BytesIO(str(data).encode('utf-8')), len(data), content_type="text/plain")
    else : 
        minio_client.put_object(bucket_name, data_destination_path, BytesIO(data.encode('utf-8')), len(data), content_type="applictation/json")
    print(data_destination_path)

    
      