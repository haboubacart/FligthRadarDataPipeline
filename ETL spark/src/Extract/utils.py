import os
from io import BytesIO
import json
import datetime
import docker

timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-8]

def get_container_ip(container_name, network):
    """_summary_

    Args:
        container_name (_type_): _description_
        network (_type_): _description_

    Returns:
        _type_: _description_
    """
    
    client = docker.from_env()
    container_attr = client.containers.get(container_name).attrs
    dict_cont = dict(container_attr['NetworkSettings']['Networks'])
    return (dict_cont["etlspark_"+network]['IPAddress'])

def add_info_to_json(backup_file, key, value):
    with open(backup_file, 'r+') as f:
            try:
                data = json.load(f)
            except :
                data = {}
            data[key] = value
            f.seek(0) 
            json.dump(data, f, indent=2)

    
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
        os.makedirs(created_folder, exist_ok=True)
        print('created folder ', created_folder)
    else:
        created_folder = data_folder_path +'/tech_year='+str(datetime.date.today()).split('-')[0]+'/tech_month='+str(datetime.date.today()).split('-')[1]+ '/tech_day='+str(datetime.date.today())+'/'
        print("Path created : ",created_folder)
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

def create_buckets(buckets_name_list, minio_client):
    for bucket_name in buckets_name_list :
        if not minio_client.bucket_exists(bucket_name):
                minio_client.make_bucket(bucket_name)

def save_data_to_minio(minio_client, bucket_name, filename, data, file_type, backup_file):
    """_summary_

    Args:
        minio_client (_type_): _description_
        bucket_name (_type_): _description_
        filename (_type_): _description_
        data (_type_): _description_
        file_type (_type_): _description_
        backup_file (_type_): _description_
    """

    data_folder = create_folder_if_not_exists(bucket_name, filename, False)
    data_destination_path = data_folder + filename + timestamp + "." + file_type
    if file_type == "txt":
        minio_client.put_object(bucket_name, data_destination_path, BytesIO(str(data).encode('utf-8')), len(data), content_type="text/plain")
    else : 
        minio_client.put_object(bucket_name, data_destination_path, BytesIO(data.encode('utf-8')), len(data), content_type="applictation/json")
    
    #ajouter le chemin du fichier stocké dans minio pour facilier l'acces à spark
    add_info_to_json(backup_file, filename, data_destination_path)
    


      