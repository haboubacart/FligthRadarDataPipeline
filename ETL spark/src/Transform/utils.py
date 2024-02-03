import json
import re
import datetime
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-8]


def get_minio_file_path_with_bucket(bucket_name, backup_file_path, folder_name):
    """_summary_

    Args:
        bucket_name (_type_): _description_
        bakuo_file_path (_type_): _description_
        folder_name (_type_): _description_

    Returns:
        _type_: _description_
    """
    with open(backup_file_path, 'r') as file:
        data = json.load(file)
    return bucket_name + "/" + data[folder_name]

def get_minio_file_path_without_bucket(backup_file_path, folder_name):
    """_summary_

    Args:
        bucket_name (_type_): _description_
        bakuo_file_path (_type_): _description_
        folder_name (_type_): _description_

    Returns:
        _type_: _description_
    """
    with open(backup_file_path, 'r') as file:
        data = json.load(file)
    return data[folder_name]


def generate_new_path(timestamp, raw_data_path, expr):
    # Utiliser une expression régulière pour extraire la partie souhaitée
    match = re.match(rf"(.*?/{expr}).*", raw_data_path)

    if match:
        return match.group(1)+timestamp+".csv"
    else:
        return None

# Test de la fonction avec votre exemple
input_string = "Flights/tech_year=2024/tech_month=02/tech_day=2024-02-03/Flights202402031426.txt"
extracted_path = generate_new_path(timestamp, input_string, "Flights")
print(extracted_path)

#print(get_container_ip("etlspark-minio-1", "spark_minio"))

