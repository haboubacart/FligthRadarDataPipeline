from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, split
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
import json
import os
import datetime
from utils import get_minio_file_path_with_bucket
from utils import get_minio_file_path_without_bucket
from utils import generate_new_path

from dotenv import load_dotenv
load_dotenv()

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-8]

backup_file_path =  "/spark/data/backup_data/saved_data_paths.json"
Minio_IP =  get_minio_file_path_without_bucket(backup_file_path, "Minio_IP_Adress")


#installer dotenv pour importer dynamiquement les variables
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://" + Minio_IP + ":9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

#<(LECO) A Coruna Airport - Altitude: 326 - Latitude: 43.302059 - Longitude: -8.37725> => ['LECO', 'A Coruna Airport', '326', '43.302059', '', '8.37725']
#traitement suppelementaire : ['LECO', 'A Coruna Airport', '326', '43.302059', '', '8.37725'] ==> ['LECO', 'A Coruna Airport', '326', '43.302059', '-8.37725']
@udf(ArrayType(StringType()))
def clean_flight_or_airport(flight_or_airport) :
    """_summary_

    Args:
        flight_or_airport (_type_): _description_

    Returns:
        _type_: _description_
    """
    flight_or_airport_element_list = str(flight_or_airport).replace('<','').replace('>','').split(' - ')
    flight_or_airport = [s.split(':')[-1].strip().replace("(", "") for s in flight_or_airport_element_list]
    for i in range(len(flight_or_airport)):
        if (flight_or_airport[i]==''):
            flight_or_airport[i+1] = str(-1*float(flight_or_airport[i+1]))     
    while '' in flight_or_airport:
        flight_or_airport.remove('')
    first_elem = flight_or_airport[0].split(')')
    first_elem.extend(flight_or_airport[1:])
    return [s.strip() for s in first_elem]


def extract_subzones_from_zone(zone, parent_zone=None):
    """_summary_

    Args:
        zone (_type_): _description_
        parent_zone (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    subzones = []
    for subzone_name, subzone_details in zone.items():
        subzone_data = {
            'Subzone': subzone_name,
            'Parent Zone': parent_zone,
            'tl_y': subzone_details.get('tl_y'),
            'tl_x': subzone_details.get('tl_x'),
            'br_y': subzone_details.get('br_y'),
            'br_x': subzone_details.get('br_x')
        }
        subzones.append(subzone_data)
        if 'subzones' in subzone_details:
            subzones.extend(extract_subzones_from_zone(subzone_details['subzones'], subzone_name))
    return subzones

def get_subzones_df(zones_data):    
    """_summary_

    Args:
        spark (_type_): _description_
        zones_data (_type_): _description_

    Returns:
        _type_: _description_
    """
    zones_data = json.loads(zones_data)
    subzones_data = []
    for zone_name, zone_details in zones_data.items():
        if 'subzones' in zone_details:
            subzones_data.extend(extract_subzones_from_zone(zone_details['subzones'], zone_name))
    subzones_schema = StructType([
        StructField("Parent Zone", StringType(), nullable=True),
        StructField("Subzone", StringType(), nullable=True),
        StructField("br_x", StringType(), nullable=True),
        StructField("br_y", StringType(), nullable=True),
        StructField("tl_x", StringType(), nullable=True),
        StructField("tl_y", StringType(), nullable=True)
    ])
    return spark.createDataFrame(subzones_data, schema=subzones_schema)

def get_zones_df(zones_data):
    zones_data = json.loads(zones_data)
    zones = []
    for zone_name, zone_data in zones_data.items():
        # Exclure les zones avec une clé "subzones"
        if 'subzones' not in zone_data:
            zones.append({
                'Zone': zone_name,
                'br_x': zone_data['br_x'],
                'br_y': zone_data['br_y'],
                'tl_x': zone_data['tl_x'],
                'tl_y': zone_data['tl_y'],
            })
    zones_schema = StructType([
        StructField("Zone", StringType(), nullable=True),
        StructField("br_x", StringType(), nullable=True),
        StructField("br_y", StringType(), nullable=True),
        StructField("tl_x", StringType(), nullable=True),
        StructField("tl_y", StringType(), nullable=True)
    ])

    return spark.createDataFrame(zones, schema=zones_schema)

def get_flight_or_airport_df(file_path, list_columns):
    """_summary_

    Args:
        file_path (_type_): _description_
        list_columns (_type_): _description_

    Returns:
        _type_: _description_
    """
    df = spark.read.text(file_path)
    df = df.withColumn("value", explode(split(df["value"], ",")))
    for i in range(len(list_columns)):
        df = df.withColumn(list_columns[i], clean_flight_or_airport(df["value"]).getItem(i))
    return df
    
@udf(ArrayType(StringType()))
def clean_flight_details(flight_details):
    return str(flight_details).split("#")

def get_flights_details_df(file_path, list_columns):
    df = spark.read.text(file_path)
    df = df.withColumn("value", explode(split(df["value"], ",")))
    for i in range(len(list_columns)):
        df = df.withColumn(list_columns[i], clean_flight_details(df["value"]).getItem(i))
    return df

def get_airlines_df(file_path):
    df = spark.read.json(file_path)
    return df


###################### Main ##################


airport_columns_list  = ['Code', 'Name', 'Altitude', 'Latitude', 'Longitude']
flight_columns_list = ['Aircraft type', 'Immatriculation', 'Altitude', 'Ground Speed', 'Heading']
flight_details_columns_list  = ['Origin', 'Destination', 'Airline Code']


origin_bucket_name = "rawzone"
dest_bucket_name = "gold"

#Ajouter la création dynamique des buckets


airlines_origin_file_path = "s3a://" + get_minio_file_path_with_bucket(origin_bucket_name, backup_file_path, "Airlines")
airlines_dest_file_path = "s3a://" + dest_bucket_name + "/" + generate_new_path(timestamp, get_minio_file_path_without_bucket(backup_file_path, "Airlines"), "Airlines")


airports_origin_file_path = "s3a://" + get_minio_file_path_with_bucket(origin_bucket_name, backup_file_path, "Airports")
airports_dest_file_path = "s3a://"  + dest_bucket_name + "/" + generate_new_path(timestamp, get_minio_file_path_without_bucket(backup_file_path, "Airports"), "Airports")


flights_origin_file_path = "s3a://" + get_minio_file_path_with_bucket(origin_bucket_name, backup_file_path, "Flights")
flights_dest_file_path = "s3a://"  + dest_bucket_name + "/" +generate_new_path(timestamp, get_minio_file_path_without_bucket(backup_file_path, "Flights"), "Flights")


flights_details_origin_file_path = "s3a://" + get_minio_file_path_with_bucket(origin_bucket_name, backup_file_path, "Flights_details")
flights_details_dest_file_path = "s3a://"  + dest_bucket_name + "/" +generate_new_path(timestamp, get_minio_file_path_without_bucket(backup_file_path, "Flights_details"), "Flights_details")


Zones_origin_file_path = "s3a://" + get_minio_file_path_with_bucket(origin_bucket_name, backup_file_path, "Zones")
Zones_dest_file_path = "s3a://"  + dest_bucket_name + "/" +generate_new_path(timestamp, get_minio_file_path_without_bucket(backup_file_path, "Zones"), "Zones")
Sub_zones_dest_file_path = Zones_dest_file_path.replace("Zones", "Sub_zones")
Zones_json_content = sc.textFile(Zones_origin_file_path).collect()[0]  


airlines_df = get_airlines_df(airlines_origin_file_path)
zones_df = get_zones_df(Zones_json_content)
subzones_df = get_subzones_df(Zones_json_content)
airport_df = get_flight_or_airport_df(airports_origin_file_path, airport_columns_list)
flights_df = get_flight_or_airport_df(flights_origin_file_path, flight_columns_list)
flights_details_df = get_flights_details_df(flights_details_origin_file_path, flight_details_columns_list)



flights_details_df.show()

airlines_df.write.csv(airlines_dest_file_path, mode="overwrite", header=True)
airport_df.write.csv(airports_dest_file_path, mode="overwrite", header=True)
zones_df.write.csv(Zones_dest_file_path, mode="overwrite", header=True)
subzones_df.write.csv(Sub_zones_dest_file_path, mode="overwrite", header=True)
flights_df.write.csv(flights_dest_file_path, mode="overwrite", header=True)
flights_details_df.write.csv(flights_details_dest_file_path, mode="overwrite", header=True)


print("okkkkkkkkkkk")
# Arrêter la session Spark
spark.stop()
