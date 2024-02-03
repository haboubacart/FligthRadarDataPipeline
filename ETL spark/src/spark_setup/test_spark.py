from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import udf, explode, split
from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, StringType, DoubleType
import json
import os

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

minio_ip = "172.18.0.3"
minio_url = "http://"+minio_ip+":9000"
print(minio_url)
# Set the MinIO access key, secret key, endpoint, and other configurations
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "oq5wASQScY3nsiYSrb12")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "UnChjBOuCYBL7CkEaI5hJmQWmVNYZ4YLu0jEn1kn")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://172.18.0.3:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
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
    


# Créer une session Spark



airport_columns_list  = ['Code', 'Name', 'Altitude', 'Latitude', 'Longitude']

file_path = "s3a://rawzone/Airports/tech_year=2024/tech_month=02/tech_day=2024-02-02/Airports202402021152.txt"
json_file_path = "s3a://rawzone/Zones/tech_year=2024/tech_month=02/tech_day=2024-02-03/Zones202402031214.json"

json_content = sc.textFile(json_file_path).collect()[0]  # Récupérer le contenu JSON sous forme de chaîne de caractères
#df = read_and_clean_airport_or_flight_file(file_path, airport_columns_list)
df = spark.read.json(json_file_path)


get_zones_df(json_content).show()
# Afficher le DataFrame résultant avec les données nettoyées

print("okkkkkkkkkkk")
# Arrêter la session Spark
spark.stop()
