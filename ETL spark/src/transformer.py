from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, StringType

from logger import setup_logger
logger = setup_logger()

#<(LECO) A Coruna Airport - Altitude: 326 - Latitude: 43.302059 - Longitude: -8.37725> => ['LECO', 'A Coruna Airport', '326', '43.302059', '', '8.37725']
#traitement suppelementaire : ['LECO', 'A Coruna Airport', '326', '43.302059', '', '8.37725'] ==> ['LECO', 'A Coruna Airport', '326', '43.302059', '-8.37725']

@pandas_udf(ArrayType(StringType()))
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

def get_subzones_data(spark, zones_data):    
    """_summary_

    Args:
        spark (_type_): _description_
        zones_data (_type_): _description_

    Returns:
        _type_: _description_
    """
    subzones_data = []
    for zone_name, zone_details in zones_data.items():
        if 'subzones' in zone_details:
            subzones_data.extend(extract_subzones_from_zone(zone_details['subzones'], zone_name))
    subzones_df = spark.createDataFrame(subzones_data)
    return subzones_df


def transorm_flights_data():
    pass

def transorm_airlines_data():
    pass

def transorm_aiports_data():
    pass


spark = SparkSession.builder \
   .appName("Transformer") \
   .getOrCreate()
# Apply the UDF to clean each line
data = [("Hello, World!",)]
df = spark.createDataFrame(data, ["message"])

# Afficher le contenu du DataFrame
df.show()
# Show the cleaned DataFrame

# Stop the Spark session
spark.stop()

