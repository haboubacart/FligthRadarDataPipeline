from logger import setup_logger
import json
from FlightRadar24 import FlightRadar24API
from utils import save_data_to_local
from utils import save_data_to_minio
from utils import get_container_ip
from utils import add_info_to_json
from utils import create_buckets


from setup_minio import setup_minio_client

minio_client = setup_minio_client()
logger = setup_logger()

#Exception as e: pour etre plus specifique

def extract_airlines_data(fr_api):
    """ Extraction des données des compagnies aériennes
    Args:
        fr_api (FlightRadar24API): _description_ : instance de FlightRadar24API
    Returns:
        json : données des compagnies aériennes
    """
    try:
        logger.info("Début d'extraction des données des compagnies aériennes")
        airlines_data = fr_api.get_airlines()
        logger.info("Fin d'extraction des données des compagnies aériennes")
        return airlines_data
    except : 
        logger.error("Une erreur est survenue pendant l'extraction des données des compagnies aériennes")
        return None

def extract_airports_data(fr_api):
    """ Extraction des données des des aéroports
    Args:
        fr_api (FlightRadar24API): _description_ : instance de FlightRadar24API
    Returns:
        json : données des aéroports
    """
    try:
        logger.info("Début d'extraction des données des aéroports")
        airports_data = fr_api.get_airports()
        logger.info("Fin d'extraction des données des aéroports")
        return str(airports_data).strip("[]")
    except : 
        logger.error("Une erreur est survenue pendant l'extraction des données aéroports")
        return None
    

def extract_flights_data(fr_api, airlines_data):
    """ Extraction des données de vols
    Args:
        fr_api (FlightRadar24API):  : instance de FlightRadar24API
        airlines_data (json): données des compagnies aériennes
    Returns:
        json : données des vols
    """
    
    try:
        logger.info("Début d'extraction des données de vols")
        flight_details_list = []
        flight_list = []
        nb_max = 5
        for airline in airlines_data[30:60]:
            airline_flights_list = fr_api.get_flights(airline["ICAO"])
            if len(airline_flights_list) > nb_max:
                airline_flights_list = airline_flights_list[0:nb_max]           
            for flight in airline_flights_list:
                flight_details = fr_api.get_flight_details(flight)
                flight.set_flight_details(flight_details)
                flight_origin = flight.origin_airport_icao
                flight_destination = flight.destination_airport_icao                

                flight_details_list.append(flight_origin + "#" + flight_destination + "#" + airline["ICAO"])
                flight_list.append(flight)
        logger.info("Fin d'extraction des données de vols")
        return (str(flight_list).strip("[]"), str(flight_details_list).strip("[]"))
    except : 
        logger.info("Une erreur est survenue pendant l'extraction des données de vols")
        return (None, None)
    
    
def exract_zones_data(fr_api):
    """ Extraction des données des zones
    Args:
        fr_api (FlightRadar24API): instance de FlightRadar24API
    Returns:
        json : données des zones
    """
    try:
        logger.info("Début d'extraction des données des zones")
        zones_data = fr_api.get_zones()
        logger.info("Fin d'extraction des données des zones")
        return zones_data
    except : 
        logger.info("Une erreur est survenue pendant l'extraction des données des zones")
        return None


def save_all_data_to_local(airlines_data, flights_data, flight_details, airports_data, zones_data, data_layer):  
    try :
        save_data_to_local(flights_data, "Flights", data_layer, "txt")
        save_data_to_local(flight_details, "Flights_details", data_layer, "txt")


        logger.info("Données de vols chargées avec succès dans la rawzone")

        save_data_to_local(airlines_data, "Airlines", data_layer, "json")
        logger.info("Données des compagnies aériennes chargées avec succès dans la rawzone")

        save_data_to_local(zones_data, "Zones", data_layer, "json")
        logger.info("Données des zones chargées avec succès dans la rawzone")

        #save_data_to_local(airports_data, "Airports", data_layer, "txt")
        logger.info("Données des aéroports chargées avec succès dans la rawzone")
    except :
        logger.error("Une erreur est survenue lors du chargement des données dans la rawzone")
    


def save_all_data_to_minio(airlines_data, flights_data, flight_details, airports_data, zones_data, data_layer, backup_file):  
    try :
        save_data_to_minio(minio_client, data_layer, "Flights", flights_data, "txt", backup_file)
        save_data_to_minio(minio_client, data_layer, "Flights_details", flight_details, "txt", backup_file)
        logger.info("Données de vols chargées avec succès dans la rawzone")

        save_data_to_minio(minio_client, data_layer, "Airlines", json.dumps(airlines_data), "json", backup_file)
        logger.info("Données des compagnies aériennes chargées avec succès dans la rawzone")

        save_data_to_minio(minio_client, data_layer, "Zones", json.dumps(zones_data), "json", backup_file)
        logger.info("Données des zones chargées avec succès dans la rawzone")

        save_data_to_minio(minio_client, data_layer, "Airports", airports_data, "txt", backup_file)
        logger.info("Données des aéroports chargées avec succès dans la rawzone")
        
    except  Exception as e :
        print(e)
        logger.error("Une erreur est survenue lors du chargement des données dans la rawzone")

if __name__ == "__main__":
    fr_api = FlightRadar24API()
    airlines_data = extract_airlines_data(fr_api)
    (flights_data, flight_details) = extract_flights_data(fr_api, airlines_data)
    airports_data = extract_airports_data(fr_api)
    zones_data = exract_zones_data(fr_api)

    #save_all_data_to_local(airlines_data, flights_data, flight_details, airports_data, zones_data, "silver") 
    create_buckets(["rawzone", "gold"], minio_client)
    save_all_data_to_minio(airlines_data, flights_data, flight_details, airports_data, zones_data, "rawzone", "../backup_data/saved_data_paths.json")
    add_info_to_json("../backup_data/saved_data_paths.json", "Minio_IP_Adress", get_container_ip("etlspark-minio-1", "spark_minio"))
    
    
