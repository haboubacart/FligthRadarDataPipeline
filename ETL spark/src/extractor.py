from logger import setup_logger
import json
import time
from FlightRadar24 import FlightRadar24API
from loader import save_data_txt
from loader import save_data_json

logger = setup_logger()


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
        return airports_data
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
        for airline in airlines_data[30:40]:
            airline_flights_list = fr_api.get_flights(airline["ICAO"])
            if len(airline_flights_list) > nb_max:
                airline_flights_list = airline_flights_list[0:nb_max]           
            for flight in airline_flights_list:
                flight_details = fr_api.get_flight_details(flight)
                flight.set_flight_details(flight_details)
                flight_origin = flight.origin_airport_icao
                flight_destination = flight.destination_airport_icao                

                flight_details_list.append(flight_origin + "#" + flight_destination)
                flight_list.append(flight)
        logger.info("Fin d'extraction des données de vols")
        return (flight_list, flight_details_list)
    except : 
        logger.info("Une erreur est survenue pendant l'extraction des données de vols")
        return None
    
    
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

def extract_and_save_data():
    fr_api = FlightRadar24API()
    airlines_data = extract_airlines_data(fr_api)
    (flights_data, flight_details) = extract_flights_data(fr_api, airlines_data)
    airports_data = extract_airports_data(fr_api)
    zones_data = exract_zones_data(fr_api)
    
    try :
        save_data_txt(flights_data, "Flights", "rawzone")
        save_data_txt(flight_details, "Flights_details", "rawzone")
        logger.info("Données de vols chargées avec succès dans la rawzone")

        save_data_json(airlines_data, "Airlines", "rawzone")
        logger.info("Données des compagnies aériennes chargées avec succès dans la rawzone")

        save_data_json(zones_data, "Zones", "rawzone")
        logger.info("Données des zones chargées avec succès dans la rawzone")

        save_data_txt(airports_data, "Airports", "rawzone")
        logger.info("Données des aéroports chargées avec succès dans la rawzone")

        
    except :
        logger.error("Une erreur est survenue lors du chargement des données dans la rawzone")
    
    
extract_and_save_data()
