from logger import setup_logger
import json
from FlightRadar24 import FlightRadar24API
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
        logger.info("Début d'extraction des données des aéroports")
        for flight in airlines_data["ICAO"]:
            flight_details = fr_api.get_flight_details(flight)
            flight.set_flight_details(flight_details)
            flight_origin = flight.origin_airport_icao
            flight_destination =  flight.destination_airport_icao
            ########
            flights_data = fr_api.get_airports()
            logger.info("Fin d'extraction des données des aéroports")
            return flights_data
    except : 
        logger.info("Une erreur est survenue pendant l'extraction des données des aéroports")
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
        zones_data = fr_api.get_airports()
        logger.info("Fin d'extraction des données des zones")
        return zones_data
    except : 
        logger.info("Une erreur est survenue pendant l'extraction des données des zones")
        return None


"""
fr_api = FlightRadar24API()
extract_airlines_data(fr_api)
extract_airports_data(fr_api)
"""    
