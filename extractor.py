import pandas as pd
from transformer import clean_flight_or_airport
from transformer import add_airline_fligths_to_table
from transformer import extract_subzones


def get_airlines_data(fr_api):
    return pd.DataFrame(fr_api.get_airlines())

def get_airports_data(fr_api):
    airport_df = pd.DataFrame(columns=['Code', 'Name', 'Altitude', 'Latitude', 'Longitude'])
    for airport in fr_api.get_airports()[0:10] : 
            airport_df.loc[len(airport_df)] = clean_flight_or_airport(airport)
    return airport_df 
    
def get_flights_data(fr_api, nb_flight_per_airline_max):
    flights_df = pd.DataFrame(columns=['Aircraft type', 'Immatriculation', 'Altitude', 'Ground Speed', 'Heading', 'Origin', 'Destination', 'Airline_Code'])
    for airline_icao_code in get_airlines_data(fr_api)['ICAO'][30:40]: #limite a supprimer 
        flights_df = add_airline_fligths_to_table(fr_api, flights_df, airline_icao_code, nb_flight_per_airline_max)
    return flights_df

def get_zones_data(fr_api):
    zones_df = pd.DataFrame.from_dict(fr_api.get_zones(),  orient='index')
    zones_df.reset_index(inplace=True)
    zones_df.rename(columns={'index': 'Zone'}, inplace=True)
    return zones_df.drop(['subzones'], axis=1)


#Possibilité d'optimisation en minimisant les appels APIs pour un même besoin en data
def get_subzones_data(fr_api):
    subzones_data = []
    data = fr_api.get_zones()
    for zone_name, zone_details in data.items():
        if 'subzones' in zone_details:
            subzones_data.extend(extract_subzones(zone_details['subzones'], zone_name))
    subzones_df = pd.DataFrame(subzones_data)
    return subzones_df