from logger import setup_logger
logger = setup_logger()

#<(LECO) A Coruna Airport - Altitude: 326 - Latitude: 43.302059 - Longitude: -8.37725> => ['LECO', 'A Coruna Airport', '326', '43.302059', '', '8.37725']
#traitement suppelementaire : ['LECO', 'A Coruna Airport', '326', '43.302059', '', '8.37725'] ==> ['LECO', 'A Coruna Airport', '326', '43.302059', '-8.37725']
def clean_flight_or_airport(flight_or_airport) : 
    flight_or_airport_element_list = str(flight_or_airport).replace('<','').replace('>','').split(' - ')
    flight_or_airport = [s.split(':')[-1].strip().replace("(", "") for s in flight_or_airport_element_list]
    for i in range(len(flight_or_airport)):
        if (flight_or_airport[i]==''):
            flight_or_airport[i+1] = -1*float(flight_or_airport[i+1])       
    while '' in flight_or_airport:
        flight_or_airport.remove('')
    first_elem = flight_or_airport[0].split(')')
    first_elem.extend(flight_or_airport[1:])
    return [s.strip() for s in first_elem]

def add_airline_fligths_to_table(fr_api, flights_df, airline_icao_code, nb_flight_max) :
    airline_flights_list = fr_api.get_flights(airline_icao_code)
    if len(airline_flights_list) < nb_flight_max:
        nb_flight_max = len(airline_flights_list)
    if nb_flight_max > 0:
        for flight in airline_flights_list[0:nb_flight_max]:
            flight_details = fr_api.get_flight_details(flight)
            flight.set_flight_details(flight_details)
            cleaned_flight = clean_flight_or_airport(flight)
            cleaned_flight.extend([flight.origin_airport_icao, flight.destination_airport_icao, airline_icao_code])
            flights_df.loc[len(flights_df)] = cleaned_flight
    return flights_df

def extract_subzones(zone, parent_zone=None):
    subzone_data = []
    for subzone_name, subzone_details in zone.items():
        subzone_data.append({
            'Subzone': subzone_name,
            'Parent Zone': parent_zone,
            'tl_y': subzone_details.get('tl_y'),
            'tl_x': subzone_details.get('tl_x'),
            'br_y': subzone_details.get('br_y'),
            'br_x': subzone_details.get('br_x')
        })
        if 'subzones' in subzone_details:
            subzone_data.extend(extract_subzones(subzone_details['subzones'], subzone_name))
    return subzone_data

logger.info('INFO')