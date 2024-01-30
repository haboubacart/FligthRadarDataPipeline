from logger import setup_logger
logger = setup_logger()

#<(LECO) A Coruna Airport - Altitude: 326 - Latitude: 43.302059 - Longitude: -8.37725> => ['LECO', 'A Coruna Airport', '326', '43.302059', '', '8.37725']
#traitement suppelementaire : ['LECO', 'A Coruna Airport', '326', '43.302059', '', '8.37725'] ==> ['LECO', 'A Coruna Airport', '326', '43.302059', '-8.37725']
def clean_flight_or_airport(flight_or_airport) :
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

def transorm_flights_data():
    pass

def transorm_airlines_data():
    pass

def transorm_aiports_data():
    pass