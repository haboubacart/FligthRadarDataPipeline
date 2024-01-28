from FlightRadar24 import FlightRadar24API
import datetime
import os
from extractor import get_airlines_data
from extractor import get_airports_data
from extractor import get_zones_data
from extractor import get_subzones_data
from extractor import get_flights_data


def create_folder_if_not_exists(data_folder):   
    created_folder = 'Data_extracted/'+ data_folder + '/tech_year='+str(datetime.date.today()).split('-')[0]+'/tech_month='+str(datetime.date.today()).split('-')[1]+ '/tech_day='+str(datetime.date.today())+'/'
    try:
        if not os.path.exists(created_folder):
            os.makedirs(created_folder)
            print('created folder ', created_folder)
        else:
            print('folder ', created_folder, ' exists')
    except :
        pass 
    return created_folder
        

def save_data_to_csv(data_folder, data_df):
    created_folder = create_folder_if_not_exists(data_folder)
    data_df.to_csv(os.path.join(created_folder, data_folder+'_5.csv'), index=False)
    

def load_data(nb_flight_per_airline_max, load_static_data=False):
    fr_api = FlightRadar24API()
    if load_static_data :
        save_data_to_csv('airlines', get_airlines_data(fr_api))
        save_data_to_csv('airports', get_airports_data(fr_api))
        save_data_to_csv('zones', get_zones_data(fr_api))
        save_data_to_csv('subzones', get_subzones_data(fr_api))
    save_data_to_csv('flights', get_flights_data(fr_api, nb_flight_per_airline_max))


if __name__=='__main__':
    load_data(5, load_static_data=True)