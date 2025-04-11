import requests 
import logging
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
tqdm.pandas()
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logging.basicConfig(
    filename='fetching_flights_data.log',
    level=logging.INFO,
    format='%(asctime)s — %(levelname)s — %(message)s'
)

def log(msg):
    print(msg)
    logging.info(msg)

def get_flight_data(api_key,flight_icao):
    '''Fetches flight data from Aviationstack API'''

    URL = f'https://api.aviationstack.com/v1/flights?access_key={api_key}&flight_icao={flight_icao}'
    response = requests.get(URL)
    data = response.json()
    return data


def get_airplane_data(api_key, icao_code):
    URL = f'https://api.aviationstack.com/v1/airplanes?access_key={api_key}&iata_code_long={icao_code}'
    response = requests.get(URL)
    data = response.json()
    return data

def get_lat(code_id, airports_df):
    for index, row in airports_df.iterrows():
        if row['ident'] == code_id:
            return row['latitude_deg']
        
def get_lon(code_id, airports_df):
    for index, row in airports_df.iterrows():
        if row['ident'] == code_id:
            return row['longitude_deg']

def fetch_flights(api_key1, api_key2):
    # Bounding Box for the Black Sea (approximately)
    # Min Longitude, Min Latitude, Max Longitude, Max Latitude
    BOUNDING_BOX = {
    'min_lat': 40,
    'max_lat': 46,
    'min_lon': 27,
    'max_lon': 41}

    airports_df = pd.read_csv('airports.csv')

    # Fetch flight data from OpenSky Network for the Black Sea region
    user_name=''
    password=''
    opensky_url='https://'+user_name+':'+password+'@opensky-network.org/api/states/all?'+'lamin='+str(BOUNDING_BOX['min_lat'])+'&lomin='+str(BOUNDING_BOX['min_lon'])+'&lamax='+str(BOUNDING_BOX['max_lat'])+'&lomax='+str(BOUNDING_BOX['max_lon'])
    log('Downloading data from OpenSky Network')
    opensky_response=requests.get(opensky_url).json()


    col_name=['icao24','callsign','origin_country','time_position','last_contact','long','lat','baro_altitude','on_ground','velocity',       
    'true_track','vertical_rate','sensors','geo_altitude','squawk','spi','position_source']
    flight_df=pd.DataFrame(opensky_response['states'])
    flight_df=flight_df.loc[:,0:16]
    flight_df.columns=col_name  
    flight_df['callsign'] = flight_df['callsign'].apply(lambda x: x.strip())
    opensky_icaos = flight_df['callsign'].tolist()

    log('Dowloading data from Aviationstack API...')
    fetched_flight_data = []
    for icao_id in tqdm(opensky_icaos):
        fetched_flight_data.append(get_flight_data(api_key1, icao_id))

    black_sea_flights = []
    for i in fetched_flight_data:
        if 'data' in i and i['data'] != []:
            black_sea_flights.append(i['data'][0])

    df = pd.json_normalize(black_sea_flights, sep='_')

    log('Adding coordinates to dataframe...')
    # Departure coordinates
    df['departure_latitude'] = df['departure_icao'].progress_apply(lambda x: get_lat(x, airports_df))
    df['departure_longitude'] = df['departure_icao'].progress_apply(lambda x: get_lon(x, airports_df))
    # Arrival coordinates
    df['arrival_latitude'] = df['arrival_icao'].progress_apply(lambda x: get_lat(x, airports_df))
    df['arrival_longitude'] = df['arrival_icao'].progress_apply(lambda x: get_lon(x, airports_df))

    black_sea_icaos = df['aircraft_icao'].tolist()
    fetched_airplanes_data = []
    log('Downloading airplanes data from Aviationstack API...')
    for icao_id in tqdm(black_sea_icaos):
        fetched_airplanes_data.append(get_airplane_data(api_key2, icao_id)['data'])

    saved_models = []
    for i in fetched_airplanes_data:
        if i != []:
            saved_models.append(i[0])

    models_df = pd.json_normalize(saved_models, sep='_')
    models_df = models_df.rename(columns={
    'iata_code_long': 'aircraft_icao'})
    models_df = models_df.drop_duplicates(subset='aircraft_icao')
    merged_df = pd.merge(df, models_df[['model_name', 'model_code', 'aircraft_icao']], 
                     on='aircraft_icao', how='left')
    
    merged_df.to_csv('mdbs.csv', index=False, encoding='utf-8')

    return merged_df


def load_to_db(df, db_name='c_2'):

    username = 'postgres'
    password = 'your_password'
    host = 'localhost'      
    port = '5432'

    db_config = {
        'user': username,
        'password': password,
        'host': host,
        'port': port}
    
    new_db_name = db_name

    conn = psycopg2.connect(
        dbname='postgres',
        **db_config
        )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (new_db_name,))
    exists = cur.fetchone()

    if not exists:
        cur.execute(f'CREATE DATABASE {new_db_name}')
        db = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{new_db_name}')
    else:
        db = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{new_db_name}')


    flights = df[['flight_date', 'flight_status', 'flight_number', 'flight_iata', 'flight_icao',
              'flight_codeshared', 'departure_icao', 'arrival_icao', 'airline_icao', 'aircraft_icao']]

    dep_cols = ['departure_icao', 'departure_iata', 'departure_airport', 'departure_timezone',
            'departure_terminal', 'departure_gate', 
            'departure_latitude', 'departure_longitude']
    arr_cols = ['arrival_icao', 'arrival_iata', 'arrival_airport', 'arrival_timezone',
            'arrival_terminal', 'arrival_gate', 
            'arrival_latitude', 'arrival_longitude']

    dep_airports = df[dep_cols].rename(columns=lambda x: x.replace("departure_", ""))
    arr_airports = df[arr_cols].rename(columns=lambda x: x.replace("arrival_", ""))
    airports = pd.concat([dep_airports, arr_airports]).drop_duplicates(subset=['icao'])

    airlines = df[['airline_icao', 'airline_iata', 'airline_name']]
    aircraft = df[['aircraft_icao', 'aircraft_iata', 'aircraft_registration', 'aircraft_icao24', 'model_name', 'model_code']]

    flights.to_sql('flights', db, if_exists='replace', index=False)
    airports.to_sql('airports', db, if_exists='replace', index=False)
    airlines.to_sql('airlines', db, if_exists='replace', index=False)
    aircraft.to_sql('aircraft', db, if_exists='replace', index=False)

    return 'Data has been uploaded to the database'
