import glob
import io
import os
import shutil
import traceback
from traceback import print_exc
import calendar
import pandas as pd
from urllib.parse import quote
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()


def get_bbox(city):
    osm_url = f"https://nominatim.openstreetmap.org/search?q={quote(city)}&format=json"
    headers = {'User-Agent': 'OpenAQCityBBox'}

    response = requests.get(osm_url, headers=headers).json()

    if not response:
        return None

    # boundingbox sisältää löydetyn kaupungin rajat
    # siinä on 4 koordinaattipisettä
    osm_bbox = response[0]['boundingbox']

    # OpenStreetMapin bounding boxin koordinaatit ovat ao järjestyksessä
    # min_y, max_y, min_x, max_x
    min_lat, max_lat, min_lon, max_lon = osm_bbox

    # järjestetään uudelleen openAQ:lle sopivaan muotoon: min_x, min_y, max_x, max_y
    openaq_bbox = f"{min_lon},{min_lat},{max_lon},{max_lat}"

    return openaq_bbox

# tämä funktio saa parametrinaan kaupungin bounding boxin get_bbox-funktiolta
def get_openaq_locations_by_bbox(_bbox):
    response = requests.get(
        f'https://api.openaq.org/v3/locations?limit=1000&page=1&order_by=id&sort_order=asc&bbox={_bbox}',
        headers={'X-API-Key': os.getenv('API_KEY')})
    _locations = []
    # muista, että http-statuskoodi 200 on OK
    # voit myös heittää poikkeuksen,
    # jos statuskoodi on jotakin muuta kuin 200
    if response.status_code == 200:
        _locations = response.json()['results']

    return _locations

def download_file_by_location(location_id, year, month):
    directory_name = "data"
    if os.path.exists(directory_name):
        shutil.rmtree(directory_name)
    if not os.path.exists(directory_name):
        os.mkdir(directory_name)

    base_url = "https://openaq-data-archive.s3.amazonaws.com"
    num_days = calendar.monthrange(year, month)[1]
    for day in range(1, num_days +1):
        date_str = f"{year}{month:02d}{day:02d}"
        key = f"records/csv.gz/locationid={location_id}/year={year}/month={month:02d}/location-{location_id}-{date_str}.csv.gz"
        full_url = f"{base_url}/{key}"

        # 2. Use requests to get the file
        response = requests.get(full_url)

        if response.status_code == 200:
            # pandas osaa avata gzip-pakatun csv
            df = pd.read_csv(io.BytesIO(response.content), compression='gzip')
            df.to_csv(f"data/{location_id}-{date_str}.csv", index=False)
            print(f"Haetaan ja tallennetaan dataa: {location_id}-{date_str}.csv.gz")
        else:
            print(f"Failed to fetch. Status: {response.status_code}")

def _populate_countries():
    with psycopg2.connect(dbname=os.getenv('DB'), user=os.getenv('DB_USER'), password=os.getenv('DB_PWD')) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM measurements;")
            cur.execute("DELETE FROM locations;")
            cur.execute("DELETE FROM cities;")
            cur.execute("DELETE FROM countries;")
            conn.commit()

            _query = 'INSERT INTO countries(id, code, name) VALUES (%s, %s, %s)'
            countries = {1: ('FI', 'Finland')}
            try:
                for key, (code, name) in countries.items():
                    cur.execute(_query,(key, code, name))
                conn.commit()
                print("Maat lisätty tietokantaan")
            except Exception as e:
                conn.rollback()
                print_exc()

def _populate_cities():
    with psycopg2.connect(dbname=os.getenv('DB'), user=os.getenv('DB_USER'), password=os.getenv('DB_PWD')) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM measurements;")
            cur.execute("DELETE FROM locations;")
            cur.execute("DELETE FROM cities;")
            conn.commit()

            _query = 'INSERT INTO cities(id, name, country_id) VALUES (%s, %s, %s)'
            cities = {1: ('Helsinki', 1)}
            try:
                for key, (name, country_id) in cities.items():
                    cur.execute(_query,(key, name, country_id))
                conn.commit()
                print("Kaupungit lisätty tietokantaan")
            except Exception as e:
                conn.rollback()
                print_exc()

def _populate_locations():
    joined_files = os.path.join("data", "*.csv")
    joined_list = glob.glob(joined_files)
    df = pd.concat(map(pd.read_csv, joined_list), ignore_index=True)
    df = df[df.columns[~df.columns.isin(['sensors_id', 'datetime', 'parameter',
                                         'units', 'value'])]]

    df['city_id'] = 1
    df = df.drop_duplicates()

    _query = 'INSERT INTO locations(location_id, lat, lon, location, city_id) VALUES (%s, %s, %s, %s, %s);'
    with psycopg2.connect(dbname=os.getenv('DB'), user=os.getenv('DB_USER'), password=os.getenv('DB_PWD')) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM measurements;")
            cur.execute("DELETE FROM locations;")
            conn.commit()
            try:
                for _index, row in df.iterrows():
                    cur.execute(_query, (row['location_id'], row['lat'],
                                row['lon'], row['location'][:-5], row['city_id']))
                conn.commit()
                print("Paikkakunnat lisätty tietokantaan")
            except Exception as e:
                conn.rollback()
                traceback.print_exc()

def _populate_sensors():
    joined_files = os.path.join("data", "*.csv")
    joined_list = glob.glob(joined_files)
    df = pd.concat(map(pd.read_csv, joined_list), ignore_index=True)
    df = df[df.columns[~df.columns.isin(['location_id', 'location',
                                         'datetime', 'lat', 'lon', 'value'])]]

    df = df.drop_duplicates()

    _query = 'INSERT INTO sensors(sensors_id, parameter, units) VALUES (%s, %s, %s);'
    with psycopg2.connect(dbname=os.getenv('DB'), user=os.getenv('DB_USER'), password=os.getenv('DB_PWD')) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM measurements;")
            cur.execute("DELETE FROM sensors;")
            conn.commit()
            try:
                for _index, row in df.iterrows():
                    cur.execute(_query, (row['sensors_id'], row['parameter'], row['units']))
                conn.commit()
                print("Sensorit lisätty tietokantaan")
            except Exception as e:
                conn.rollback()
                traceback.print_exc()

def _populate_measurements():
    joined_files = os.path.join("data", "*.csv")
    joined_list = glob.glob(joined_files)
    df = pd.concat(map(pd.read_csv, joined_list), ignore_index=True)
    df = df[df.columns[~df.columns.isin(['location', 'lat','lon',
                                         'parameter', 'units'])]]

    _query = 'INSERT INTO measurements(datetime, value, location_id, sensors_id) VALUES (%s, %s, %s, %s);'
    with psycopg2.connect(dbname=os.getenv('DB'), user=os.getenv('DB_USER'), password=os.getenv('DB_PWD')) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE measurements RESTART IDENTITY;")
            conn.commit()
            try:
                for _index, row in df.iterrows():
                    cur.execute(_query, (row['datetime'], row['value'],
                                        row['location_id'], row['sensors_id']))
                conn.commit()
                print("Mittaustulokset lisätty tietokantaan onnistuneesti")
            except Exception as e:
                conn.rollback()
                traceback.print_exc()

def run():
    # ikiluuppi pyörii koko ajan, jos ei käyttäjä katkaise suoritusta valitsemalla 0
    while True:
        _choice = input(
            "Valitse, mitä haluat tehdä:  "
            "\n(0: Lopeta, "
            "\n1: Lataa kuukauden ajalta mittausdatat csv:nä   "
            "\n2: Vie maat tietokantaan   "
            "\n3: Vie kaupungit tietokantaan   "
            "\n4: Vie paikkakunnat tietokantaan   "
            "\n5: Vie sensorit tietokantaan    "
            "\n6: Vie mittaustulokset tietokantaan    "
        )

        # jos käyttäjä valitsee 0, ohjelman suoritus lopetetaan
        if _choice == "0":
            break
        # jos valitaan 1
        elif _choice == "1":
            # kysytään vuosi
            year = input("Anna vuosi:  ")
            # ja kuukausi
            month = input("Anna kuukausi:  ")
            location_id= 2975
            download_file_by_location(int(location_id),int(year), int(month))
        elif _choice == "2":
            _populate_countries()
        elif _choice == "3":
            _populate_cities()
        elif _choice == "4":
            _populate_locations()
        elif _choice == "5":
            _populate_sensors()
        elif _choice == "6":
            _populate_measurements()

    print()
    print("done")

if __name__ == "__main__":
    bbox = get_bbox("Helsinki")
    run()



