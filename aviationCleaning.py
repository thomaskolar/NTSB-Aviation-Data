#AUTHOR: THOMAS KOLAR
import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np

#Function to Connect to database
def connect_to_db(db_params):
    return psycopg2.connect(
        dbname=db_params['dbname'],
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host'],
        port=db_params['port']
    )

#Fetch raw NTSB data from previous script
def fetch_data_from_ntsb_table(cursor, source_table):
    fetch_query = sql.SQL("SELECT * FROM {table}").format(table=sql.Identifier(source_table))
    cursor.execute(fetch_query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(data, columns=columns)

#Drop blank collumns and unnecessary data for analysis
def drop_unnecessary_columns(data):
    columns_to_delete = [
        'reportno', 'hassafetyrec', 'reporttype', 'originalpublishdate', 
        'eventid', 'scheduled', 'repgenflag', 'docketurl', 
        'docketpublishdate', 'unnamed_37'
    ]
    return data.drop(columns=columns_to_delete, errors='ignore')

#Fill missing data with "Not-Listed" or "None"
def fill_missing_values(data):
    columns_to_fill = [
        'eventtype', 'eventdate', 'city', 'state', 'country', 
        'probablecause', 'make', 'model', 
        'airportid', 'airportname', 'purposeofflight', 'far', 
        'aircraftdamage', 'weathercondition', 'operator', 
        'reportstatus', 'n'
    ]
    data[columns_to_fill] = data[columns_to_fill].fillna('Not-Listed')
    data['highestinjurylevel'] = data['highestinjurylevel'].fillna('None')
    return data

#Fill missing longitude and latitude with 0
def clean_long_lad(data):
    data['longitude'] = pd.to_numeric(data['longitude'], errors='coerce').fillna(0)
    data['latitude'] = pd.to_numeric(data['latitude'], errors='coerce').fillna(0)
    return data

#Filling missing values in the injury count collumns to 0
def convert_injury_columns(data):
    injury_columns = ['fatalinjurycount', 'seriousinjurycount', 'minorinjurycount']
    data[injury_columns] = data[injury_columns].fillna(0).astype(int)
    return data

#Clean the number of engines column
def clean_number_of_engines(data):
    
    def clean_entry(entry):
        if pd.isna(entry):
            return np.nan
        engines = [int(e) for e in str(entry).split(',') if e.isdigit()]
        return sum(engines) if engines else np.nan

    data['numberofengines'] = data['numberofengines'].apply(clean_entry)
    mode_value = data['numberofengines'].mode()[0]
    data['numberofengines'].fillna(mode_value, inplace=True)
    return data

#Clean the amateurbuilt column
def clean_amateur_built(data):
    
    def clean_entry(entry):
        if pd.isna(entry):
            return 'UNKNOWN'
        values = set(str(entry).split(','))
        if values.issubset({'TRUE', 'FALSE'}):
            return values.pop() if len(values) == 1 else 'UNKNOWN'
        return 'UNKNOWN'

    data['amateurbuilt'] = data['amateurbuilt'].apply(clean_entry)
    return data

#Filter to only keep rows where the country is the United States
def filter_united_states(data):
    return data[data['country'] == 'United States'].reset_index(drop=True)

#Clean and standardize the weathercondition column
def clean_weather_condition(data):
    
    def standardize_weather(entry):
        if pd.isna(entry):
            return 'Unknown'
        entry_upper = str(entry).strip().upper()
        if entry_upper in ['VMC', 'VFR']:
            return 'Visual'
        elif entry_upper in ['IMC', 'IFR']:
            return 'Instrument'
        elif entry_upper in ['UNKNOWN', 'UNK']:
            return 'Unknown'
        else:
            return 'Unknown'
    
    data['weathercondition'] = data['weathercondition'].apply(standardize_weather)
    return data

#Clean and format the eventdate column
def clean_event_date(data):
    
    def format_date(entry):
        if pd.isna(entry):
            return None
        try:
            return pd.to_datetime(entry).strftime('%Y-%m-%d')
        except ValueError:
            return None

    data['eventdate'] = data['eventdate'].apply(format_date)
    return data

#Clean and standardize the highestinjurylevel column
def clean_highest_injury_level(data):
    
    def standardize_injury_level(entry):
        if pd.isna(entry) or entry in ['None', 'Unknown']:
            return 'None'
        valid_entries = {'Serious', 'Fatal', 'Minor'}
        return entry if entry in valid_entries else 'None'
    
    data['highestinjurylevel'] = data['highestinjurylevel'].apply(standardize_injury_level)
    return data

#Clean and standardize the 'aircraftcategory' column
def clean_aircraft_category(data):
    
    def standardize_category(entry):
        if pd.isna(entry):
            return 'Unknown'
        categories = set(entry.split(','))
        category_mapping = {
            'GLI': 'Glider',
            'BALL': 'Balloon',
            'AIR': 'Airplane',
            'HELI': 'Helicopter',
            'WSFT': 'Weight-Shift-Control',
            'PPAR': 'Powered Parachute',
            'CSF': 'Commercial Space Flight',
            'GYRO': 'Gyroplane',
            'UNMANNED': 'Unmanned Aircraft',
            'ULTR': 'Ultralight',
            'BLIM': 'Blimp',
            'PLFT': 'Powered Lift',
            'UNK': 'Unknown'
        }
        standardized_categories = {category_mapping.get(cat, 'Unknown') for cat in categories}
        if len(standardized_categories) > 1:
            return 'Mixed'
        elif len(standardized_categories) == 1:
            return standardized_categories.pop()
        else:
            return 'Unknown'
    
    data['aircraftcategory'] = data['aircraftcategory'].apply(standardize_category)
    return data

#Clean and standardize the aircraftdamage column
def clean_aircraft_damage(data):
    
    def standardize_damage(entry):
        if pd.isna(entry):
            return 'Unknown'
        damages = set(entry.split(','))
        valid_damages = {'Substantial', 'Destroyed', 'Minor', 'Unknown', 'None'}
        valid_filtered = [damage for damage in damages if damage in valid_damages]
        if len(valid_filtered) > 1:
            return 'Mixed'
        elif len(valid_filtered) == 1:
            return valid_filtered[0]
        else:
            return 'Unknown'
    
    data['aircraftdamage'] = data['aircraftdamage'].apply(standardize_damage)
    return data

#Clean and standardize abbreviations in the purposeofflight column
def clean_purpose_of_flight(data):
    
    def standardize_purpose(entry):
        if pd.isna(entry):
            return 'Unknown'
        purposes = set(entry.split(','))
        valid_purposes = {
            'PERS': 'Personal',
            'BUS': 'Business',
            'UNK': 'Unknown',
            'ASHO': 'Aerial Show',
            'INST': 'Instructional',
            'POSI': 'Positioning',
            'AAPL': 'Agricultural Application',
            'OWRK': 'Other Work',
            'PUBL': 'Public Use',
            'BANT': 'Banner Tow',
            'FIRF': 'Firefighting',
            'FERY': 'Ferry',
            'PUBU': 'Public Use',
            'FLTS': 'Flight Test',
            'AOBV': 'Aerial Observation',
            'EXEC': 'Executive Transport',
            'GLDT': 'Glider Tow',
            'PUBF': 'Public Firefighting',
            'SKYD': 'Skydiving',
            'ADRP': 'Adverse Weather Research'
        }
        standardized_purposes = {valid_purposes.get(purpose, 'Unknown') for purpose in purposes}
        if len(standardized_purposes) > 1:
            return 'Mixed'
        elif len(standardized_purposes) == 1:
            return standardized_purposes.pop()
        else:
            return 'Unknown'
    
    data['purposeofflight'] = data['purposeofflight'].apply(standardize_purpose)
    return data

#Function to run all the clean functions for each column and return the clean data
def clean_data(data):
    """Clean the ntsb_data."""
    data = drop_unnecessary_columns(data)
    data = data.dropna(subset=['eventdate'])
    data = filter_united_states(data)
    data = fill_missing_values(data)
    data = clean_long_lad(data)
    data = convert_injury_columns(data)
    data = clean_number_of_engines(data)
    data = clean_amateur_built(data)
    data = clean_weather_condition(data)
    data = clean_event_date(data)
    data = clean_highest_injury_level(data)
    data = clean_aircraft_category(data)
    data = clean_aircraft_damage(data)
    data = clean_purpose_of_flight(data)
    return data

#Create table if one does not already exist
def create_table(cursor, table_name):
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            ntsbno TEXT PRIMARY KEY,
            eventtype TEXT,
            mkey INTEGER,
            eventdate TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            n TEXT,
            highestinjurylevel TEXT,
            fatalinjurycount INTEGER,
            seriousinjurycount INTEGER,
            minorinjurycount INTEGER,
            probablecause TEXT,
            latitude FLOAT,
            longitude FLOAT,
            make TEXT,
            model TEXT,
            aircraftcategory TEXT,
            airportid TEXT,
            airportname TEXT,
            amateurbuilt TEXT,
            numberofengines TEXT,
            purposeofflight TEXT,
            far TEXT,
            aircraftdamage TEXT,
            weathercondition TEXT,
            operator TEXT,
            reportstatus TEXT
        );
    """).format(table=sql.Identifier(table_name))
    cursor.execute(create_table_query)

#Insert cleaned data into DB
def insert_data(cursor, table_name, data):
    insert_query = sql.SQL("""
        INSERT INTO {table} (
            ntsbno, eventtype, mkey, eventdate, city, state, country, n, highestinjurylevel,
            fatalinjurycount, seriousinjurycount, minorinjurycount, probablecause, latitude,
            longitude, make, model, aircraftcategory, airportid, airportname, amateurbuilt,
            numberofengines, purposeofflight, far, aircraftdamage, weathercondition, operator,
            reportstatus
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ntsbno)
        DO UPDATE SET
            eventtype = EXCLUDED.eventtype,
            mkey = EXCLUDED.mkey,
            eventdate = EXCLUDED.eventdate,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            country = EXCLUDED.country,
            n = EXCLUDED.n,
            highestinjurylevel = EXCLUDED.highestinjurylevel,
            fatalinjurycount = EXCLUDED.fatalinjurycount,
            seriousinjurycount = EXCLUDED.seriousinjurycount,
            minorinjurycount = EXCLUDED.minorinjurycount,  -- Corrected here
            probablecause = EXCLUDED.probablecause,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            make = EXCLUDED.make,
            model = EXCLUDED.model,
            aircraftcategory = EXCLUDED.aircraftcategory,
            airportid = EXCLUDED.airportid,
            airportname = EXCLUDED.airportname,
            amateurbuilt = EXCLUDED.amateurbuilt,
            numberofengines = EXCLUDED.numberofengines,
            purposeofflight = EXCLUDED.purposeofflight,
            far = EXCLUDED.far,
            aircraftdamage = EXCLUDED.aircraftdamage,
            weathercondition = EXCLUDED.weathercondition,
            operator = EXCLUDED.operator,
            reportstatus = EXCLUDED.reportstatus;
    """).format(table=sql.Identifier(table_name))

    for _, row in data.iterrows():
        cursor.execute(insert_query, tuple(row))


### MAIN ###
def main():
    #Define PostgreSQL connection parameters
    db_params = {
        'dbname': 'database name',
        'user': 'postgres',
        'password': 'password to database',
        'host': 'localhost',
        'port': '5432'
    }

    #Connect to the DB
    conn = connect_to_db(db_params)
    cursor = conn.cursor()

    try:
        #Fetch and clean data from ntsb_data table
        source_table = 'ntsb_data'
        data = fetch_data_from_ntsb_table(cursor, source_table)
        cleaned_data = clean_data(data)

        #Insert/update cleaned data into aviation_data table
        target_table = 'aviation_data'
        create_table(cursor, target_table)
        insert_data(cursor, target_table, cleaned_data)

        #Commit changes
        conn.commit()

    #Close connection to DB
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
