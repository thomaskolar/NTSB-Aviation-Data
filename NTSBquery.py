#AUTHOR: THOMAS KOLAR
import requests
from bs4 import BeautifulSoup
import psycopg2
import pandas as pd
from io import StringIO

#Connect to PostgreSQL database
def connect_to_db(db_params):
    return psycopg2.connect(
        dbname=db_params['dbname'],
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host'],
        port=db_params['port']
    )

#Create a table "ntsb_data" if it doesn't exist (with NtsbNo as the primary key)
def create_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ntsb_data (
            NtsbNo TEXT PRIMARY KEY,
            EventType TEXT,
            Mkey TEXT,
            EventDate TEXT,
            City TEXT,
            State TEXT,
            Country TEXT,
            ReportNo TEXT,
            N TEXT,
            HasSafetyRec TEXT,  
            ReportType TEXT,
            OriginalPublishDate TEXT, 
            HighestInjuryLevel TEXT,
            FatalInjuryCount TEXT,  
            SeriousInjuryCount TEXT,  
            MinorInjuryCount TEXT, 
            ProbableCause TEXT,
            EventID TEXT,
            Latitude TEXT,
            Longitude TEXT,
            Make TEXT,
            Model TEXT,
            AirCraftCategory TEXT,
            AirportID TEXT,
            AirportName TEXT,
            AmateurBuilt TEXT,
            NumberOfEngines TEXT,
            Scheduled TEXT,
            PurposeOfFlight TEXT,
            FAR TEXT,
            AirCraftDamage TEXT,
            WeatherCondition TEXT,
            Operator TEXT,
            ReportStatus TEXT,
            RepGenFlag TEXT,
            DocketUrl TEXT,
            DocketPublishDate TEXT,
            Unnamed_37 TEXT
        );
    """)

#Fetch the latest NTSB aviation data from the website
def fetch_ntsb_data():
    url = 'https://www.ntsb.gov/Pages/AviationQueryv2.aspx'
    response = requests.get(url)

    #Parse the response for necessary values
    soup = BeautifulSoup(response.content, 'html.parser')
    viewstate = soup.find('input', {'id': '__VIEWSTATE'})['value']
    eventvalidation = soup.find('input', {'id': '__EVENTVALIDATION'})['value']

    #Prepare the payload and make a POST request
    payload = {
        '__VIEWSTATE': viewstate,
        '__EVENTVALIDATION': eventvalidation,
        'ctl00$PlaceHolderMain$AviationHome1$btnSubmit': 'Submit Query'
    }
    post_response = requests.post(url, data=payload)

    #Construct the CSV download URL
    query_id = post_response.url.split('queryId=')[1]
    csv_url = f"https://www.ntsb.gov/_layouts/15/NTSB.AviationInvestigationSearch/Download.ashx?queryId={query_id}&type=csv"

    #Download the CSV data
    csv_response = requests.get(csv_url)
    csv_content = StringIO(csv_response.text)
    return pd.read_csv(csv_content, low_memory=False)

#Insert or update the fetched data into the database.
def insert_data(cursor, data):
    for _, row in data.iterrows():
        cursor.execute("""
            INSERT INTO ntsb_data (
                NtsbNo, EventType, Mkey, EventDate, City, State, Country, ReportNo, N, HasSafetyRec, 
                ReportType, OriginalPublishDate, HighestInjuryLevel, FatalInjuryCount, SeriousInjuryCount, 
                MinorInjuryCount, ProbableCause, EventID, Latitude, Longitude, Make, Model, AirCraftCategory, 
                AirportID, AirportName, AmateurBuilt, NumberOfEngines, Scheduled, PurposeOfFlight, FAR, 
                AirCraftDamage, WeatherCondition, Operator, ReportStatus, RepGenFlag, DocketUrl, 
                DocketPublishDate, Unnamed_37
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (NtsbNo) 
            DO UPDATE SET 
                EventType = EXCLUDED.EventType,
                Mkey = EXCLUDED.Mkey,
                EventDate = EXCLUDED.EventDate,
                City = EXCLUDED.City,
                State = EXCLUDED.State,
                Country = EXCLUDED.Country,
                ReportNo = EXCLUDED.ReportNo,
                N = EXCLUDED.N,
                HasSafetyRec = EXCLUDED.HasSafetyRec,
                ReportType = EXCLUDED.ReportType,
                OriginalPublishDate = EXCLUDED.OriginalPublishDate,
                HighestInjuryLevel = EXCLUDED.HighestInjuryLevel,
                FatalInjuryCount = EXCLUDED.FatalInjuryCount,
                SeriousInjuryCount = EXCLUDED.SeriousInjuryCount,
                MinorInjuryCount = EXCLUDED.MinorInjuryCount,
                ProbableCause = EXCLUDED.ProbableCause,
                EventID = EXCLUDED.EventID,
                Latitude = EXCLUDED.Latitude,
                Longitude = EXCLUDED.Longitude,
                Make = EXCLUDED.Make,
                Model = EXCLUDED.Model,
                AirCraftCategory = EXCLUDED.AirCraftCategory,
                AirportID = EXCLUDED.AirportID,
                AirportName = EXCLUDED.AirportName,
                AmateurBuilt = EXCLUDED.AmateurBuilt,
                NumberOfEngines = EXCLUDED.NumberOfEngines,
                Scheduled = EXCLUDED.Scheduled,
                PurposeOfFlight = EXCLUDED.PurposeOfFlight,
                FAR = EXCLUDED.FAR,
                AirCraftDamage = EXCLUDED.AirCraftDamage,
                WeatherCondition = EXCLUDED.WeatherCondition,
                Operator = EXCLUDED.Operator,
                ReportStatus = EXCLUDED.ReportStatus,
                RepGenFlag = EXCLUDED.RepGenFlag,
                DocketUrl = EXCLUDED.DocketUrl,
                DocketPublishDate = EXCLUDED.DocketPublishDate,
                Unnamed_37 = EXCLUDED.Unnamed_37;
        """, tuple(row))

###Main###
def main():
    #Define PostgreSQL connection parameters
    db_params = {
        'dbname': 'databasename',
        'user': 'postgres',
        'password': 'password',
        'host': 'localhost',
        'port': '5432'
    }

    #Connect to the DB
    conn = connect_to_db(db_params)
    cursor = conn.cursor()

    try:
        #Create table if not exists
        create_table(cursor)

        #Fetch NTSB data
        ntsb_data = fetch_ntsb_data()

        #Insert or update data into the database
        insert_data(cursor, ntsb_data)

        #Commit changes
        conn.commit()

    finally:
        #Close database connection
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
