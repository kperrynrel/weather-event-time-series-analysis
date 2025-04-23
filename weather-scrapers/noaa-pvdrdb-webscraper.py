"""
NOAA Storm Events Database Webscraper
"""

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from time import sleep
import requests
import os
from aws_access_cred_manager import aws_keys_and_tokens
import pvdrdb_tools
import boto3
import botocore
import psycopg2
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="myApp")

google_maps_api_key ="YOUR API KEY HERE"


def generate_lat_lon(address, google_maps_api_key):
    """
    Gets the address of a latitude, longitude coordinates using Google
    Geocoding API. Please note rates for running geocoding checks here:
        https://developers.google.com/maps/billing-and-pricing/pricing

    Parameters
    -----------
    latitude: float
        Latitude coordinate of the site.
    longitude: float
        Longitude coordinate of the site.
    google_maps_api_key: string
        Google Maps API Key for geocoding a site. For further information,
        see here:
        https://developers.google.com/maps/documentation/geocoding/overview

    Returns
    -----------
    address: str
        Address of given latitude, longitude coordinates.
    """
    # return response object
    r = requests.get(
        'https://maps.googleapis.com/maps/api/geocode/json?address=' +
        str(address) + "&key=" + google_maps_api_key,
        verify=False)
    # Raise an exception if address is not successfully returned
    if r.status_code != 200:
        raise ValueError("Response status code " +
                         str(r.status_code) +
                         ": Address not pulled successfully from API.")
    data = r.json()
    basic_details = data["results"][0]['geometry']['location']
    return basic_details['lat'], basic_details['lng']


class NOAAWebScraper():

    def __init__(self, db, year_cutoff=2020,
                 url="https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/",
                 fips_file="fips_codes.csv"):
        self.url = url
        self.db = db
        self.year_cutoff = year_cutoff
        self.fips_df = pd.read_csv(fips_file)
        self.time_zone_dict = {'EST-5': 'Etc/GMT+5',
                               'CST-6': 'Etc/GMT+6',
                               'MST-7': 'Etc/GMT+7',
                               'PST-8': 'Etc/GMT+8',
                               'HST-10': 'Etc/GMT+10',
                               'AKST-9': 'Etc/GMT+9',
                               'SST-11': 'Etc/GMT+11',
                               'AST-4': 'Etc/GMT+4',
                               'GST10': 'Etc/GMT+10',
                               'PDT-7': 'Etc/GMT+7',
                               'EDT-4': 'Etc/GMT+4',
                               'CDT-5': 'Etc/GMT+5',
                               'CST': 'US/Central',
                               'MST': 'US/Mountain',
                               'EST': 'US/Eastern',
                               'PST': 'US/Pacific',
                               'UNK': 'UTC',
                               'CDT': 'US/Central',
                               'EDT': 'US/Eastern',
                               'MDT': 'US/Mountain',
                               'GMT': 'UTC',
                               'AST': 'Canada/Atlantic', 
                               'HST': 'US/Hawaii',
                               'SST': 'US/Samoa'}

    def pullAllData(self):
        url_info = requests.get(self.url).text
        soup = BeautifulSoup(url_info)
        master_dataframe = pd.DataFrame()
        for link in soup.findAll("a"):
            current_link = link.get("href")
            if (current_link.endswith('gz') &
                ("StormEvents_details" in current_link)):
                date = int(current_link.split("_")[3].replace("d", ""))
                if date >= self.year_cutoff:
                    sleep(1)
                    url_file_path = self.url + "/" + current_link
                    fn = current_link
                    print('Found CSV: ' + current_link)
                    print('Downloading %s' % current_link)
                    with open(fn, "wb") as f:
                        r = requests.get(url_file_path)
                        f.write(r.content)
                    # Read in as a pandas dataframe
                    df = pd.read_csv(fn)
                    # Append to the master dataframe
                    master_dataframe = pd.concat([master_dataframe, df], axis=0)
                    # Delete the resulting gzip attachment
                    os.remove(fn)
        return master_dataframe

    def omitExistingInserts(self, dataframe):
        """
        Drop all of the rows that are already in the database.

        Parameters
        ----------
        dataframe : TYPE
            DESCRIPTION.

        Returns
        -------
        None.
        """
        ## Pull of the weather events data via SQL
        sql = ("""select * from pvdrdb.weather_events where start_timestamp>='01-01-"""
               + str(self.year_cutoff) + "'")
        self.db.dbops.execute(sql)
        self.db.dbconn.commit()
        weather_events_df = pd.DataFrame(self.db.dbops.fetchall())
        weather_events_df.columns = [desc[0] for desc in 
                                     self.db.dbops.description]
        timestamp_cols = ['start_timestamp', 'end_timestamp']
        for col in timestamp_cols:
            weather_events_df[col] = pd.to_datetime(weather_events_df[col])
        # Omit all of the already existing entries and keep the new
        # rows for insertion.
        merge_cols = ['start_timestamp', 'end_timestamp', 
                      'location', 'event_type',
                      'begin_latitude', 'begin_longitude',
                      'end_latitude', 'end_longitude', 'episode_narrative']
        overlapping_entries = pd.merge(
            weather_events_df, dataframe[merge_cols],
            on = merge_cols, how='left', indicator=True)


    def processDataFrame(self, dataframe):
        """
        Process the storm events dataframe for insertion into PVDRDB.
        """
        dataframe['start_datetime'] = pd.to_datetime(
            dataframe['BEGIN_DATE_TIME'])
        dataframe['timezone'] = [self.time_zone_dict[x]
                                 for x in list(dataframe.CZ_TIMEZONE)]
        dataframe['start_datetime'] = [
            x.tz_localize(y, ambiguous='NaT',
                          nonexistent='shift_forward') for x, y in
                                       zip(dataframe['start_datetime'],
                                           dataframe['timezone'])]
        dataframe['end_datetime'] = pd.to_datetime(
            dataframe['END_DATE_TIME'])
        dataframe['end_datetime'] = [
            x.tz_localize(y, ambiguous='NaT',
                          nonexistent='shift_forward') for x, y in
                                     zip(dataframe['end_datetime'],
                                         dataframe['timezone'])]
        # Get the county based on the state + county FIPS ids
        dataframe['FIPS'] = [(f'{x:02}' + f'{y:03}') for x, y in
                             zip(dataframe['STATE_FIPS'],
                                 dataframe['CZ_FIPS'])]
        self.fips_df['county-level'] = [
            f'{x:05}' for x in list(self.fips_df['county-level'])]
        dataframe = pd.merge(dataframe, self.fips_df,
                             right_on='county-level',
                             left_on='FIPS', how='left')
        dataframe_cleaned = dataframe[[
            'start_datetime', 'end_datetime', 'EVENT_TYPE',
            'CZ_NAME', 'STATE', 'CZ_TIMEZONE',
            'DAMAGE_PROPERTY', 'DAMAGE_CROPS', 'MAGNITUDE',
            'MAGNITUDE_TYPE', 'BEGIN_LAT', 'BEGIN_LON',
            'END_LAT', 'END_LON',
            'EPISODE_NARRATIVE', 'EVENT_NARRATIVE', 'FIPS', 'place']]
        dataframe_cleaned.loc[dataframe_cleaned['place'].isna(), "place"] = (
            dataframe_cleaned['CZ_NAME'] + " County")
        dataframe_cleaned['address'] = (dataframe_cleaned['place'] + " " + 
                                        dataframe_cleaned['STATE'] + ", United States")
        update_dict_list = list()
        # Clean up the property and crop damage data
        for index, row in dataframe_cleaned.iterrows():
            row_dict = row.to_dict()
            damage_property = row['DAMAGE_PROPERTY']
            damage_crops = row['DAMAGE_CROPS']
            # Update the damage property column
            if str(damage_property) == 'nan':
                row_dict['DAMAGE_PROPERTY'] = None
            elif 'K' in str(damage_property):
                try:
                    row_dict['DAMAGE_PROPERTY'] = float(
                        damage_property.replace("K", "")) * 1000
                except:
                    row_dict['DAMAGE_PROPERTY'] = 1000
            elif 'M' in str(damage_property):
                try:
                    row_dict['DAMAGE_PROPERTY'] = float(
                        damage_property.replace("M", "")) * 1000000
                except:
                    row_dict['DAMAGE_PROPERTY'] = 1000000
            elif 'B' in str(damage_property):
                row_dict['DAMAGE_PROPERTY'] = float(
                    damage_property.replace("B", "")) * 1000000000
            else:
                row_dict['DAMAGE_PROPERTY'] = damage_property
            # Update the damage crops column
            if str(damage_crops) == 'nan':
                row_dict['DAMAGE_CROPS'] = None
            elif 'K' in str(damage_crops):
                try:
                    row_dict['DAMAGE_CROPS'] = float(
                        damage_crops.replace("K", "")) * 1000
                except:
                    row_dict['DAMAGE_CROPS'] = 1000
            elif 'M' in str(damage_crops):
                try:
                    row_dict['DAMAGE_CROPS'] = float(
                        damage_crops.replace("M", "")) * 1000000
                except:
                    row_dict['DAMAGE_CROPS'] = 1000000
            elif 'B' in str(damage_crops):
                row_dict['DAMAGE_CROPS'] = float(
                    damage_crops.replace("B", "")) * 1000000000
            else:
                row_dict['DAMAGE_CROPS'] = damage_crops
            update_dict_list.append(row_dict)

        dataframe_cleaned = pd.DataFrame(update_dict_list)
        dataframe_no_lat_lon = dataframe_cleaned[dataframe_cleaned[
            'BEGIN_LAT'].isna()]
        addresses_to_look_up = dataframe_no_lat_lon['address'].drop_duplicates()
        Add a latitude-longitude coordinates for cases where its missing
        (based on the general address)
        new_lat_lon_list = list()
        for address in addresses_to_look_up:
            try:
                lat, lon = generate_lat_lon(address, google_maps_api_key)
                new_lat_lon_list.append({"address": address,
                                          "latitude": lat,
                                          "longitude": lon})
            except:
                print("Couldn't find lat/lon for following address: " +
                      address)
        dataframe_lat_lon = pd.DataFrame(new_lat_lon_list)
        dataframe_cleaned = pd.merge(dataframe_cleaned, dataframe_lat_lon,
                                      on="address", 
                                      how='left')
        dataframe_cleaned.loc[~dataframe_cleaned['latitude'].isna(), 
                              "BEGIN_LAT"] = dataframe_cleaned['latitude']
        dataframe_cleaned.loc[~dataframe_cleaned['longitude'].isna(), 
                              "BEGIN_LON"] = dataframe_cleaned['longitude']
        dataframe_cleaned = dataframe_cleaned.drop(['latitude', 'longitude'],
                                                    axis=1)
        df.loc[df['END_LAT'].isna(), 'END_LAT'] = df['BEGIN_LAT']

        df.loc[df['END_LON'].isna(), 'END_LON'] = df['BEGIN_LON']
        # Start and end datetime
        dataframe_cleaned = dataframe_cleaned[
            ~dataframe_cleaned['start_datetime'].isna()]
        dataframe_cleaned = dataframe_cleaned[
            ~dataframe_cleaned['end_datetime'].isna()]
        dataframe_cleaned = dataframe_cleaned[['start_datetime',
                                               'end_datetime',
                                               'STATE',
                                               'address',
                                               'EVENT_TYPE',
                                               'BEGIN_LAT', 'BEGIN_LON',
                                               'END_LAT', 'END_LON',
                                               'MAGNITUDE',
                                               'MAGNITUDE_TYPE',
                                               'DAMAGE_PROPERTY',
                                               'DAMAGE_CROPS',
                                               'EPISODE_NARRATIVE']]
        dataframe_cleaned = dataframe_cleaned.rename(
            columns={'start_datetime': 'start_timestamp',
                     'end_datetime': 'end_timestamp',
                     'STATE': 'state',
                     'address': 'location',
                     'EVENT_TYPE': 'event_type',
                     'BEGIN_LAT': 'begin_latitude',
                     'BEGIN_LON': 'begin_longitude',
                     'END_LAT': 'end_latitude',
                     'END_LON': 'end_longitude',
                     'MAGNITUDE': 'magnitude',
                     'MAGNITUDE_TYPE': 'magnitude_type',
                     'DAMAGE_PROPERTY': 'damage_property',
                     'DAMAGE_CROPS': 'damage_crops',
                     'EPISODE_NARRATIVE': 'episode_narrative'})
        dataframe_cleaned['episode_narrative'] = dataframe_cleaned[
            'episode_narrative'].str.replace(",", "")
        dataframe_cleaned['episode_narrative'] = dataframe_cleaned[
            'episode_narrative'].str[:2048]
        dataframe_cleaned['comments'] = np.nan
        # Order the dataframe by start date
        dataframe_cleaned = dataframe_cleaned.sort_values(
            by=['start_timestamp']).reset_index(drop=True)
        dataframe_cleaned = dataframe_cleaned.replace({np.nan: 'NULL',
                                                       None: "NULL"})
        dataframe_cleaned = dataframe_cleaned.replace(",", "")
        dataframe_cleaned = dataframe_cleaned[dataframe_cleaned[
            'begin_latitude'] != "NULL"]
        return dataframe_cleaned

    
    def massInsertData(self, tableName, tmpFilename,
                       bucket_name = 'pvdrdb-inbox') :
        '''
        Insert a large number of data records into Time Series Table
        Method takes in pre-prepared data table matching CSV file
        stored in tmp directory.All values will be in this file so the
        process only has to push that to DB using PostgreSQL commands
        '''
        #Check to see if any files and particular file is here.
        s3Client = boto3.client('s3',
                                aws_access_key_id=self.db.aws['key'],
                                aws_secret_access_key=self.db.aws['secret'])
        bucketList = s3Client.list_objects_v2(Bucket=bucket_name)
        #if 'Contents' not in bucketList.keys():
            #return False
        #For S3 to AWS RedShift DB. First connect to stores with data file
        s3 = boto3.resource('s3',
                            aws_access_key_id=self.db.aws['key'],
                            aws_secret_access_key=self.db.aws['secret'])
        bucket = s3.Bucket(bucket_name)
        # Check there is data in file, or exit 
        try:
            fileSize = bucket.Object(tmpFilename).content_length
        except botocore.exceptions.ClientError as e:
            print ("ERROR: Finding bucket object file size failed: " + str(e))
            #return False
        else:    
            if fileSize <=0:
                print ("ERROR: No data in bulk CSV file to process")
                #return False
        #Process data into  AWS RedShift database
        credentials = ("access_key_id '" + self.db.aws['key'] +
                       "' secret_access_key '"  + self.db.aws['secret'] + "'")
        sql = ("COPY pvdrdb." + tableName  +
               " FROM  's3://" + bucket_name + "/" +  tmpFilename
               + "' NULL AS 'NULL' EMPTYASNULL DELIMITER ',' CSV "  + credentials +  ";")
        print (sql)
        try:
            self.db.dbops.execute(sql, tmpFilename)
        except psycopg2.Error as e:
            print(
                "ERROR: Unable to insert data into database.\n"
                + str(e.pgerror))
            #return False
        self.db.dbconn.commit()        
        return True 


if __name__ == "__main__":
    # Initialize the class
    db = pvdrdb_tools.pvdrdb_queries.PVDRDBQuery()
    db.connectToDB()
    noaa = NOAAWebScraper(db,
                          year_cutoff=2005)
    master_storm_df = noaa.pullAllData()
    # Process the data for insertion
    master_storm_df = noaa.processDataFrame(master_storm_df)
    # Feed the data into the weather_events table
    tmpFileNamePath = 's3://pvdrdb-transfer/bulkTransferFileNOAAStormEvents.csv'
    master_storm_df.to_csv(tmpFileNamePath, index=True, header=False,
                            storage_options={"key": db.aws['key'],
                                            "secret": db.aws['secret']})
    inserted_properly = noaa.massInsertData(
        tableName='weather_events',
        tmpFilename="bulkTransferFileNOAAStormEvents.csv",
        bucket_name='pvdrdb-transfer')
