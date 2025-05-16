"""
System linker class for extreme weather.
"""

import pandas as pd
import geopy.distance
import plotly.express as px
from datetime import timedelta


sub_event_type_df = pd.read_csv("./master-weather-category.csv")


class SystemLinker():

    def __init__(self, db, system_metadata, weather_distance_config):
        self.db = db
        self.system_metadata = system_metadata
        # Pull the associated weather data from the database.
        self.pullWeatherData()
        self.weather_distance_config = weather_distance_config
        # Subset the data to only include weather event types that
        # we care about (in the config dictionary)
        self.subsetWeatherData()

    def pullWeatherData(self):
        """
        Pull down the weather data from the associated PVDRDB table, for
        joining with the associated system data.

        Returns
        -------
        None.

        """
        sql = "select * from pvdrdb.weather_events"
        self.db.dbops.execute(sql)
        self.db.dbconn.commit()
        self.weather_df = pd.DataFrame(self.db.dbops.fetchall())
        self.weather_df.columns = [desc[0] for desc in
                                   self.db.dbops.description]
        return

    def subsetWeatherData(self):
        """
        Subset the weather events based on what's in the weather distance
        configuration (pull out all of the events we don't care about).

        Returns
        -------
        None.

        """
        weather_events = list(self.weather_distance_config.keys())
        self.weather_df = self.weather_df[self.weather_df['event_type'].isin(
            weather_events)]
        return

    def linkData(self):
        """
        Link the data sets for systems and extreme weather.

        Parameters
        ----------
        None.

        Returns
        -------
        system_weather_events: Pandas DataFrame
            Pandas dataframe containing the data sets for systems that are
            present during extreme weather events.

        Notes
        -----
        system_metadata_df must contain columns 'latitude', 'longitude',
        'started_on', 'ended_on'.
        """
        # Select the max degree difference needed based on the max distance
        # in the weather event config
        max_deg_distance = max(self.weather_distance_config.values()) / 111
        system_weather_event_master = pd.DataFrame()
        # Get cases where systems are near weather events
        for index, row in self.system_metadata.iterrows():
            latitude = row['latitude']
            longitude = row['longitude']
            try:
                started_on = pd.to_datetime(
                    row['started_on'], format="%m/%d/%Y %H:%M").date()
                ended_on = pd.to_datetime(
                    row['ended_on'], format="%m/%d/%Y %H:%M").date()
            except Exception as e:
                print(e)
                continue
            # Get any weather cases within 1 decimal points of system
            weather_sub = self.weather_df[
                ((abs(self.weather_df['begin_latitude'] - latitude) <
                  max_deg_distance) &
                 (abs(self.weather_df['begin_longitude'] - longitude) <
                  max_deg_distance))
                | ((abs(self.weather_df['end_latitude'] - latitude) <
                    max_deg_distance) &
                   (abs(self.weather_df['end_longitude'] - longitude) <
                    max_deg_distance))]
            # Check if weather events occur during time series period
            weather_sub = weather_sub[(pd.to_datetime(
                weather_sub['start_timestamp']).dt.date >= started_on) &
                (pd.to_datetime(weather_sub['end_timestamp']).dt.date
                 <= ended_on)]
            # Get the min distance to the storm event
            sys_coords = (latitude, longitude)
            weather_sub['distance_to_weather_event_start_km'] = [
                geopy.distance.geodesic(sys_coords, (x, y)).km
                for x, y in zip(weather_sub['begin_latitude'],
                                weather_sub['begin_longitude'])]
            weather_sub['distance_to_weather_event_end_km'] = [
                geopy.distance.geodesic(sys_coords, (x, y)).km
                for x, y in zip(weather_sub['end_latitude'],
                                weather_sub['end_longitude'])]
            weather_sub['min_distance_to_weather_event_km'] = weather_sub[[
                'distance_to_weather_event_start_km',
                'distance_to_weather_event_end_km']].min(axis=1)
            # Remove sites where there are no matching cases
            if len(weather_sub) != 0:
                # Clean up the weather dataframe to remove any duplicates
                weather_sub = self.cleanUpWeatherData(weather_sub)
                # Filter systems within specified distance of weather event
                for event_type in self.weather_distance_config:
                    within_distance = self.weather_distance_config.get(
                        event_type)
                    weather_sub_event = weather_sub[weather_sub['event_type']
                                                    == event_type]
                    weather_sub_event = weather_sub_event[
                        weather_sub_event['min_distance_to_weather_event_km']
                        <= within_distance]
                    for x, y in row.to_dict().items():
                        weather_sub_event[x] = y
                    system_weather_event_master = pd.concat([
                        system_weather_event_master, weather_sub_event])
        system_weather_event_master = system_weather_event_master.rename(
            columns={'latitude': 'system_latitude',
                     'longitude': 'system_longitude',
                     'started_on': 'system_data_started_on',
                     'ended_on': 'system_data_ended_on',
                     })
        # Get the number of days of data before and after the extreme
        # weather event in question
        system_weather_event_master['system_data_days_before_event'] = (
            pd.to_datetime(system_weather_event_master[
                'weather_event_started_on']).dt.tz_convert(None)
            - pd.to_datetime(system_weather_event_master[
                'system_data_started_on'])).dt.days
        system_weather_event_master['system_data_days_after_event'] = (
            pd.to_datetime(system_weather_event_master['system_data_ended_on'])
            - pd.to_datetime(system_weather_event_master[
                'weather_event_ended_on']).dt.tz_convert(None)).dt.days
        return system_weather_event_master

    def generatePlotlyGraphic(self, data_type,
                              system_ac_power_data, weather_events,
                              ac_power_units, subsystem_name, day_window=14):
        """
        Generate a plotly graphic showing system power data and associated
        weather events.

        Parameters
        ----------
        data_type: Str.
            'PV' or 'wind', based on the data source being analyzed
        system_ac_power_data: Pandas Dataframe
            Pandas dataframe containing datetime index and an ac power column.
        weather_events: Pandas Dataframe
            Pandas dataframe that contains the 'event_type', 'start_timestamp',
            and 'end_timestamp' columns for systems that are present during
            the weather events.
        ac_power_units: str
            Unit of ac power for the system.
        sybsystem_name: str
            Subsystem name.
        day_window: int, default 14
            Number of days before and after a weather event.

        Returns
        -------
        None.

        """
        weather_events['weather_event_started_on'] = pd.to_datetime(weather_events[
                'weather_event_started_on'])
        weather_events['weather_event_ended_on'] = pd.to_datetime(weather_events[
                        'weather_event_ended_on'])
        min_event_start = weather_events[
            'weather_event_started_on'].min().date()
        max_event_end = weather_events[
            'weather_event_ended_on'].max().date()
        # Get before and after periods for an event
        sys_data = system_ac_power_data[
             (system_ac_power_data.index.date >= (
                 min_event_start - pd.Timedelta(days=day_window))) &
             (system_ac_power_data.index.date <= (
                 max_event_end + pd.Timedelta(days=day_window)))]
        # order by index
        sys_data = sys_data.sort_index()
        if data_type == 'wind':
            # Build the plotly graphic with the weather events
            operator_name = weather_events["operator_name"].iloc[0]
            site_name = weather_events["site_name"].iloc[0]
            title = f"{operator_name} {site_name}, {subsystem_name} AC Power"
            fig = px.line(sys_data, y=sys_data,
                          title=title).update_layout(
                              xaxis_title="Datetime",
                              yaxis_title=f"AC Power ({ac_power_units})")
        else:
            system_id = weather_events["system_id"].iloc[0]
            title = f"{system_id} AC Power"
            fig = px.line(sys_data, y=sys_data.columns,
                          title=title).update_layout(
                              xaxis_title="Datetime",
                              yaxis_title=f"AC Power ({ac_power_units})")
        # Loop through all of the events and generate the associated vrect's
        # in the plotly graphic
        for idx, row in weather_events.iterrows():
            event_start = pd.to_datetime(row['weather_event_started_on'])
            event_end = pd.to_datetime(row['weather_event_ended_on'])
            event_type = row['event_type']
            # Add lines for event start and end date
            fig.add_vrect(x0=event_start, x1=event_end,
                          fillcolor="red",
                          annotation_text=event_type,
                          opacity=0.2)
        if data_type == 'wind':
            fig.write_html(
                f"./plots/{operator_name}_{site_name}_{subsystem_name}_weather_events.html",
                full_html=False, include_plotlyjs="cdn")
        else:
            fig.write_html(
                f"./plots/{system_id}_weather_events.html",
                full_html=False, include_plotlyjs="cdn")            
        return

    def examinePVPerformance(self,system_ac_power_data, weather_events):
        """
        For PV data, look at the 24 hour period around each extreme
        weather event, and compare it to non-extreme weather periods.
        """
        pct_median_output_list = list()
        for idx, row in weather_events.iterrows():
            for column in list(system_ac_power_data.columns):
                ac_power_stream = system_ac_power_data[column]
                extreme_weather_date = pd.to_datetime(
                    row['weather_event_started_on']).date()
                sum_daily_production = ac_power_stream[
                    (ac_power_stream.index.date >= extreme_weather_date) &
                    (ac_power_stream.index.date <=
                     (extreme_weather_date+ timedelta(days=1)))].sum()
                # Look at the mean for similar dates on different years
                month_performance =  ac_power_stream[
                    ac_power_stream.index.month ==
                    extreme_weather_date.month]
                month_performance_median = month_performance.groupby(
                    month_performance.index.date).transform("sum").median()
                # Look at how the extreme weather performance compares to the
                # monthly median
                pct_median_output = sum_daily_production / month_performance_median
                # append extreme weather event to master list
                row = row.to_dict()
                row['data_stream'] = column
                row['pct_median_output'] = pct_median_output
                pct_median_output_list.append(row)
        agg_df = pd.DataFrame(pct_median_output_list)
        return agg_df

    def cleanUpWeatherData(self, weather_events):
        """
        Clean up the weather event data to prevent duplicates.
        """
        # Create a master "event" category so we're removing duplicated/similar
        # categories
        weather_events = pd.merge(weather_events, sub_event_type_df,
                                  on='event_type')
        # Filter weather events down to day/type (aggregation). This will help
        # with the number of occurrences
        weather_events['start_date'] = weather_events['start_timestamp'].dt.date
        weather_events['end_date'] = weather_events['end_timestamp'].dt.date
        # Explode list of dates
        weather_events["dates"] = weather_events.apply(lambda row: 
                                                       pd.date_range(
                                                           row["start_date"],
                                                           row["end_date"]),
                                                       axis=1)
        weather_events_exploded = weather_events.explode("dates")
        weather_events_sub = weather_events_exploded[['dates',
                                                      'weather_event_master'
                                                      ]].drop_duplicates()
        # Sort by type and date
        weather_events_sub = weather_events_sub.sort_values([
            'weather_event_master', 'dates'])
        # Isolate events over subsequent days
        weather_events_sub['day_diff'] =  (weather_events_sub['dates'] -
                                           weather_events_sub['dates'].shift(1)
                                           ).dt.days
        weather_events_sub.loc[weather_events_sub['day_diff'] != 1, 
                               'day_diff'] = 0
        weather_events_sub['weather_event_idx'] = weather_events_sub[
            'day_diff'].eq(0).cumsum().sub(1)
        # Join data back with original dataframe so we can aggregate data
        # up by its associated weather_event_idx
        weather_events_exploded_new = pd.merge(weather_events_exploded, 
                                               weather_events_sub, 
                                               on = ['dates', 'weather_event_master'])
        # Reset the date lengths based on the min and max by index
        weather_events_exploded_new['weather_event_started_on_agg'] = weather_events_exploded_new.groupby(
            "weather_event_idx")['start_timestamp'].transform("min")
        weather_events_exploded_new['weather_event_ended_on_agg'] = weather_events_exploded_new.groupby(
            "weather_event_idx")['end_timestamp'].transform("max")
        # Also take max magnitude and damage levels associated with the storm
        weather_events_exploded_new['max_magnitude'] = weather_events_exploded_new.groupby(
            "weather_event_idx")['magnitude'].transform("max")
        weather_events_exploded_new['max_damage_property'] = weather_events_exploded_new.groupby(
            "weather_event_idx")['damage_property'].transform("max")
        weather_events_exploded_new['max_damage_crops'] = weather_events_exploded_new.groupby(
            "weather_event_idx")['damage_crops'].transform("max")                
        # Take the nearest distance value for each weather index
        weather_events_exploded_new['nearest_distance'] = weather_events_exploded_new.groupby(
            "weather_event_idx")['min_distance_to_weather_event_km'].transform("min")        
        weather_events_exploded_new = weather_events_exploded_new[
            weather_events_exploded_new['min_distance_to_weather_event_km'] ==
            weather_events_exploded_new['nearest_distance']].drop_duplicates()
        # Take first occurance of weather event index        
        weather_events_exploded_new.drop_duplicates(
            subset='weather_event_idx', keep='first', inplace=True)
        # Update the columns based on the aggregated storm values
        weather_events_exploded_new['weather_event_started_on'] = \
            weather_events_exploded_new['weather_event_started_on_agg']          
        weather_events_exploded_new['weather_event_ended_on'] = \
            weather_events_exploded_new['weather_event_ended_on_agg']
        weather_events_exploded_new['magnitude'] = \
            weather_events_exploded_new['max_magnitude']            
        weather_events_exploded_new['damage_property'] = \
            weather_events_exploded_new['max_damage_property']
        weather_events_exploded_new['damage_crops'] = \
            weather_events_exploded_new['max_damage_crops']
        # Clean up the data frame
        weather_events_exploded_new = weather_events_exploded_new[[
               'weather_event_id', 'state',
               'location', 'event_type', 'begin_latitude', 'begin_longitude',
               'end_latitude', 'end_longitude', 
               'weather_event_started_on', 'weather_event_ended_on',
               'magnitude', 'magnitude_type',
               'damage_property', 'damage_crops', 'episode_narrative', 
               'comments', 'distance_to_weather_event_start_km',
               'distance_to_weather_event_end_km',
               'min_distance_to_weather_event_km',
               'weather_event_master']]
        return weather_events_exploded_new
