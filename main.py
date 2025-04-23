import pandas as pd
import plotly.express as px
import weather_event_system_linker as we
import pvdrdb_tools as pvdrdb
import glob as glob
import os


# Distances are in KM!!!
weather_distance_config = {'Flood': 30,
                            'Coastal Flood': 30,
                            'Flash Flood': 30,
                            'Heavy Rain': 10,
                            'Waterspout': 10,
                            'Tornado': 5,
                            'Thunderstorm Wind': 20,
                            'Marine Thunderstorm Wind': 20,
                            'Marine Strong Wind': 20,
                            'High Wind': 20,
                            'Strong Wind': 20,
                            'Dust Devil': 10,
                            'Marine High Wind': 20,
                            'Funnel Cloud': 10,
                            'Marine Hail': 10,
                            'Hail': 10,
                            "Sleet": 10,
                            'Lightning': 10,
                            'Marine Lightning': 10,
                            'Debris Flow': 10,
                            'Wildfire': 50,
                            "Volcanic Ash": 10,
                            "Dense Smoke": 50,
                            'Heat': 50,
                            'Excessive Heat': 50,
                            "Extreme Cold/Wind Chill": 30,
                            'Lake-Effect Snow': 30,
                            'Winter Storm': 30,
                            'Winter Weather': 30,
                            "Ice Storm": 30,
                            "Cold/Wind Chill": 30,
                            "Blizzard": 30,
                            'Heavy Snow': 30,
                            "Frost/Freeze": 30,
                            "Hurricane": 150,
                            "Marine Hurricane/Typhoon": 150,
                            "Hurricane (Typhoon)": 150,
                            "Tropical Depression": 150,
                            "Marine Tropical Storm": 150,
                            "Marine Tropical Depression": 150,
                            'Tropical Storm': 150,
                            "Tsunami": 10,
                           }


system_metadata_file = "./metadata/pv_system_metadata.csv"
LINK_DATA = True
WRITE_CSV_RESULTS = True
GENERATE_PLOTS = True
# 'PV' or 'wind'
data_type='PV'

if __name__ == "__main__":
    # Read in the associated system metadata
    system_metadata = pd.read_csv(system_metadata_file)
    # Connect to database
    db = pvdrdb.PVDRDBQuery()
    db.connectToDB()
    # Initialize System Linker class
    sys_linker = we.SystemLinker(db, system_metadata, weather_distance_config)
    if LINK_DATA:
        system_weather_event_master = sys_linker.linkData()
    if WRITE_CSV_RESULTS:
        system_weather_event_master.to_csv(
            "system_weather_event_master.csv", index=False)
    system_weather_event_master = pd.read_csv("system_weather_event_master.csv",
                                              parse_dates=True)
    ##### PLOT GENERATOR (HOOKED INTO S3) ######
    if GENERATE_PLOTS:
        if data_type=='wind':
            # Get all of the system information
            wind_sys = system_weather_event_master[[
                'NREL_grouping', 'asset_name']].drop_duplicates()
            master_agg_df = pd.DataFrame()
            for idx, row in wind_sys.iterrows():
                # Pull the associated turbine data and plot it.
                try:
                    df = pd.read_csv(os.path.join(
                            "s3://pvdrdb-analysis/REGROW_Wind_Data/SCADA_reformatted/",
                            str(row['NREL_grouping']) + "_Turbine_" +
                            str(row['asset_name']) + ".csv"), index_col=0,
                            storage_options={"key": db.aws['key'],
                                             "secret": db.aws['secret']})
                    df.index = pd.to_datetime(df.index, utc=True, )
                    df.index = df.index.tz_convert('America/Chicago')
                    df = df[df.index.notnull()]
                    weather_df_sub = system_weather_event_master[
                        (system_weather_event_master['asset_name'] ==
                         row['asset_name']) &
                        (system_weather_event_master['NREL_grouping'] ==
                         row['NREL_grouping'])]
                    # sys_linker.generatePlotlyGraphic(data_type = 'wind',
                    #                                  system_ac_power_data=df['Power'], 
                    #                                  weather_events = weather_df_sub,
                    #                                  ac_power_units= 'kW',
                    #                                  subsystem_name = row['asset_name'])
                    # Run the percent deviation from median
                    agg_df = sys_linker.examineTurbinePerformance(
                        system_ac_power_data=df['Power'],
                        weather_events = weather_df_sub)
                    master_agg_df = pd.concat([master_agg_df, agg_df])
                except Exception as e:
                    print(e)
        if data_type == 'PV':
            pv_systems = list(system_weather_event_master[
                'system_id'].drop_duplicates().astype(int))
            master_agg_df = pd.DataFrame()
            logger_issue = list()
            for system_id in pv_systems:
                try:
                    # read in the data from the associated S3 bucket
                    df = pd.read_csv(os.path.join(
                            "s3://pvdrdb-inbox/Analysis_input/PVDRDB/",
                            str(system_id) + ".csv"), index_col=0, parse_dates=True,
                            storage_options={"key": db.aws['key'],
                                             "secret": db.aws['secret']})
                    # Resample data to hourly frequency
                    df = df.resample('60min').mean()
                    # Get all of the associated AC power streams to plot
                    ac_power_streams = [x for x in list(df.columns) if 'ac_power' in x]
                    weather_df_sub = system_weather_event_master[
                        system_weather_event_master['system_id'] == system_id]
                    agg_df = sys_linker.examinePVPerformance(
                        system_ac_power_data=df[ac_power_streams],
                        weather_events = weather_df_sub)
                    master_agg_df = pd.concat([master_agg_df, agg_df])
                    master_agg_df.to_csv("system_weather_event_master_performance.csv")
                    sys_linker.generatePlotlyGraphic(data_type = 'PV',
                                                      system_ac_power_data=df[ac_power_streams], 
                                                      weather_events=weather_df_sub,
                                                      ac_power_units='kW',
                                                      subsystem_name=str(system_id))
                except:
                    logger_issue.append(system_id)
                    
            
