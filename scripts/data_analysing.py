import logging
from utils import csv_to_dataframe, average_delay_per_airport,flight_count_per_carrier,route_analysis,average_flights_per_day_of_week, average_flights_per_day_of_month, save_to_csv
from utils.analyse_data_functions import route_analysis
from pyspark import SparkContext
from pyspark.sql import SparkSession

# Configure logging to only show error messages
logging.basicConfig(level=logging.ERROR)


absolute_path = '/home/malek/airflow/'

# Specify absolute paths to the CSV files
paths_to_merged_data = f"{absolute_path}merged_data/merged_flights_data.csv"
print( paths_to_merged_data)   
path_to_export_folder = f"{absolute_path}analysis_data"

# Create a Spark session and Initialize Spark
sc = SparkContext("local", "dataF_frame_titanic")
sc.setLogLevel('ERROR')
spark = SparkSession.builder.appName("DataIngestion").getOrCreate()
   
   
    
try:
        
        # Read CSV data into PySpark DataFrames using the function from utils
        merged_flights_data = csv_to_dataframe(spark, paths_to_merged_data, "merged_flights_data")
        # Analysing data and save reports to csv files  

        # Analyze and save average delay per airport
        average_delay = average_delay_per_airport(merged_flights_data)
        save_to_csv(average_delay, path_to_export_folder, 'average_delay_by_airoport')

        # Analyze and save flight count per airline carrier
        flight_count = flight_count_per_carrier(merged_flights_data)
        save_to_csv(flight_count, path_to_export_folder, 'flights_per_airline_carrier')

        # Analyze and save route analysis
        route_analysis_df = route_analysis(merged_flights_data)
        save_to_csv(route_analysis_df, path_to_export_folder, 'route_analysis')

        # Analyze and save average flights per day of the week
        flights_per_day_of_week_df = average_flights_per_day_of_week(merged_flights_data)
        save_to_csv(flights_per_day_of_week_df, path_to_export_folder, 'flights_per_day_of_week' )

        # Analyze and save average flights per day of the month
        flights_per_day_of_month_df = average_flights_per_day_of_month(merged_flights_data)
        save_to_csv(flights_per_day_of_month_df, path_to_export_folder, 'flights_per_day_of_month' )


except Exception as e:
        logging.error(f"Error occurred: {str(e)}")


