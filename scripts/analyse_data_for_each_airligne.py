from pyspark.sql.functions import col
from utils import csv_to_dataframe, average_delay_by_airport, delays_over_time_by_airline, average_delay_by_airport, save_to_csv
from pyspark import SparkContext
from pyspark.sql import SparkSession
import logging
logging.basicConfig(level=logging.ERROR)


absolute_path = '/home/malek/airflow/'

# Specify absolute paths to the CSV files
paths_to_merged_data = f"{absolute_path}merged_data/merged_flights_data.csv"


path_to_export_folder = f"{absolute_path}analysis_data/airlignes/"

# Create a Spark session and Initialize Spark
sc = SparkContext("local", "dataF_frame_titanic")
sc.setLogLevel('ERROR')
spark = SparkSession.builder.appName("DataIngestion").getOrCreate()



    

try:
    
    # Read CSV data into PySpark DataFrames using the function from utils
    merged_flights_data = csv_to_dataframe(spark, paths_to_merged_data, "merged_flights_data")

    # Collecter la liste unique des compagnies aériennes
    airlines = merged_flights_data.select("Carrier").distinct().rdd.flatMap(lambda x: x).collect()

    # Appliquer la fonction pour chaque compagnie aérienne
    for airline in airlines:

        average_delay_by_airport_df, airline = average_delay_by_airport(merged_flights_data, airline)
        save_to_csv(average_delay_by_airport_df, f'{path_to_export_folder}/{airline}', f"average_delay_by_destination_airport_for_{airline}")


        delays_over_time_by_airline_df, airline =  delays_over_time_by_airline(merged_flights_data, airline)
        save_to_csv(delays_over_time_by_airline_df, f'{path_to_export_folder}/{airline}', f"delays_over_time_for_{airline}_airline")
      

except Exception as e:
        logging.error(f"Error occurred: {str(e)}")



