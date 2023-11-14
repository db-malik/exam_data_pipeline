import logging
from utils  import csv_to_dataframe, fill_missing_with_mean, drop_duplicates_and_describe, check_for_nulls, impute_mean, merge_flights_with_airports, average_delay_per_airport,flight_count_per_carrier,route_analysis,average_flights_per_day_of_week, average_flights_per_day_of_month, save_to_csv
from pyspark.sql.functions import col

from pyspark import SparkContext
from pyspark.sql import SparkSession

import sys
sys.path.append('/home/malek/airflow/')

# Configure logging to only show error messages
logging.basicConfig(level=logging.ERROR)


absolute_path = '/home/malek/airflow/'


# Specify absolute paths to the CSV files
airports_csv_path = f"{absolute_path}data/airports.csv"
flights_csv_path = f"{absolute_path}data/flights.csv"
raw_flights_data = f"{absolute_path}data/flights.csv"

      
# Create a Spark session
# Initialize Spark
sc = SparkContext("local", "dataF_frame_titanic")
sc.setLogLevel('ERROR')
spark = SparkSession.builder.appName("DataIngestion").getOrCreate()


# def data_loading(absolute_paths, spark):
   
try:
        # Read CSV data into PySpark DataFrames using the function from utils
        airports_data = csv_to_dataframe(spark, airports_csv_path, "airports_dataframe")
        flights_data = csv_to_dataframe(spark, flights_csv_path, "flights_dataframe")
        raw_flights_data = csv_to_dataframe(spark, raw_flights_data, "raw_flights_data")

        # log a sample of the ingested data and description
        print("Airports Data:")
        airports_data.show()

        print("Aireport Data descriptions:")
        airports_data.describe().show()

        print("Flights Data:")
        flights_data.show()

        print("Flight Data descriptions: ")
        flights_data.describe().show()

        print("Raw Flight Data : ")
        raw_flights_data.show()

        print("Raw Flight Data descriptions:")
        raw_flights_data.describe().show()
      

        # calculate a briaf summary of input data (airports raw flight and flight)
        flights_data_summary = flights_data.summary()
        airports_data_summary = airports_data.summary()
        raw_flights_data_summary  = raw_flights_data.summary()
        flights_data_sample = flights_data.limit(10)
        airports_data_sample = airports_data.limit(10)
        raw_flights_data_sample =  raw_flights_data.limit(10)
        

        # Export input data analysis to csv files  
        save_to_csv(flights_data_summary, f"{absolute_path}analysis_input_data", "flights_data_summary")
        save_to_csv(airports_data_summary, f"{absolute_path}analysis_input_data", "airports_data_summary")
        save_to_csv(raw_flights_data, f"{absolute_path}analysis_input_data", "airports_data_summary")
        save_to_csv(airports_data_sample, f"{absolute_path}analysis_input_data", "airports_data_sample")
        save_to_csv(flights_data_sample, f"{absolute_path}analysis_input_data", "flights_data_sample")
        save_to_csv(raw_flights_data_sample, f"{absolute_path}analysis_input_data", "raw_flights_data_sample")


        # Check missing values in raw_flight_data
        flight_data_without_nulls = fill_missing_with_mean(flights_data, "DepDelay")
        flight_data_without_nulls = fill_missing_with_mean(flights_data, "ArrDelay")
        raw_flight_data_without_nulls = fill_missing_with_mean(raw_flights_data, "DepDelay")
        raw_flight_data_without_nulls = fill_missing_with_mean(raw_flights_data, "ArrDelay")

        
        # Join raw flights and flight using the common column
        full_flights_data = flights_data.union(raw_flights_data)
        
        full_flights_data_summary = full_flights_data.summary()

        save_to_csv(full_flights_data_summary, f"{absolute_path}analysis_input_data", "full_flights_data_summary")

        # Remove duplicates from flight_data and log in terminal
        full_flights_data_unduplicated = drop_duplicates_and_describe(full_flights_data)
        full_flights_data_unduplicated_summary = full_flights_data.summary()
        

        save_to_csv(full_flights_data_unduplicated_summary, f"{absolute_path}analysis_input_data", "full_flights_data_unduplicated_summary")

       

        # ---------------------- merge data begins  --------------------------------------------

      
        # merged_data = merge_flights_with_airports(flights_data, airports_data)
        merged_data = flights_data.join(airports_data.withColumnRenamed("airport_id", "origin_airport_id"),
                                flights_data["OriginAirportID"] == col("origin_airport_id"),
                                "left_outer")
        

        # Rename columns
        merged_data = merged_data.withColumnRenamed("city", "origin_city") \
                         .withColumnRenamed("state", "origin_state") \
                         .withColumnRenamed("name", "origin_name")
        
        # Drop the duplicate columns resulting from the join
        merged_data = merged_data.drop("origin_airport_id")
        
        # # Join DataFrames on the respective columns for destination airport
        merged_data = merged_data.join(airports_data.withColumnRenamed("airport_id", "dest_airport_id"),
                               merged_data["DestAirportID"] == col("dest_airport_id"),
                               "left_outer")

        # # Rename columns for destination airport
        merged_data = merged_data.withColumnRenamed("city", "dest_city") \
                         .withColumnRenamed("state", "dest_state") \
                         .withColumnRenamed("name", "dest_name")
        


        
        # Drop the duplicate columns resulting from the join
        merged_data = merged_data.drop("dest_airport_id")



        # ---------------------- merge data ends --------------------------------------------


        merged_data.show()
        merged_data.describe().show()
        merged_data_sample  = merged_data.limit(10)
        # save merged_data  in csv file
        save_to_csv(merged_data_sample, f"{absolute_path}merged_data", "merged_flights_data_sample")
        save_to_csv(merged_data, f"{absolute_path}merged_data", "merged_flights_data")



    
except Exception as e:
        logging.error(f"Error occurred: {str(e)}")




