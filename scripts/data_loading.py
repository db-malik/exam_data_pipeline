import logging
from utils  import csv_to_dataframe, check_missing_values, drop_duplicates_and_describe, check_for_nulls, impute_mean, merge_flights_with_airports, average_delay_per_airport,flight_count_per_carrier,route_analysis,average_flights_per_day_of_week, average_flights_per_day_of_month, save_to_csv
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

        # log a sample of the ingested data and description
        print("Airports Data:")
        airports_data.show()

        print("Flights Data:")
        flights_data.show()

        print("Flight Data descriptions:")
        flights_data.describe().show()

        print("Aireport Data descriptions:")
        airports_data.describe().show()
      

        # calculte a briaf summary of input data (airports and flight)
        flights_data_summary = flights_data.summary()
        airports_data_summary = airports_data.summary()
        flights_data_sample = flights_data.limit(10)
        airports_data_sample = airports_data.limit(10)
        

        # log the input data analysis 
        print('flights data summary : ')
        print(flights_data_summary)

        print('airports data summary : ')
        print(airports_data_summary)

        print('flights data sample : ')
        print(flights_data_sample)

        print('airports data sample : ')
        print(airports_data_sample)


        # Export input data analysis to csv files  
        save_to_csv(flights_data_summary, f"{absolute_path}analysis_input_data", "flights_data_summary")
        save_to_csv(flights_data_sample, f"{absolute_path}analysis_input_data", "flights_data_sample")
        save_to_csv(airports_data_summary, f"{absolute_path}analysis_input_data", "airports_data_summary")
        save_to_csv(airports_data_sample, f"{absolute_path}analysis_input_data", "airports_data_sample")


        # Check missing values in raw_flight_data
        missing_values_flight_data = check_missing_values(flights_data)

        # log missing values 
        print('missing values flight data : ')
        missing_values_flight_data.show()

        # save missing valus in csv file
        save_to_csv(missing_values_flight_data, f"{absolute_path}analysis_input_data", "missing_values_flight_data")


        # Remove duplicates from flight_data and log in terminal
        deduplicated_flights_data = drop_duplicates_and_describe(flights_data)

        print('deduplicated_flights_data : ')
        deduplicated_flights_data.show()
        # check for nulls and log the total number of nulls
        check_for_nulls(flights_data)


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




