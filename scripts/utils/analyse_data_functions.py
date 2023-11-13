from pyspark.sql.functions import  col, avg, count, date_format, hour
from pyspark.sql.functions import count

def average_delay_per_airport(data):
    """
    Calculate and compare the average arrival delay for each airport.

    Parameters:
    - data: PySpark DataFrame, containing flight data

    Returns:
    - PySpark DataFrame with columns: 'origin_airport_id', 'origin_city', 'origin_state', 'origin_name', 'average_arrival_delay'
    """
    
   # Calculate average departure delay per airport
    avg_dep_delay = data.groupBy("OriginAirportID", "origin_name").agg(avg("DepDelay").alias("average_DepDelay"))

    # Calculate average arrival delay per airport
    avg_arr_delay = data.groupBy("OriginAirportID", "origin_name").agg(avg("ArrDelay").alias("average_ArrDelay"))

    # Join the two DataFrames on OriginAirportID
    result_df = avg_dep_delay.join(avg_arr_delay, on=["OriginAirportID", "origin_name"], how="inner")

    # Rename columns and order the result
    result_df = result_df.select("origin_name", "average_DepDelay", "average_ArrDelay").orderBy("origin_name")
    
    return result_df



def flight_count_per_carrier(data):
    # Calculate the number of flights per carrier
    flight_count_df = data.groupBy("Carrier").agg(count("*").alias("FlightCount"))

    return flight_count_df.orderBy("FlightCount", ascending=False)





def route_analysis(data):
    """
    Perform route analysis on the given flight data.

    Parameters:
    - data: PySpark DataFrame containing flight data.

    Returns:
    - PySpark DataFrame with route analysis results.
    """
   

    # Group by origin and destination airports
    route_analysis_df = data.groupBy("OriginAirportID", "DestAirportID") \
        .agg(avg("DepDelay").alias("AvgDepDelay"), avg("ArrDelay").alias("AvgArrDelay"), count("*").alias("FlightCount"))

    # Join with airport data to get city and state information
    route_analysis_df = route_analysis_df.join(
        data.select("OriginAirportID", "origin_city", "origin_state", "origin_name").distinct(),
        "OriginAirportID", "left_outer"
    ).join(
        data.select("DestAirportID", "dest_city", "dest_state", "dest_name").distinct(),
        "DestAirportID", "left_outer"
    )

    # Select relevant columns
    route_analysis_df = route_analysis_df.select(
        "origin_city", "origin_state", "origin_name",
        "dest_city", "dest_state", "dest_name",
        "AvgDepDelay", "AvgArrDelay", "FlightCount"
    )

    return route_analysis_df




def average_flights_per_day_of_week(flights_data):
    """
    Calculate the average number of flights for each day of the week.

    Parameters:
    - flights_data: PySpark DataFrame containing flight data

    Returns:
    - Analysis results as a PySpark DataFrame
    """
    # Group by DayOfWeek and calculate the average number of flights
    average_flights_per_weekday = flights_data.groupBy("DayOfWeek") \
                                  .agg({"DayOfWeek": "count"}) \
                                  .orderBy("DayOfWeek") \
                                  .withColumnRenamed("count(DayOfWeek)", "AverageFlights")

    return average_flights_per_weekday



def average_flights_per_day_of_month(flights_data):
    """
    Calculate the average number of flights for each day of the month.

    Parameters:
    - flights_data: PySpark DataFrame containing flight data

    Returns:
    - Analysis results as a PySpark DataFrame
    """
    # Group by DayofMonth and calculate the average number of flights
    average_flights_per_day_of_month = flights_data.groupBy("DayofMonth") \
                                  .agg({"DayofMonth": "count"}) \
                                  .orderBy("DayofMonth") \
                                  .withColumnRenamed("count(DayofMonth)", "AverageFlights")

    return average_flights_per_day_of_month

