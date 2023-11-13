from pyspark.sql.functions import col

def merge_flights_with_airports(flights_data, airports_data):
    """
    Merge flights_data and airports_data, and rename columns for origin and destination.
    """
    # Rename airport columns in airports_data to avoid ambiguity after the merge
    airports_data = airports_data \
        .withColumnRenamed("airport_id", "origin_airport_id") \
        .withColumnRenamed("city", "origin_city") \
        .withColumnRenamed("state", "origin_state") \
        .withColumnRenamed("name", "origin_name")

    # Join flights_data with airports_data for origin airport
    merged_data = flights_data \
        .join(airports_data, flights_data["OriginAirportID"] == airports_data["origin_airport_id"], "left") \
        .drop("origin_airport_id")

    # Rename destination columns in airports_data to avoid ambiguity after the merge
    airports_data = airports_data \
        .withColumnRenamed("airport_id", "dest_airport_id") \
        .withColumnRenamed("city", "dest_city") \
        .withColumnRenamed("state", "dest_state") \
        .withColumnRenamed("name", "dest_name")

    # Join merged_data with airports_data for destination airport
    merged_data = merged_data \
        .join(airports_data, merged_data["DestAirportID"] == airports_data["dest_airport_id"], "left") \
        .drop("dest_airport_id")

    # Rename columns to distinguish between origin and destination
    merged_data = merged_data \
        .withColumnRenamed("origin_city", "origin_city") \
        .withColumnRenamed("origin_state", "origin_state") \
        .withColumnRenamed("origin_name", "origin_name") \
        .withColumnRenamed("dest_city", "dest_city") \
        .withColumnRenamed("dest_state", "dest_state") \
        .withColumnRenamed("dest_name", "dest_name")

    return merged_data
