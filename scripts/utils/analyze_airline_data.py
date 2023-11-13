
from pyspark.sql.functions import col

def delays_over_time_by_airline(data, airline_name):
    # Filtrer les données pour la compagnie aérienne spécifiée
    airline_data = data.filter(col("Carrier") == airline_name)

    # Analyse des retards de départ et d'arrivée au fil du temps
    delays_over_time = airline_data.groupBy("DayofMonth").agg({"DepDelay": "avg", "ArrDelay": "avg"}).orderBy("DayofMonth")

    return delays_over_time, airline_name




def average_delay_by_airport(data, airline_name):
    # Filtrer les données pour la compagnie aérienne spécifiée
    airline_data = data.filter(col("Carrier") == airline_name)
    
    # Calculer la moyenne des retards à l'arrivée et au départ par aéroport
    average_delay_per_airport = airline_data.groupBy("DestAirportID",  "dest_name").agg(
        {"ArrDelay": "avg", "DepDelay": "avg"}
    ).withColumnRenamed("avg(ArrDelay)", "AverageArrivalDelay").withColumnRenamed("avg(DepDelay)", "AverageDepartureDelay") \
        .withColumnRenamed("DestAirportID", "AirportID").withColumnRenamed("dest_name", "AirportName")

    return average_delay_per_airport, airline_name


