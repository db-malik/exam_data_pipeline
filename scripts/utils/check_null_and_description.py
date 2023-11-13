# from pyspark.sql.functions import col

# from spark.python.pyspark.sql.functions import count, when

# def check_for_nulls(dataframe):
#     """
#     Check for null values in each column of a DataFrame and show the description.
#     """
#     # Check for null values
#     null_counts = dataframe.select([col(c).alias(c) for c in dataframe.columns]).agg(*[(count(when(col(c).isNull(), c)).alias(c)) for c in dataframe.columns])
    
#     # Show the count of null values
#     print('Number of nulls')
#     null_counts.show()

