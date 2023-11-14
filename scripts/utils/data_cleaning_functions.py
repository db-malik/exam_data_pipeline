from pyspark.sql.functions import count, when, col,  mean



def fill_missing_with_mean(dataframe, column_name):
    """
    Fill missing values in a specific column with the mean value.
    """
    # Calculate the mean of the specified column
    mean_value = dataframe.select(mean(col(column_name))).collect()[0][0]

    # Fill missing values with the calculated mean
    filled_dataframe = dataframe.withColumn(column_name, when(col(column_name).isNull(), mean_value).otherwise(col(column_name)))

    return filled_dataframe

# drop duplicates
def drop_duplicates_and_describe(dataframe):
    """
    Drop duplicate rows from a DataFrame and show a description of the deduplicated DataFrame.
    """
    # Drop duplicate rows
    deduplicated_dataframe = dataframe.dropDuplicates()
    
    # Show description of the deduplicated DataFrame
    deduplicated_dataframe.describe().show()

    return deduplicated_dataframe

# check for nulls
def check_for_nulls(dataframe):
    """
    Check for null values in each column of a DataFrame and show the description.
    """
    # Check for null values
    null_counts = dataframe.select([col(c).alias(c) for c in dataframe.columns]).agg(*[(count(when(col(c).isNull(), c)).alias(c)) for c in dataframe.columns])
    
    # Show the count of null values
    print('Number of nulls')
    null_counts.show()

    return null_counts

