from pyspark.sql.functions import count, when, col


def check_missing_values(dataframe):
    """
    Check and display the count of missing values in each column of a DataFrame.
    """
    return dataframe.select([count(when(col(c).isNull(), c)).alias(c) for c in dataframe.columns])


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

