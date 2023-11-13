import logging


def csv_to_dataframe(spark, csv_path, dataframe_name):
    """
    Read CSV data into a PySpark DataFrame.

    Parameters:
    - spark: SparkSession
    - csv_path: str, path to the CSV file
    - dataframe_name: str, name to assign to the DataFrame

    Returns:
    - PySpark DataFrame or None if an error occurs
    """
    try:
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        return df
    except Exception as e:
        logging.error(f"Error occurred while reading CSV: {str(e)}")
        return None
