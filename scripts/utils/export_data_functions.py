from datetime import datetime
import os

def save_to_csv(dataframe, output_path, filename):
    """
    Save a PySpark DataFrame to a CSV file.

    Parameters:
    - dataframe: PySpark DataFrame
    - output_path: str, path to save the CSV file
    - filename: str, the name of the CSV file (without extension)

    Returns:
    - None
    """
    if(filename == 'merged_flights_data'):
        # Construct the full path with the filename and extension
        full_path = f'{output_path}/{filename}.csv'
    else:

        # Get the current date in the desired format
        current_datetime = datetime.now().strftime("%d_%b_%Y_%H_%M")

        # Construct the full path with the filename and extension
        full_path = f'{output_path}/{current_datetime}_{filename}.csv'

    try:
        # Create the output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)

        # Convert PySpark DataFrame to Pandas DataFrame and write to a single CSV file
        dataframe.toPandas().to_csv(full_path, index=False)

        print(f"DataFrame saved to {full_path}")
    except Exception as e:
        print(f"Error saving DataFrame: {e}")
