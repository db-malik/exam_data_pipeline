
from pyspark.sql.functions import mean

def impute_mean(dataframe, column):
    """
    Impute missing values in a numerical column with the mean.
    
    Parameters:
    - dataframe: The input DataFrame.
    - column: The column in which missing values are to be imputed.
    
    Returns:
    - The DataFrame with missing values in the specified column imputed with the mean.
    """
    mean_value = dataframe.select(mean(column)).collect()[0][0]
    return dataframe.na.fill(mean_value, subset=[column])


