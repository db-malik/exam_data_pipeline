from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark import SparkContext
from pyspark.sql import SparkSession
from utils import csv_to_dataframe
from spark.python.pyspark.sql.connect.plan import Limit
import logging
from scripts.utils.export_data_functions import save_to_csv

absolute_path = '/home/malek/airflow/'

# Specify absolute paths to the CSV files
paths_to_merged_data = f"{absolute_path}merged_data/merged_flights_data.csv"
print( paths_to_merged_data)   
path_to_export_folder = f"{absolute_path}a_predections"

# Create a Spark session and Initialize Spark
sc = SparkContext("local", "dataF_frame_titanic")
sc.setLogLevel('ERROR')
spark = SparkSession.builder.appName("DataIngestion").getOrCreate()



# Load the data
merged_flights_data = csv_to_dataframe(spark, paths_to_merged_data, "merged_flights_data")

# Create a target column (1 for delayed, 0 for not delayed)
df = merged_flights_data.withColumn("target", (merged_flights_data["ArrDelay"] > 0).cast("integer"))

# Display the first 10 rows of the DataFrame
df.show(10)

# Create a VectorAssembler to assemble the features
feature_cols = ["DayofMonth", "DayOfWeek", "DepDelay", "OriginAirportID", "DestAirportID"]
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
# Display the first 10 rows of the DataFrame


# Create a StringIndexer to encode the "Carrier" column
carrier_indexer = StringIndexer(inputCol="Carrier", outputCol="CarrierIndex")

# Create a OneHotEncoder to encode the "CarrierIndex" column
carrier_encoder = OneHotEncoder(inputCol="CarrierIndex", outputCol="CarrierVec")

# Create a logistic regression model
lr = LogisticRegression(featuresCol="features", labelCol="target")

# Create a pipeline with the steps
pipeline = Pipeline(stages=[vector_assembler, carrier_indexer, carrier_encoder, lr])

# Create a pipeline with the steps
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# Train the model
model = pipeline.fit(train_data)

# Make predictions on the test set
predictions = model.transform(test_data)
predictions.collect()

logging.info(predictions.show())

save_to_csv(predictions, path_to_export_folder, 'predections')


# Evaluate the model
evaluator = BinaryClassificationEvaluator(labelCol="target", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)

logging.info(f"Area under ROC curve: {auc}")

