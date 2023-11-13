from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark import SparkContext
from utils import csv_to_dataframe


# Create a Spark session
# Create a Spark session and Initialize Spark
sc = SparkContext("local", "dataF_frame_titanic")
spark = SparkSession.builder.appName("Mlib").getOrCreate()
sc.setLogLevel('ERROR')

# Specify absolute paths to the CSV files
absolute_path = '/home/malek/airflow/'
paths_to_merged_data = f"{absolute_path}merged_data/merged_flights_data.csv"

# Load the data
merged_flights_data = csv_to_dataframe(spark, paths_to_merged_data, "merged_flights_data")
merged_flights_data.printSchema()

# Convert labels to numerical indices
indexer = StringIndexer(inputCol="ArrDelay", outputCol="label")
indexed_data = indexer.fit(merged_flights_data).transform(merged_flights_data)

# Define the columns to use as features
feature_columns = ["DestAirportID", "OriginAirportID", "DayofMonth", "DayOfWeek", "DepDelay"]

# Create a VectorAssembler
vector_assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Create a RandomForestClassifier for multiclass classification
random_forest = RandomForestClassifier(labelCol='label', featuresCol='features', numTrees=10)

# Create a pipeline
pipeline = Pipeline(stages=[vector_assembler, random_forest])

# Split the data into training and test sets
(training_data, test_data) = indexed_data.randomSplit([0.8, 0.2], seed=123)

# Train the model
model = pipeline.fit(training_data)

# Get feature importances
feature_importances = model.stages[-1].featureImportances

# Display feature importances
print("Feature Importances:")
for i, (col, importance) in enumerate(zip(feature_columns, feature_importances)):
    print(f"{i + 1}. Feature {col}: {importance}")

# Make predictions on the test set
predictions = model.transform(test_data)

# Evaluate model performance
evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
accuracy = evaluator.evaluate(predictions)
print(f"Model Accuracy on Test Set: {accuracy}")
