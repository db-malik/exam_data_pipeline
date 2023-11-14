# Data Engineering Project - Flight Data Analysis

## Overview

This project aims to conduct a thorough analysis of flight data for a given airline, along with designing a data model that can predict flight delays. The data used for this project is publicly available. you can get from : https://www.kaggle.com/datasets/tylerx/flights-and-airportsdata?select=flights.csv



## Table of Contents

- [Environment Setup](#environment-setup)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Data Ingestion](#data-ingestion)
- [Data Cleaning and Preparation](#data-cleaning-and-preparation)
- [Machine Learning Model](#machine-learning-model)
- [Orchestration with Apache Airflow](#orchestration-with-apache-airflow)
- [Argumentation](#argumentation)
- [Contact](#contact)


## Prerequisites


- Python 
- Before proceeding with the installation, ensure that you have Java installed on your machine, as Apache Spark is built on top of the Java Virtual      Machine (JVM).
- Installation of Apache Spark and Apache Airflow
- Git Repository

## Environment Setup

Ensure that you have the following tools installed on your local system:

- Apache Spark
- Apache Airflow

## Project Structure

```plaintext
project_repository/
|-- README.md
|-- airflow/
|   |-- dags/
|   |   |-- flight_pipeline.py
|-- scripts/
|   |-- utils/
|   |-- data_analysing.py
|   |-- data_loading.py
|   |-- ml_mpredection.py
|   |-- analyse_data_for_each_airligne.py
|-- data/
|-- spark/
```


data/: Contains CSV files with flight and airport data.
scripts/: Python scripts for data loading, analysis, and other tasks.
utils/: Utility functions used across the project.



## Getting Started

1. **Clone the Git repository:**

   ```bash
   git clone <repository_url>
   cd project_repository
   ```

2. **Set up your environment by installing Spark and Airflow.**
   # Flight Data Analysis with Apache Spark and Apache Airflow

## Installation




## Apache Spark Installation

  ### Step 1: Download and Extract Apache Spark

    Visit the [official Apache Spark website](https://spark.apache.org/downloads.html) and download the pre-built package for Hadoop. Choose the version that suits your requirements.

    ```bash
    tar -zxvf spark-<version>-bin-hadoop<version>.tgz
    ```

    Replace <version> with the specific version numbers in the filename.

2. **Set up your environment by installing Spark and Airflow.**


## Apache Spark Data Preparation

1. **Clone the repository:**

 
2. **Clean and prepare the data and evaluate the quality of data using Spark operations:**

   ```bash
   spark-submit scripts/data_loading.py
   ```

3. **Analyse data en general:**

   ```bash
   spark-submit scripts/data_analysing.py
   ```

4. **Analyse data en for each airligne company:**

   ```bash
   spark-submit scripts/analyse_data_for_each_airligne.py
   ```


## Machine Learning Model

1. **Design and train a machine learning model:**

   ```bash
   spark-submit scripts/ml_predection.py
   ```


## Apache Airflow Orchestration

1. **Orchestrate the entire data pipeline using Apache Airflow:**

   ```bash
   airflow trigger_dag flight_pipeline
   ```

##  Project Workflow

  1.  Data Loading: The data_loading.py script loads flight and airport data into Spark DataFrames.
     
      Results: a analysis csv file stored in analysis_input_data  folder
               a merged_data contain the full cleaned data stored in merged_data/merged_flights_data.csv

  2. Data Analysis: The data_analysis.py and analyse_data_for_each_airligne.py scripts includes various functions for analyzing flight data, such as average delays, route analysis, and more:

        Results: a analysis csv file stored in analysis_data  folder

  3. Machine Learning: The ml_predection_for_airlines.py script demonstrates a simple machine learning task, predicting flight delays using logistic regression.
      the predection evaluation log it terminal


  
