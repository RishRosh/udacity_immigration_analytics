# US Immigration Data Lake
## Data Engineering Capstone Project - Udacity
#### Objective: To empower U.S CBP (Customs & Border Protection) in making better decisions based on immigration patterns

<img src="https://github.com/RishRosh/udacity_immigration_analytics/blob/master/images/Front.jpg" align="centre">

## Overview

The purpose of the project is to combine what we've learned through the Udacity Data Engineering program. This project will be based on a real world example and is important part of  portfolio that will help to achieve data engineering-related career goals.

## Scenario

The U.S. CBP (Customs and Border Protection) needs help to analyze the data on immigration and find patterns to help them make better decisions on increasing / decreasing staffing, identify busy ports, busy times of the year and trends over time. They have provided the immigration database and it will need to be augmented with demographics data, U.S. cities, ports and codes data sets. 

What they get in return are analytics tables which they can slice and dice with different topics (dimensions) that they are interested in. 

## The Architecture

The complete solution is based on top of **Amazon Web Services (AWS)**. The datasets are preprocessed with **Apache Spark** and stored in a staging area in **AWS S3 bucket**. Then this data is loaded in to **Amazon Redshift** cluster using python pipeline that transfers and checks the data quality of the analytics tables.

#### The Data Model

<img src="https://github.com/RishRosh/udacity_immigration_analytics/blob/master/images/Conceptual.jpg" align="centre">

## Structure of the Project

The project can be broken down in to two ETL loads: 
#### ETL - 1:
- **etl1.py** - File that was ran on EMR cluster to load S3 data. It outputs staging files back in S3. These are the steps: <br>
        1. EMR with applications: Spark 2.4.4, Zeppelin 0.8.2, Livy 0.6.0, JupyterHub 1.0.0 <br>
        2. Release label: emr-5.28.0<br>
        3. To install the `configparser` run the below command:<br> 
        `sudo pip install configparser` <br>
        4. Copy the etl1.py to `/home/hadoop` like below:<br>
        `scp -i <local_path/PEM_FILE.pem <local_path\etl1.py> hadoop@xxx.amazonaws.com:/home/hadoop/`<br>
        5. Run the job by typing the below command: <br> 
        `/usr/bin/spark-submit --master yarn ./etl1.py`<br>
        6. Uses `dl.cfg` to load S3 keys

- **etl1.ipynb** - A Jupyter notebook that was used to build out Spark code


#### ETL - 2:
- **etl2.py** - File that creates Redshift tables, copies data from S3 and loads star schema.<br>
        1. `creates_tables.py` script can be run to simply create/re-create tables<br>
        2. Uses `dwh2.cfg` file to load the configurations for Redshift cluster and S3 bucket<br>
        3. File `etl2.ipynb` is a Jupyter notebook to get step-by-step execution of the load<br>

Following the Udacity guide for this project, the structure is as shown below:

 - Step 1: Scope the Project and Gather Data
 - Step 2: Explore and Assess the Data
 - Step 3: Define the Data Model
 - Step 4: Run ETL to Model the Data
 - Step 5: Complete Project Write Up

*** For exploring further, please go to the 
Link: [US_Immigration_Data_Lake](https://github.com/saurabhsoni5893/US-Immigration-Data-Lake/blob/master/US_Immigration_Data_Lake.ipynb)
