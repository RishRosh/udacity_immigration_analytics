import configparser
import os
import logging
from datetime import timedelta, datetime
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id, lit, substring
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek

# logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# configuration
config = configparser.ConfigParser()
config.read('dl.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['S3']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['S3']['AWS_SECRET_ACCESS_KEY']
# INPUT_S3_BUCKET = config['S3']['INPUT_S3_BUCKET']
# OUTPUT_S3_BUCKET = config['S3']['OUTPUT_S3_BUCKET']


def create_spark_session():
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:3.0.0-s_2.12").\
    appName("ETL1").\
    enableHiveSupport().getOrCreate()
    return spark

# original config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\

def load_demo(spark, input_data):
    """Load demographics csv data file
    
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            
        Returns:
            PySpark dataframe
    """    
    # start logging message
    logging.info("Started loading demographics data")
    
    # load the file
    f_demo = os.path.join(input_data + "us-cities-demographics.csv")
    df = spark.read.format('csv').options(header=True, delimiter=';').load(f_demo)
    
    # Get only selected columns and de-dupe them
    df_demo = df.select("City", "Median Age", "Male Population", "Female Population", "Total Population", \
                        "Number of Veterans", "Foreign-born", "Average Household Size", "State Code")\
                .drop_duplicates()
    
    # end logging message and return the dataframe
    logging.info("Completed loading demographics data")
    return df_demo


def load_us_cities(spark, input_data):
    """Load US Cities csv data file
    
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            
        Returns:
            PySpark dataframe
    """   
    # start logging message
    logging.info("Started loading US Cities data")
    
    # load file
    f_uscit = os.path.join(input_data + "uscities.csv")
    df = spark.read.format('csv').options(header=True).load(f_uscit)
    
    # select only the relevant columns
    df_cit = df.select("id", "city", "state_id", "state_name", "lat","lng")\
                .drop_duplicates()
    
    # end logging and return dataframe
    logging.info("Completed loading US Cities data")
    return df_cit

def load_airport(spark, input_data):
    """Load airport codes csv data file
    
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            
        Returns:
            PySpark dataframe
    """   
    # start logging message
    logging.info("Started loading airport codes data")

    # load file
    f_air = os.path.join(input_data + "airport-codes_csv.csv")
    df = spark.read.format('csv').options(header=True).load(f_air)

    # transform and de-dupe data
    df_air = df.where(df['iso_country']=="US")\
            .select("municipality", "iata_code", "local_code", "iso_region") \
            .withColumn("country", substring("iso_region",1,2))\
            .withColumn("state_code", substring("iso_region",4,2)) \
            .na.drop(subset=["iata_code","local_code"]) \
            .drop_duplicates()
    df_air = df_air.select("municipality", "iata_code", "local_code", "state_code")

    # end logging message and return data frame
    logging.info("Completed loading airport codes data")
    return df_air


def process_city_demo_airport(df_airport, df_cit, df_demo, output_data):
    """Processes city, demographics and airport data 
        and outputs them to a parquet format
        
        Arguments:
            df_airport {object}: dataframe
            df_cit {object}: dataframe
            df_demo {object}: dataframe
            output_data {object}: Target S3 endpoint
            
        Returns:
            none
    """       
    # Join city and airport data
    logging.info("Joining airport codes and city data")
    df_ap_cit = df_airport.join(df_cit, (df_airport.municipality==df_cit.city) \
                           & (df_airport.state_code==df_cit.state_id))\
            .select("id", "city", "iata_code", "state_id", "state_name", "lat", "lng" )

    # Join demographics data to it
    logging.info("Joining demographics data")
    df_ap_cit_demo = df_ap_cit.join(df_demo, (df_ap_cit.city==df_demo.City) \
                        & (df_ap_cit.state_id==df_demo["State Code"]))\
                    .select("id", df_ap_cit["city"], "iata_code", "state_id", "state_name", "lat", "lng"\
                        ,col("Median Age").alias("median_age")\
                        ,col("Male Population").alias("male_population")\
                        ,col("Female Population").alias("female_population")\
                        ,col("Total Population").alias("total_population")\
                        ,col("Number of Veterans").alias("no_of_veterans")\
                        ,col("Foreign-born").alias("foreign_born")\
                        ,col("Average Household Size").alias("avg_household_size"))
    
    # write out the data
    df_ap_cit_demo.write.parquet(os.path.join(output_data, "cit_demo_air/"), mode="overwrite")
    logging.info("Processed city, demographics and airport data")
    
def load_immi(spark, sas_file):
    """Load immigration SAS data file
    
        Arguments:
            spark {object}: SparkSession object
            sas_file {object}: Path of the SAS file
            
        Returns:
            PySpark dataframe
    """  
    # start loading
    logging.info("Started loading immigration data")
    df = spark.read.format('com.github.saurfang.sas.spark')\
            .load(sas_file)
    
    # function to convert sas date
    date_format = "%Y-%m-%d"
    convert_sas_udf = udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime(date_format))

    # process immigration data
    df_immi = df.select("cicid", "arrdate", "i94yr", "i94mon", "i94visa", "i94port", "airline", "fltno", "visatype")\
                .na.drop(subset=["arrdate", "i94yr", "i94mon", "i94visa", "i94port", "airline", "fltno", "visatype"]) \
                .withColumn("cicid", col("cicid").cast("int")) \
                .withColumn("i94mon", col("i94mon").cast("int")) \
                .withColumn("i94yr", col("i94yr").cast("int")) \
                .withColumn("i94visa", col("i94visa").cast("int")) \
                .withColumn("arrdate", convert_sas_udf("arrdate")) \
                .withColumn("mon", col("i94mon").cast("int")) \
                .withColumn("yr", col("i94yr").cast("int")) \
                .drop_duplicates()
    
    logging.info("Finished loading immigration data")
    return df_immi    
    
def process_immi(spark, sas_input_dir, output_data): 
    """Processes immigration data  and outputs immigration, 
    time, visa and flights data in parquet format
        
        Arguments:
            spark {object}: SparkSession object
            sas_input_dir {object}: Source S3 dir endpoint
            output_data {object}: Target S3 endpoint
            
        Returns:
            none
    """ 
    
    # process all files
    df_immi = None
    df_t = None
    onlyfiles = [f for f in listdir(sas_input_dir) if isfile(join(sas_input_dir, f))]
    for i in onlyfiles:
        logging.info("Started processing immigration file {}".format(i))
        sas_file = join(sas_input_dir, i)
        if df_immi is None:
            df_immi = load_immi(spark, sas_file)
        else:
            df_t = load_immi(spark, sas_file)
            df_immi = df_immi.union(df_t)
        logging.info("Finished processing immigration file {}".format(i))
    
    logging.info("Completed processing immigration data")
    logging.info("Writing out immigration data")
    
    # write immi data 
    df_immi.write.parquet(os.path.join(output_data, "immigration/"), mode="overwrite", partitionBy=["yr","mon"])
    logging.info("Writing immigration data completed")
    
    
    logging.info("Processing time data")
    # process time data
    time_table = df_immi.withColumn("day",dayofmonth("arrdate"))\
                    .withColumn("week",weekofyear("arrdate"))\
                    .withColumn("month",month("arrdate"))\
                    .withColumn("year",year("arrdate"))\
                    .withColumn("weekday",dayofweek("arrdate")) \
                    .select("arrdate", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time data 
    time_table.write.parquet(os.path.join(output_data, "time/"), mode="overwrite")
    logging.info("Time data processed")
    
    logging.info("Processing flights data")
    
    # process flights data
    flights = df_immi.select("airline", "fltno").drop_duplicates()
                
    # write flights data 
    flights.write.parquet(os.path.join(output_data, "flights/"), mode="overwrite")
    logging.info("Flights data processed")
    
    logging.info("Processing visa data")
    
    # process visa data
    visas = df_immi.select("i94visa", "visatype").drop_duplicates()
                    
    # write visa data 
    visas.write.parquet(os.path.join(output_data, "visas/"), mode="overwrite")
    logging.info("Visa data processed")

def main():
    
    logging.info("Starting pipeline for immigration datasets")
    spark = create_spark_session()

    input_data = "s3a://zishi/dend_capstone/input/"
    sas_input_dir  = '../../data/18-83510-I94-Data-2016/'
    sas_input_file = 'i94_apr16_sub.sas7bdat'
    output_data = "s3a://zishi/dend_capstone/output"

    df_demo = load_demo(spark, input_data)
    df_cit = load_us_cities(spark, input_data)
    df_airport = load_airport(spark, input_data)
    
    process_city_demo_airport(df_airport, df_cit, df_demo, output_data)
    process_immi(spark, sas_input_dir, output_data)
    
    logging.info("Pipeline for immigration datasets completed")


if __name__ == "__main__":
    main()

