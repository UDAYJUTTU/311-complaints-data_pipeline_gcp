from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
conf=SparkConf().setAppName('311-complaints').setMaster('yarn')
sc=SparkContext(conf=conf)
spark=SparkSession.builder.config(conf=conf).getOrCreate()
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys

bucket_path=str(sys.argv[1])
Schema = StructType([
    StructField('Unique_key',IntegerType() , False),
    StructField('Created_Date', TimestampType(), True),
    StructField('Closed_Date', TimestampType(), True),
    StructField('Agency', StringType(), False),
    StructField('Agency_Name', StringType(), True),
    StructField('Complaint_Type', StringType(), True),
    StructField('Descriptor', StringType(), True),
    StructField('Location_Type', StringType(), True),
    StructField('Incident_Zip',IntegerType() , True),
    StructField('Incident_Address', StringType(), True),
    StructField('Street_Name', StringType(), True),
    StructField('Cross_Street_1', StringType(), True),
    StructField('Cross_Street_2', StringType(), True),
    StructField('Intersection_Street_1', StringType(), True),
    StructField('Intersection_Street_2', StringType(), True),
    StructField('Address_Type', StringType(), True),
    StructField('City',StringType() , True),
    StructField('Landmark', StringType(), True),
    StructField('Facility_Type', StringType(), True),
    StructField('Status', StringType(), True),
    StructField('Due_Date', StringType(), True),
    StructField('Resolution_Description', StringType(), True),
    StructField('Resolution_Action_Updated_Date', TimestampType(), True),
    StructField('Community_Board:', StringType(), True),
    StructField('BBL',IntegerType() , True),
    StructField('Borough', StringType(), True),
    StructField('X_Coordinate', StringType(), True),
    StructField('Y_Coordinate', DoubleType(), True),
    StructField('Open_Data_Channel_Type', StringType(), True),
    StructField('Park_Facility_Name', StringType(), True),
    StructField('Park_Borough', StringType(), True),
    StructField('Vehicle_Type', StringType(), True),
    StructField('Taxi_Company_Borough', StringType(), True),
    StructField('Taxi_Pick_Up_Location',IntegerType() , True),
    StructField('Bridge_Highway_Name', StringType(), True),
    StructField('Bridge_Highway_Direction', StringType(), True),
    StructField('Road_Ramp', StringType(), True),
    StructField('Bridge_Highway_Segment', StringType(), True),
    StructField('Latitude', DoubleType(), True),
    StructField('Longitude', DoubleType(), True),
    StructField('Zip_Codes', StringType(), True),
    StructField('Community_Districts',StringType() , True),
    StructField('Borough_Boundaries', StringType(), True),
    StructField('City_Council_Districts', StringType(), True),
    StructField('Police_Precincts', StringType(), True),
    ])

df=spark.read.csv(bucket_path,header=True,schema=Schema)
data_311=df.select([df.Unique_key,df.Created_Date,df.Agency,df.Agency_Name,df.Complaint_Type,df.Location_Type,df.Incident_Zip,df.City,df.Status,df.Borough,df.Latitude,df.Longitude])
data_311=data_311.withColumn("Year",year(df['Created_Date']).cast(IntegerType()))
data_311=data_311.withColumn("Month",month(df['Created_Date']).cast(IntegerType()))
data_311.write.format('bigquery').option('table','nyc-311-complaints.311_complaints_analysis.raw_data').option("temporaryGcsBucket","311-complaints-temp-bucket").mode('append').save()
sc.stop()