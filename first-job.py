#######################################
#  IMPORT LIBRARIES AND SET VARIABLES
#######################################

#Import Python Modules
from datetime import datetime

#Import PySpark Modules
from pyspark.context import SparkContext
import pyspark.sql.functions as f

#Import Glue Modules
from awsglue.utils import gtResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import job

#Initialize Context and Sessions
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

#Parameters
glue_db = "mytest"
glue_tbl = "baltimore_politician_csv"
s3_write_path = "s3://sayak-special/output/"

#######################################
#  EXTRACT READ DATA
#######################################

dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_tbl)

#Convert dynamic frame to data frame to use standard pyspark functions

data_frame = dynamic_frame_read.toDF()

#######################################
#  TRANSFORM (MODIFY DATA)
#######################################

data_frame_aggregated = data_frame.groupby("PARTY")

data_frame_aggregated = data_frame_aggregated.orderby(f.desc("SALARY"))



#######################################
#  LOAD (WRITE DATA)
#######################################

#Create just 1 partition
data_frame_aggregated = data_frame_aggregated.repartition(1)

#Convert back to dynamic frame

dynamic_frame_write = DynamicFrame.fromDF(data_frame_aggregated, glue_context, "dynamic_frame_write")

#Write data back to S3

glue_context.write_dynamic_frame.from_options(
  frame = dynamic_frame_write,
  connection_type = "s3",
  connection_options = {
    path : s3_write_path,
  },
  format = "csv"
)


