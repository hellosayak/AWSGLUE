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



glueContext = GlueContext(SparkContext.getOrCreate())
