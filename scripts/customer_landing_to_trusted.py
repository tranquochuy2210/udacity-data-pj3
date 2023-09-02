import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket landing zone
S3bucketlandingzone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://huytq57-project-3/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucketlandingzone_node1",
)

# Script generated for node Filter customer
Filtercustomer_node1693651619304 = Filter.apply(
    frame=S3bucketlandingzone_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filtercustomer_node1693651619304",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Filtercustomer_node1693651619304,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://huytq57-project-3/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
