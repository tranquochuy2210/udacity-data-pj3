import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customerTrustedZone
customerTrustedZone_node1693653039641 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://huytq57-project-3/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customerTrustedZone_node1693653039641",
)

# Script generated for node accelerometerLandingZone
accelerometerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://huytq57-project-3/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometerLandingZone_node1",
)

# Script generated for node Join
Join_node1693653094321 = Join.apply(
    frame1=accelerometerLandingZone_node1,
    frame2=customerTrustedZone_node1693653039641,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1693653094321",
)

# Script generated for node Drop Fields
DropFields_node1693653813655 = DropFields.apply(
    frame=Join_node1693653094321,
    paths=["user", "x", "y", "timeStamp", "z"],
    transformation_ctx="DropFields_node1693653813655",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1693658884056 = DynamicFrame.fromDF(
    DropFields_node1693653813655.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1693658884056",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://huytq57-project-3/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="project-3", catalogTableName="accelerometer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropDuplicates_node1693658884056)
job.commit()
