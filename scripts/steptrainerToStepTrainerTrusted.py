import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node stepTrainerLanding
stepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://huytq57-project-3/step-training/landing/"],
        "recurse": True,
    },
    transformation_ctx="stepTrainerLanding_node1",
)

# Script generated for node S3CustomerCurated
S3CustomerCurated_node1693659570573 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://huytq57-project-3/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="S3CustomerCurated_node1693659570573",
)

# Script generated for node Join
Join_node1693659507508 = Join.apply(
    frame1=stepTrainerLanding_node1,
    frame2=S3CustomerCurated_node1693659570573,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1693659507508",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1693659507508,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://huytq57-project-3/step-training/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
