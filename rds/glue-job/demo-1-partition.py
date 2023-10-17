import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_now

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Relational DB
RelationalDB_node1697507787687 = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "demo1",
        "connectionName": "mysql",
        "jobBookmarkKeys":["customer_id","updated_time"],"jobBookmarkKeysSortOrder":"asc"
    },
    transformation_ctx="RelationalDB_node1697507787687",
)

# Script generated for node Add Current Timestamp
AddCurrentTimestamp_node1697508640676 = RelationalDB_node1697507787687.gs_now(
    colName="partition_col"
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.getSink(
    path="s3://testbb-123/etl-output/demo-1-partition/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["partition_col"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node2",
)
S3bucket_node2.setCatalogInfo(catalogDatabase="gluedb", catalogTableName="demo-1-partition")
S3bucket_node2.setFormat("glueparquet")
S3bucket_node2.writeFrame(AddCurrentTimestamp_node1697508640676)
job.commit()
