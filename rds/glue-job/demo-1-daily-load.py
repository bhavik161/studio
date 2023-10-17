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
RelationalDB_node1697549406237 = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "demo1",
        "connectionName": "mysql",
        "jobBookmarkKeys":["customer_id"],"jobBookmarkKeysSortOrder":"asc"
    },
    transformation_ctx="RelationalDB_node1697549406237",
)

# Script generated for node Add Current Timestamp
AddCurrentTimestamp_node1697549433329 = RelationalDB_node1697549406237.gs_now(
    colName="load_date", dateFormat="%Y-%m-%d"
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.getSink(
    path="s3://testbb-123/etl-output/daily-load/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node2",
)
S3bucket_node2.setCatalogInfo(
    catalogDatabase="gluedb", catalogTableName="demo1-dailyload"
)
S3bucket_node2.setFormat("glueparquet")
S3bucket_node2.writeFrame(AddCurrentTimestamp_node1697549433329)
job.commit()
