import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Row
import datetime
import logging
from awsglue import DynamicFrame

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "src_db_name",
    "src_tbl_name",
    "datalake_bkt_name",
    "datalake_bkt_prefix"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info(f'{{"starting_job": "{args["JOB_NAME"]}"}}')

data_frame_datasource0 = glueContext.create_data_frame.from_catalog(
    database=args["src_db_name"],
    table_name=args["src_tbl_name"],
    transformation_ctx="datasource0",
    additional_options={
        "startingPosition": "TRIM_HORIZON", "inferSchema": "true"}
)


def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        datasource0 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame")
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour
        minute = now.minute
        path_datasink1 = f"s3://{args['datalake_bkt_name']}/{args['datalake_bkt_prefix']}" + "/ingest_year=" + "{:0>4}".format(str(year)) + "/ingest_month=" + "{:0>2}".format(
            str(month)) + "/ingest_day=" + "{:0>2}".format(str(day)) + "/ingest_hour=" + "{:0>2}".format(str(hour)) + "/"
        datasink1 = glueContext.write_dynamic_frame.from_options(
            frame=datasource0,
            connection_type="s3",
            connection_options={
                "path": path_datasink1
            },
            format="parquet",
            transformation_ctx="datasink1"
        )
        logger.info(f'{{"batch_process_successful":True}}')


glueContext.forEachBatch(
    frame=data_frame_datasource0,
    batch_function=processBatch,
    options={
        "windowSize": "100 seconds",
        "checkpointLocation": f"s3://{args['datalake_bkt_name']}/{args['datalake_bkt_prefix']}/checkpoint"
        # "s3://raw-data-bkt-010/stream-etl/checkpoint/"
    }
)

job.commit()
