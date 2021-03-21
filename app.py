#!/usr/bin/env python3

from stacks.back_end.serverless_kinesis_producer_stack.serverless_kinesis_producer_stack import ServerlessKinesisProducerStack
from stacks.back_end.glue_stacks.glue_table_stack import GlueTableStack
from stacks.back_end.glue_stacks.glue_job_stack import GlueJobStack
from stacks.back_end.glue_stacks.glue_crawler_stack import GlueCrawlerStack
from stacks.back_end.s3_stack.s3_stack import S3Stack
from aws_cdk import core as cdk

app = cdk.App()

# Kinesis Data Producer on Lambda
serverless_kinesis_producer_stack = ServerlessKinesisProducerStack(
    app,
    f"{app.node.try_get_context('project')}-producer-stack",
    stack_log_level="INFO",
    description="Miztiik Automation: Kinesis Data Producer on Lambda")


# S3 Bucket to hold our datasources
etl_bkt_stack = S3Stack(
    app,
    f"{app.node.try_get_context('project')}-data-src-bkt-stack",
    stack_log_level="INFO",
    description="Miztiik Automation: S3 Bucket to hold our datasources"
)

# Glue Stacks
glue_tbl_stack = GlueTableStack(
    app,
    f"{app.node.try_get_context('project')}-txns-tbl-stack",
    stack_log_level="INFO",
    src_stream=serverless_kinesis_producer_stack.get_stream,
    description="Miztiik Automation: Glue Table Stack"
)


# Glue Job Stack
glue_job_stack = GlueJobStack(
    app,
    f"{app.node.try_get_context('project')}-job-stack",
    stack_log_level="INFO",
    src_stream=serverless_kinesis_producer_stack.get_stream,
    glue_db_name=glue_tbl_stack.glue_db_name.value_as_string,
    glue_table_name=glue_tbl_stack.glue_table_name.value_as_string,
    etl_bkt=etl_bkt_stack.data_bkt,
    description="Miztiik Automation: Glue Job Stack"
)

# Glue Crawler
glue_crawler_stack = GlueCrawlerStack(
    app,
    f"{app.node.try_get_context('project')}-crawler-stack",
    stack_log_level="INFO",
    _glue_etl_role=glue_job_stack._glue_etl_role,
    etl_bkt=etl_bkt_stack.data_bkt,
    etl_bkt_prefix=glue_job_stack.etl_prefix,
    glue_db_name=glue_tbl_stack.glue_db_name.value_as_string,
    description="Miztiik Automation: Glue Crawler Stack"
)


# Stack Level Tagging
_tags_lst = app.node.try_get_context("tags")

if _tags_lst:
    for _t in _tags_lst:
        for k, v in _t.items():
            cdk.Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
