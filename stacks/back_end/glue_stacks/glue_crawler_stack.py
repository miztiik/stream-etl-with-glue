from aws_cdk import aws_iam as _iam
from aws_cdk import aws_logs as _logs
from aws_cdk import aws_glue as _glue
from aws_cdk import aws_s3_assets as _s3_assets
from aws_cdk import core as cdk
from stacks.miztiik_global_args import GlobalArgs


class GlueCrawlerStack(cdk.Stack):
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        stack_log_level: str,
        _glue_etl_role,
        etl_bkt,
        etl_bkt_prefix,
        glue_db_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.template_options.metadata = {"License": "Miztiik Corp."}

        # Glue Crawler
        sale_txns_crawler = _glue.CfnCrawler(
            self,
            "glueDataLakeCrawler",
            name="sale_txns_crawler",
            role=_glue_etl_role.role_arn,
            database_name=f"{glue_db_name}",
            table_prefix="sales_txns_in_parquet_",
            description="Miztiik Automation: Crawl the sales transactions in parquet format, store in table to enable Athena Querying",
            targets={
                "s3Targets": [
                    {
                        "path": f"s3://{etl_bkt.bucket_name}/{etl_bkt_prefix}",
                        "exclusions": ["checkpoint/**"]
                    }
                ]
            },
            configuration="{\"Version\":1.0,\"CrawlerOutput\":{\"Partitions\":{\"AddOrUpdateBehavior\":\"InheritFromTable\"},\"Tables\":{\"AddOrUpdateBehavior\":\"MergeNewColumns\"}}}",
            schedule=_glue.CfnCrawler.ScheduleProperty(
                schedule_expression="cron(0 * * * ? *)"
            )
        )
        # Configuration As JSON in human readable format
        """
        {
            "Version": 1,
            "CrawlerOutput": {
                "Partitions": {
                    "AddOrUpdateBehavior": "InheritFromTable"
                },
                "Tables": {
                    "AddOrUpdateBehavior": "MergeNewColumns"
                }
            }
        }
        and SchemaChangePolicy 
        {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE"
        }
        """

        ###########################################
        ################# OUTPUTS #################
        ###########################################
        output_0 = cdk.CfnOutput(
            self,
            "AutomationFrom",
            value=f"{GlobalArgs.SOURCE_INFO}",
            description="To know more about this automation stack, check out our github page.",
        )

        output_1 = cdk.CfnOutput(
            self,
            "SaleTransactionsCrawler",
            value=f" https://console.aws.amazon.com/glue/home?region={cdk.Aws.REGION}#crawler:name={sale_txns_crawler.name}",
            description="Glue ETL Job.",
        )
