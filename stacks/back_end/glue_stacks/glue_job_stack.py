from aws_cdk import aws_iam as _iam
from aws_cdk import aws_logs as _logs
from aws_cdk import aws_glue as _glue
from aws_cdk import aws_s3_assets as _s3_assets
from aws_cdk import core as cdk
from stacks.miztiik_global_args import GlobalArgs


class GlueJobStack(cdk.Stack):
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        stack_log_level: str,
        glue_db_name: str,
        glue_table_name: str,
        etl_bkt,
        src_stream,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.template_options.metadata = {"License": "Miztiik Corp."}

        # Glue Job IAM Role
        self._glue_etl_role = _iam.Role(
            self, "glueJobRole",
            assumed_by=_iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3ReadOnlyAccess"
                ),
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ]
        )
        self._glue_etl_role.add_to_policy(
            _iam.PolicyStatement(
                actions=[
                    "s3:*"
                ],
                resources=[
                    f"{etl_bkt.bucket_arn}",
                    f"{etl_bkt.bucket_arn}/*"
                ]
            )
        )

        self._glue_etl_role.add_to_policy(
            _iam.PolicyStatement(
                actions=[
                    "kinesis:DescribeStream"
                ],
                resources=[
                    f"{src_stream.stream_arn}"
                ]
            )
        )

        src_stream.grant_read(self._glue_etl_role)

        # Create the Glue job to convert incoming JSON to parquet
        # Read GlueSpark Code
        try:
            with open(
                "stacks/back_end/glue_stacks/glue_job_scripts/kinesis_streams_batch_to_s3_etl.py",
                encoding="utf-8",
                mode="r",
            ) as f:
                kinesis_streams_batch_to_s3_etl = f.read()
        except OSError:
            print("Unable to read Glue Job Code")
            raise

        etl_script_asset = _s3_assets.Asset(
            self,
            "etlScriptAsset",
            path="stacks/back_end/glue_stacks/glue_job_scripts/kinesis_streams_batch_to_s3_etl.py"
        )

        self.etl_prefix = "stream-etl"
        _glue_etl_job = _glue.CfnJob(
            self,
            "glueJsonToParquetJob",
            name="stream-etl-processor",
            description="Glue Job to process stream of events from Kinesis data stream and store them in parquet format in S3",
            role=self._glue_etl_role.role_arn,
            glue_version="2.0",
            command=_glue.CfnJob.JobCommandProperty(
                name="gluestreaming",
                script_location=f"s3://{etl_script_asset.s3_bucket_name}/{etl_script_asset.s3_object_key}",
                python_version="3"
            ),
            default_arguments={
                "--src_db_name": glue_db_name,
                "--src_tbl_name": glue_table_name,
                "--datalake_bkt_name": etl_bkt.bucket_name,
                "--datalake_bkt_prefix": f"{self.etl_prefix}/",
                "--job-bookmark-option": "job-bookmark-enable"
            },
            allocated_capacity=1,
            # timeout=2,
            max_retries=2,
            execution_property=_glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1)
        )

        # Configure a Trigger - Every hour
        _glue_etl_job_trigger = _glue.CfnTrigger(
            self,
            "glueEtlJobtrigger",
            type="SCHEDULED",
            description="Miztiik Automation: Trigger streaming etl glue job every hour",
            schedule="cron(0 1 * * ? *)",
            start_on_creation=False,
            actions=[_glue.CfnTrigger.ActionProperty(
                job_name=f"{_glue_etl_job.name}",
                timeout=2
            )
            ]
        )
        _glue_etl_job_trigger.add_depends_on(_glue_etl_job)

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
            "StreamingETLGlueJob",
            value=f"https://console.aws.amazon.com/gluestudio/home?region={cdk.Aws.REGION}#/jobs",
            description="Glue ETL Job.",
        )
