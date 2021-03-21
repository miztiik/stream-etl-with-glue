from aws_cdk import aws_s3 as _s3
from aws_cdk import core as cdk
from stacks.miztiik_global_args import GlobalArgs


class S3Stack(cdk.Stack):

    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        stack_log_level: str,
        custom_bkt_name: str = None,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.data_bkt = _s3.Bucket(
            self,
            "dataBucket",
            # auto_delete_objects=True,
            # removal_policy=cdk.RemovalPolicy.DESTROY,
            # bucket_name="new-app-bucket-example",
        )

        # Lets set custom bucket name if it is set
        if custom_bkt_name:
            cfn_data_bkt = self.data_bkt.node.default_child
            cfn_data_bkt.add_override("Properties.BucketName", custom_bkt_name)

        ###########################################
        ################# OUTPUTS #################
        ###########################################
        output_0 = cdk.CfnOutput(
            self,
            "AutomationFrom",
            value=f"{GlobalArgs.SOURCE_INFO}",
            description="To know more about this automation stack, check out our github page."
        )
        output_1 = cdk.CfnOutput(
            self,
            "dataSourceBucket",
            value=f"{self.data_bkt.bucket_name}",
            description=f"The datasource bucket name"
        )
        output_2 = cdk.CfnOutput(
            self,
            "dataSourceBucketUrl",
            value=f"https://console.aws.amazon.com/s3/buckets/{self.data_bkt.bucket_name}",
            description=f"The datasource bucket name"
        )
