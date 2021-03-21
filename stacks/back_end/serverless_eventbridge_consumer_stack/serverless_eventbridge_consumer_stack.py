from aws_cdk import aws_iam as _iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_events as _evnts
from aws_cdk import aws_events_targets as _evnts_tgt
from aws_cdk import aws_logs as _logs
from aws_cdk import core


class GlobalArgs:
    """
    Helper to define global statics
    """

    OWNER = "MystiqueAutomation"
    ENVIRONMENT = "production"
    REPO_NAME = "event-driven-with-eventbridge"
    SOURCE_INFO = f"https://github.com/miztiik/{REPO_NAME}"
    VERSION = "2021_03_06"
    MIZTIIK_SUPPORT_EMAIL = ["mystique@example.com", ]


class ServerlessEventBridgeConsumerStack(core.Stack):

    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        stack_log_level: str,
        orders_bus,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Add your stack resources below)

        # Read Lambda Code
        try:
            with open("stacks/back_end/serverless_eventbridge_consumer_stack/lambda_src/eventbridge_data_consumer.py",
                      encoding="utf-8",
                      mode="r"
                      ) as f:
                msg_consumer_fn_code = f.read()
        except OSError:
            print("Unable to read Lambda Function Code")
            raise
        msg_consumer_fn = _lambda.Function(
            self,
            "msgConsumerFn",
            function_name=f"events_consumer_fn",
            description="Process messages in EventBridge queue",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.InlineCode(
                msg_consumer_fn_code),
            handler="index.lambda_handler",
            timeout=core.Duration.seconds(3),
            reserved_concurrent_executions=1,
            environment={
                "LOG_LEVEL": f"{stack_log_level}",
                "APP_ENV": "Production"
            }
        )

        msg_consumer_fn_version = msg_consumer_fn.latest_version
        msg_consumer_fn_version_alias = _lambda.Alias(
            self,
            "msgConsumerFnAlias",
            alias_name="MystiqueAutomation",
            version=msg_consumer_fn_version
        )

        # Create Custom Loggroup for Producer
        msg_consumer_fn_lg = _logs.LogGroup(
            self,
            "msgConsumerFnLogGroup",
            log_group_name=f"/aws/lambda/{msg_consumer_fn.function_name}",
            removal_policy=core.RemovalPolicy.DESTROY,
            retention=_logs.RetentionDays.ONE_DAY
        )

        # Restrict Produce Lambda to be invoked only from the stack owner account
        msg_consumer_fn.add_permission(
            "restrictLambdaInvocationToOwnAccount",
            principal=_iam.AccountRootPrincipal(),
            action="lambda:InvokeFunction",
            source_account=core.Aws.ACCOUNT_ID,
            source_arn=orders_bus.event_bus_arn
        )

        # Event Pattern
        self.orders_pattern = _evnts.EventPattern(
            detail_type=["sales-events"]
        )

        # EventBridge Routing Rule
        self.orders_routing = _evnts.Rule(
            self,
            f"ordersEventRoutingRule01",
            description="A simple events routing rule",
            enabled=True,
            event_bus=orders_bus,
            event_pattern=self.orders_pattern,
            rule_name="orders_routing_to_consumer",
            targets=[_evnts_tgt.LambdaFunction(handler=msg_consumer_fn)]
        )

        self.orders_routing.apply_removal_policy(
            core.RemovalPolicy.DESTROY
        )

        # Restrict Produce Lambda to be invoked only from the stack owner account
        data_producer_fn.add_permission(
            "restrictLambdaInvocationToFhInOwnAccount",
            principal=_iam.AccountRootPrincipal(),
            action="lambda:InvokeFunction",
            source_account=core.Aws.ACCOUNT_ID
        )

        ###########################################
        ################# OUTPUTS #################
        ###########################################
        output_0 = core.CfnOutput(
            self,
            "AutomationFrom",
            value=f"{GlobalArgs.SOURCE_INFO}",
            description="To know more about this automation stack, check out our github page."
        )

        output_2 = core.CfnOutput(
            self,
            "msgConsumer",
            value=f"https://console.aws.amazon.com/lambda/home?region={core.Aws.REGION}#/functions/{msg_consumer_fn.function_name}",
            description="Process events received from eventbridge event bus"
        )
