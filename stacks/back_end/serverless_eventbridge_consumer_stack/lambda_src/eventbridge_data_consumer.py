# -*- coding: utf-8 -*-

import json
import logging
import os
import random
import boto3
from botocore.exceptions import ClientError


"""
.. module: eventbridge_data_consumer
    :Actions: Consume messages from EventBridge Queue
    :copyright: (c) 2021 Mystique.,
.. moduleauthor:: Mystique
.. contactauthor:: miztiik@github issues
"""


__author__ = "Mystique"
__email__ = "miztiik@github"
__version__ = "0.0.1"
__status__ = "production"


class GlobalArgs:
    OWNER = "Mystique"
    ENVIRONMENT = "production"
    MODULE_NAME = "eventbridge_data_consumer"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    RELIABLE_QUEUE_NAME = os.getenv("RELIABLE_QUEUE_NAME")


def set_logging(lv=GlobalArgs.LOG_LEVEL):
    """ Helper to enable logging """
    logging.basicConfig(level=lv)
    logger = logging.getLogger()
    logger.setLevel(lv)
    return logger


LOG = set_logging()
sqs_client = boto3.client("sqs")


def _rand_coin_flip():
    r = False
    if os.getenv("TRIGGER_RANDOM_DELAY", True):
        if random.randint(1, 100) > 90:
            r = True
    return r


def lambda_handler(event, context):
    resp = {"status": False}
    LOG.info(f"Event: {json.dumps(event)}")
    resp["status"] = True
    LOG.info(f'{{"resp":{json.dumps(resp)}}}')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": resp
        })
    }
