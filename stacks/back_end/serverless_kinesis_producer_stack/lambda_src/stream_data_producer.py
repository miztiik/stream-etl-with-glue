import json
import logging
import datetime
import os
import random
import uuid

import boto3


class GlobalArgs:
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    MAX_MSGS_TO_PRODUCE = int(os.getenv("MAX_MSGS_TO_PRODUCE", 5))
    STREAM_NAME = os.getenv("STREAM_NAME")
    STREAM_AWS_REGION = os.getenv("AWS_REGION")


def set_logging(lv=GlobalArgs.LOG_LEVEL):
    logging.basicConfig(level=lv)
    logger = logging.getLogger()
    logger.setLevel(lv)
    return logger


logger = set_logging()


def _rand_coin_flip():
    r = False
    if os.getenv("TRIGGER_RANDOM_FAILURES", True):
        if random.randint(1, 100) > 90:
            r = True
    return r


def _gen_uuid():
    return str(uuid.uuid4())


def send_data(client, data, key, stream_name):
    logger.info(
        f'{{"data":{json.dumps(data)}}}')
    resp = client.put_records(
        Records=[
            {
                "Data": json.dumps(data),
                "PartitionKey": key},
        ],
        StreamName=stream_name
    )
    logger.info(f"Response:{resp}")


client = boto3.client(
    "kinesis", region_name=GlobalArgs.STREAM_AWS_REGION)


def lambda_handler(event, context):
    resp = {"status": False}
    logger.info(f"Event: {json.dumps(event)}")

    _usr_names = ["Aarakocra", "Aasimar", "Beholder", "Bugbear", "Centaur", "Changeling", "Deep Gnome", "Deva", "Dragonborn", "Drow", "Dwarf", "Eladrin", "Elf", "Firbolg", "Genasi", "Githzerai", "Gnoll", "Gnome", "Goblin", "Goliath", "Hag", "Half-Elf",
                  "Half-Orc", "Halfling"]

    _categories = ["Books", "Games", "Mobiles", "Groceries", "Shoes", "Stationaries", "Laptops",
                   "Tablets", "Notebooks", "Camera", "Printers", "Monitors", "Speakers", "Projectors", "Cables", "Furniture"]

    _evnt_types = ["sales-events", "inventory-events"]

    _t_limit = context.get_remaining_time_in_millis()
    try:
        msg_cnt = 0
        p_cnt = 0
        sale_evnts = 0
        inventory_evnts = 0
        tot_sales = 0
        while context.get_remaining_time_in_millis() > 100:
            # _s = random.randint(1, 500)
            _s = round(random.random() * 100, 2)
            _evnt_type = random.choice(_evnt_types)
            _u = _gen_uuid()
            if _evnt_type == "sales-events":
                sale_evnts += 1
            elif _evnt_type == "inventory-events":
                inventory_evnts += 1
            evnt_body = {
                "request_id": _u,
                "name": random.choice(_usr_names),
                "category": random.choice(_categories),
                "store_id": f"store_{random.randint(1, 5)}",
                "evnt_time": datetime.datetime.now().isoformat(),
                "evnt_type": _evnt_type,
                "new_order": True,
                "sales": _s,
                "contact_me": "github.com/miztiik"
            }
            # Randomly make the return type order
            if bool(random.getrandbits(1)):
                evnt_body.pop("new_order", None)
                evnt_body["is_return"] = True
            # Random remove store_id from message
            if _rand_coin_flip():
                evnt_body.pop("store_id", None)
                evnt_body["bad_msg"] = True
                p_cnt += 1
            send_data(
                client,
                evnt_body,
                _u,
                GlobalArgs.STREAM_NAME
            )
            msg_cnt += 1
            tot_sales += _s

            logger.debug(
                f'{{"remaining_time":{context.get_remaining_time_in_millis()}}}')
        resp["msg_cnt"] = msg_cnt
        resp["bad_msgs"] = p_cnt
        resp["sale_evnts"] = sale_evnts
        resp["inventory_evnts"] = inventory_evnts
        resp["tot_sales"] = tot_sales
        resp["status"] = True
        logger.info(f'{{"resp":{json.dumps(resp)}}}')

    except Exception as e:
        logger.error(f"ERROR:{str(e)}")
        resp["error_message"] = str(e)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": resp
        })
    }
