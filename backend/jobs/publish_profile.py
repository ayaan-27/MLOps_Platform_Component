# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205


import sys,os
import pika
from pika.exchange_type import ExchangeType
import json

sys.path.append(os.getcwd())
import utils.logs as logs

LOGGER = logs.get_logger()




# credentials = pika.PlainCredentials('user', 'PASSWORD')
# parameters = pika.ConnectionParameters('ad54e5e5c5cef471a854ac242511c8f2-1244485869.us-east-1.elb.amazonaws.com',heartbeat=60, credentials=credentials)
# connection = pika.BlockingConnection(parameters)
# channel = connection.channel()
# routing_key="paceml"

'''
channel.exchange_declare(exchange="pacemlexchange",
                exchange_type=ExchangeType.direct,
                passive=False,
                durable=True,
                auto_delete=False)

channel.queue_declare(queue = "long_running_task",
                    durable=True,
                    passive=False,
                    auto_delete=False)

channel.queue_bind(queue="long_running_task",
            exchange="pacemlexchange",
            routing_key="paceml")


body = {
"type": "preprocess",
"pre_process_dict": {"MPG": [{"clip": [{"method": "value"}, {"u_min": ""}, {"u_max": ""}]}]},
"dataset_id":1,
"version_id":1,
"project_id":1,
"user_id":1
}

dataset_id = body["dataset_id"]
version_id = body["version_id"]
project_id = body["project_id"]
user_id = body["user_id"]
res_job = jm.make_job(dataset_id=dataset_id, version_id=version_id, project_id=project_id,
                    user_id=user_id)

body["job_id"] = res_job["msg"]
channel.basic_publish(
    'pacemlexchange', routing_key, json.dumps(body),
    pika.BasicProperties(content_type='application/json'))

'''
def publish_profile(body):
    try:

        LOGGER.info("inside publish")
        dataset_id = body['dataset_id']
        version_id = body['version_id']

        
        credentials = pika.PlainCredentials('user', 'PASSWORD')
        parameters = pika.ConnectionParameters('ad54e5e5c5cef471a854ac242511c8f2-1244485869.us-east-1.elb.amazonaws.com',heartbeat=60, credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        routing_key="paceml"

        channel.exchange_declare(exchange="pacemlexchange",
            exchange_type=ExchangeType.direct,
            passive=False,
            durable=True,
            auto_delete=False)

        channel.queue_declare(queue = "long_running_task",
                            durable=True,
                            passive=False,
                            auto_delete=False)

        channel.queue_bind(queue="long_running_task",
                    exchange="pacemlexchange",
                    routing_key="paceml")

        channel.basic_publish(
            'pacemlexchange', routing_key, json.dumps(body),
            pika.BasicProperties(content_type='application/json'))
        print("Published")

        return {"status":1, "msg":{"status":"job pushed", "dataset_id": dataset_id, "version_id":version_id}}
    except Exception as e:
        print(e)
        LOGGER.exception(e)
