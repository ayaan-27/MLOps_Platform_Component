import functools
import time
import pika
import os,sys
import json

sys.path.append(os.getcwd())
from preprocessing.preprocess_controller import start_preprocess_job
from augmentation.augment_controller import start_autoencode_job
from augmentation.augment_controller import start_sampling_job
from feature_eng.feature_eng_controller import start_feature_eng_job
from auto_ml.auto_ml_controller import start_auto_ml_job
from profiling.profile_controller import start_profiling_job

import utils.logs as logs

LOGGER = logs.get_logger()

'''
amqpuser = os.environ['amqpuser']
amqppassword = os.environ['amqppassword']
amqphost = os.environ['amqphost']
queue = os.environ['queue']
routing_key = os.environ['routingkey']
exchange = os.environ['exchange']
'''

amqpuser = 'user'
amqppassword = 'PASSWORD'
amqphost = 'ad54e5e5c5cef471a854ac242511c8f2-1244485869.us-east-1.elb.amazonaws.com'
queue = 'long_running_task'
routing_key = 'paceml'
exchange = 'pacemlexchange'


#logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def on_message(chan, method_frame, _header_frame, body, userdata=None):
    """Called when a message is received. Log message and ack it."""
    #time.sleep(1)
    json_body = json.loads(body.decode('utf-8'))
    job_type = json_body['type'] #parse the body as json and read the type of job
    
    if 'preprocess' in job_type:
        print("Inside Pre-process")
        res = start_preprocess_job(body=json_body)
        print(res)
        chan.basic_ack(delivery_tag=method_frame.delivery_tag)
    elif 'autoencode' in job_type:
        print("Inside autoencode")
        res = start_autoencode_job(body=json_body)
        chan.basic_ack(delivery_tag=method_frame.delivery_tag)
    elif 'sampling' in job_type:
        res = start_sampling_job(body=json_body)
        print("Inside sampling")
        chan.basic_ack(delivery_tag=method_frame.delivery_tag)
    elif 'feature_eng' in job_type:
        print("Inside feature_eng")
        res = start_feature_eng_job(body=json_body)
        chan.basic_ack(delivery_tag=method_frame.delivery_tag)
    elif 'auto_ml' in job_type:
        print("Inside auto_ml")
        res = start_auto_ml_job(body=json_body)
        chan.basic_ack(delivery_tag=method_frame.delivery_tag)
    elif 'job_profile' in job_type:
        LOGGER.info("inside job_profile")
        res =   start_profiling_job(body=json_body)
        chan.basic_ack(delivery_tag=method_frame.delivery_tag)    
    # chan.basic_ack(delivery_tag=method_frame.delivery_tag)


def main():
    """Main method."""
    credentials = pika.PlainCredentials(amqpuser,amqppassword )
    parameters = pika.ConnectionParameters(amqphost,heartbeat=60, credentials=credentials)
    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()
    res = channel.queue_declare(queue=queue, durable=True)
    channel.queue_bind(
        queue=queue, exchange=exchange, routing_key=routing_key)
    channel.basic_qos(prefetch_count=10)
    print('Messages in queue %d' % res.method.message_count)

    on_message_callback = functools.partial(
        on_message, userdata='on_message_userdata')
    channel.basic_consume(queue, on_message_callback)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()
    print('Messages in queue %d' % res.method.message_count)


if __name__ == '__main__':
    main()

