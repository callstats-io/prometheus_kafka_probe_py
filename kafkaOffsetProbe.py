'''
Created on 6 Nov 2017

@author: Shaohong
'''

import os
from flask import Flask
from prometheus_client import Gauge, CollectorRegistry, generate_latest
import helper
import logging
import app_config
import json

LOGGER = logging.getLogger(__name__)

# the flask app
app = Flask(__name__)

# create the Gauge object to track the offset change
registry = CollectorRegistry()
producerOffsetTracker = Gauge('kafka_offset', 'the latest available offsets', ['service', 'topic', 'partition'], registry=registry)
consumerGroupOffsetTracker = Gauge('cg_kafka_offset', 'the consumer group offsets', ['service', 'topic', 'partition', 'consumergroup'], registry=registry)


def readEnvConfig(env_var_name, default=""):
    env_var_val = os.environ.get(env_var_name, default)
    return env_var_val.strip()

@app.route('/ping')
def ping():
    return 'pong'


@app.route('/internal/metrics')
def getKafkaOffset():
    # fetch the latest available offsets from kafka and return the metrics
    kafka_hosts = readEnvConfig("KAFKAHOSTS")
    kafka_helper = helper.KafkaHelper(kafka_hosts)
    topic_offset_info = kafka_helper.getLatestOffset()
    #LOGGER.info(topic_offset_info)

    if topic_offset_info:
        for offsetInfo in topic_offset_info:
            if offsetInfo['offset'] > 0:
                producerOffsetTracker.labels('broker', offsetInfo['topic'], offsetInfo['partition']).set(offsetInfo['offset'])

    # check consumer group offsets
    consumerGroupListJsonStr=readEnvConfig("CONSUMERGROUPS")
    if consumerGroupListJsonStr:
        consumerGroupsToCheck = json.loads(consumerGroupListJsonStr)
        # go through the list of (consumergroup: topic)
        for topicName, cgNames in consumerGroupsToCheck.items():
            for cgName in cgNames:
                cgOffsetsInfo = kafka_helper.getConsumerGroupOffsets(topicName, cgName)
                for partition_id, offset in cgOffsetsInfo.items():
                    consumerGroupOffsetTracker.labels('broker', topicName, partition_id, cgName).set(offset)

    return generate_latest(registry)


def main():
    # start the http server
    listeningPort = readEnvConfig("PORT", default="8080")
    LOGGER.info("listening on port {}".format(listeningPort))

    app.run(host='0.0.0.0', port=int(listeningPort))

if __name__ == '__main__':
    main()
