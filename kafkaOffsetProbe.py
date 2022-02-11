'''
Created on 6 Nov 2017

@author: Shaohong
'''

import os
from flask import Flask
from prometheus_client import Gauge, CollectorRegistry, generate_latest
import logging
import json
import asyncio
import pykafka

LOGGER = logging.getLogger(__name__)

# the flask app
app = Flask(__name__)

# create the Gauge object to track the offset change
registry = CollectorRegistry()
producerOffsetTracker = Gauge('kafka_offset', 'the latest available offsets', ['service', 'topic', 'partition'], registry=registry)
consumerGroupOffsetTracker = Gauge('cg_kafka_offset', 'the consumer group offsets', ['service', 'topic', 'partition', 'consumergroup'], registry=registry)
cgOffsetsInfo_list = []
offset_info = {}

def readEnvConfig(env_var_name, default=""):
    env_var_val = os.environ.get(env_var_name, default)
    return env_var_val.strip()

kafka_hosts = readEnvConfig("KAFKAHOSTS")
client = pykafka.KafkaClient(hosts=kafka_hosts)

@app.route('/ping')
def ping():
    return 'pong'


@app.route('/internal/metrics')
def getKafkaOffset():
    # fetch the latest available offsets from kafka and return the metrics
    topic_offset_info = getLatestOffset()
    #LOGGER.info(topic_offset_info)

    if topic_offset_info:
        for offsetInfo in topic_offset_info:
            if offsetInfo['offset'] > 0:
                producerOffsetTracker.labels('broker', offsetInfo['topic'], offsetInfo['partition']).set(offsetInfo['offset'])

    # check consumer group offsets
    consumerGroupListJsonStr = readEnvConfig("CONSUMERGROUPS")
    if consumerGroupListJsonStr:
        consumerGroupsToCheck = json.loads(consumerGroupListJsonStr)

        # go through the list of (consumergroup: topic)
        cg = createCGAsync(consumerGroupsToCheck)

    return generate_latest(registry)

def createCGAsync(consumerGroupsToCheck):
    tasks_cg = []
    for topicName, cgNames in consumerGroupsToCheck.items():
        for cgName in cgNames:
            tasks_cg.append(getConsumerGroupOffsets(topicName, cgName))
            # cgOffsetsInfo = getConsumerGroupOffsets(topicName, cgName)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.gather(*tasks_cg))
    loop.close()

    return "ok"

async def getConsumerGroupOffsets(topicName, consumerGroupName):
    '''
    returns the consumer group offset info in the form of a dictionary: {partition_id: offset)
    '''
    topic = client.topics[topicName]
    consumer = topic.get_simple_consumer(consumer_group=consumerGroupName,
                                         auto_start=False,
                                         reset_offset_on_fetch=False)
    current_offsets = consumer.fetch_offsets()
    # filter out consumer group offsets of '-1'
    consumerGroupOffsetInfo = {}

    for p_id, res in current_offsets:
        if res.offset >= 0:
            consumerGroupOffsetInfo[p_id] = res.offset

    for partition_id, offset in consumerGroupOffsetInfo.items():
        consumerGroupOffsetTracker.labels('broker', topicName, partition_id, consumerGroupName).set(offset)

    cgOffsetsInfo_list.append(consumerGroupOffsetInfo)

def getOffsetInfo(topicName=None):
    '''
    get offset info for the given topic. If no topic is given, returns offset info for all topics
    '''

    # default is to get offset info for all topics
    topic_names = client.topics.keys()
    #    loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)
    # loop = asyncio.get_event_loop()

    if topicName:
        if topicName not in topic_names:
            LOGGER.error("Topic '{}' does not exist".format(topicName))
            raise ValueError("topic {} doesn't exist!".format(topicName))
        else:
            topic_names = [topicName]

    tasks = []

    for topic_name in topic_names:
        tasks.append(getOffset(topic_name))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()

    return offset_info

async def getOffset(topic_name):
    try:
        earliest_offsets = client.topics[topic_name].earliest_available_offsets()
        latest_offsets = client.topics[topic_name].latest_available_offsets()
        offset_info[str(topic_name, 'utf-8')] = {'earliest': earliest_offsets, 'latest': latest_offsets}
    except Exception:
        pass

def getLatestOffset(topicName=None):
    offset_info = getOffsetInfo(topicName)
    if not offset_info:
        return None

    # return the (topic, partition, offset) tuple
    latestOffsets = []
    for topicName in offset_info.keys():

        latestOffsetInfo = offset_info[topicName]['latest']
        for partition in latestOffsetInfo:
            offsetPartitionResponse = latestOffsetInfo[partition]
            # LOGGER.info(offsetPartitionResponse)
            latestOffsets.append(
                {'topic': topicName, 'partition': partition, 'offset': offsetPartitionResponse.offset[0]})

    return latestOffsets


def main():
    # start the http server
    listeningPort = readEnvConfig("PORT", default="8080")
    LOGGER.info("listening on port {}".format(listeningPort))

    app.run(host='0.0.0.0', port=int(listeningPort))

if __name__ == '__main__':
    main()
