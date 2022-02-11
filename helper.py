from __future__ import print_function

import logging
import pykafka
import asyncio
from prometheus_client import Gauge, CollectorRegistry, generate_latest


LOGGER = logging.getLogger(__name__)
offset_info = {}
registry = CollectorRegistry()
consumerGroupOffsetTracker = Gauge('cg_kafka_offset', 'the consumer group offsets',
                                   ['service', 'topic', 'partition', 'consumergroup'], registry=registry)

class KafkaHelper:
    def __init__(self, kafka_hosts):
        self.client = pykafka.KafkaClient(hosts=kafka_hosts)

    def getOffsetInfo(self, topicName=None):
        '''
        get offset info for the given topic. If no topic is given, returns offset info for all topics
        '''

        # default is to get offset info for all topics
        # print(client.topics.keys())
        topic_names = self.client.topics.keys()
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
            tasks.append(self.getOffset(topic_name))

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()

        return offset_info

    async def getOffset(self,topic_name):
        try:
            earliest_offsets = self.client.topics[topic_name].earliest_available_offsets()
            latest_offsets = self.client.topics[topic_name].latest_available_offsets()
            offset_info[str(topic_name, 'utf-8')] = {'earliest': earliest_offsets, 'latest': latest_offsets}
        except Exception:
            pass

    def getLatestOffset(self,topicName=None):
        offset_info = self.getOffsetInfo(topicName)
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
