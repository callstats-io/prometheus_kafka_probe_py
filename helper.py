from __future__ import print_function

import logging
import pykafka
LOGGER = logging.getLogger(__name__)

class KafkaHelper:
    def __init__(self, zookeeper_host, broker_version='0.9.0'):
        self.client = pykafka.KafkaClient(zookeeper_hosts=zookeeper_host, broker_version=broker_version)

    def getOffsetInfo(self, topicName=None):
        '''
        get offset info for the given topic. If no topic is given, returns offset info for all topics
        '''

        # default is to get offset info for all topics
        topic_names = self.client.topics.keys()
        if topicName:
            if topicName not in topic_names:
                LOGGER.error("Topic '{}' does not exist".format(topicName))
                raise ValueError("topic {} doesn't exist!".format(topicName))
            else:
                topic_names = [topicName]

        # return results in the form of a dictionary where keys are topic_name and values are the partition_num, offset info
        offset_info = {}

        for topic_name in topic_names:
            earliest_offsets = self.client.topics[topic_name].earliest_available_offsets()
            latest_offsets = self.client.topics[topic_name].latest_available_offsets()
            offset_info[topic_name] = {'earliest': earliest_offsets, 'latest': latest_offsets}

        return offset_info

    def getLatestOffset(self, topicName=None):
        offset_info = self.getOffsetInfo(topicName)
        if not offset_info:
            return None

        # return the (topic, partition, offset) tuple
        latestOffsets = []
        for topicName in offset_info.keys():
            latestOffsetInfo = offset_info[topicName]['latest']
            for partition in  latestOffsetInfo:
                offsetPartitionResponse = latestOffsetInfo[partition]
                # LOGGER.info(offsetPartitionResponse)
                latestOffsets.append({'topic': topicName, 'partition': partition, 'offset': offsetPartitionResponse.offset[0]})

        return latestOffsets

