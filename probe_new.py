import asyncio
import time
from flask import Flask
#import helper
from prometheus_client import Gauge, CollectorRegistry, generate_latest
import json
import pykafka
import logging

LOGGER = logging.getLogger(__name__)

# the flask app
app = Flask(__name__)

# create the Gauge object to track the offset change
registry = CollectorRegistry()
producerOffsetTracker = Gauge('kafka_offset', 'the latest available offsets', ['service', 'topic', 'partition'], registry=registry)
consumerGroupOffsetTracker = Gauge('cg_kafka_offset', 'the consumer group offsets', ['service', 'topic', 'partition', 'consumergroup'], registry=registry)

offset_info = {}
kafka_hosts = "localhost:9092"
client = pykafka.KafkaClient(hosts=kafka_hosts)
topic_names = client.topics.keys()
cgOffsetsInfo_list = []

async def say_after(delay, what):
    await asyncio.sleep(delay)
    print("finished "+what)
    print(f"current {time.strftime('%X')}")
    print(what)

@app.route('/internal/test')
def call_run_main():
    asyncio.get_event_loop().run_until_complete(run_main())
    return "OK"

async def run_main():
    tasks = []
    tasks.append(asyncio.create_task(
        say_after(1, 'hello')))

    tasks.append(asyncio.create_task(
        say_after(2, 'world')))

    tasks.append(asyncio.create_task(
        say_after(5, 'slow')))

    print(f"started at {time.strftime('%X')}")

    # Wait until both tasks are completed (should take
    # around 2 seconds.)

    #for task in tasks:
    #    await task
    # await task1
    # await task2
    # await task3

    print(f"finished at {time.strftime('%X')}")


@app.route('/internal/metrics')
def getKafkaOffset():
    # fetch the latest available offsets from kafka and return the metrics
    kafka_hosts = "localhost:9092"
    #kafka_helper = helper.KafkaHelper(kafka_hosts)
    topic_offset_info = getLatestOffset()
    #LOGGER.info(topic_offset_info)
    if topic_offset_info:
        for offsetInfo in topic_offset_info:
            if offsetInfo['offset'] > 0:                
		producerOffsetTracker.labels('broker', offsetInfo['topic'], offsetInfo['partition']).set(offsetInfo['offset'])

    # # check consumer group offsets
        consumerGroupListJsonStr="{\"ConferenceBackbone\": [\"dataprocessor_rra_001\"],\"NetworkBackbone\": [\"dataprocessor_rra_001\",\"network-data-monitoring\"],\"AppAnalyzer\": [\"appanalyzer_001\",\"confclassifier_001\",\"confaggregation_001\"],\"ConferenceImportRequest\":[\"eventWriter-group\"],\"payment_conference_data_usage\":[\"payment-service-data-point-consumer\"], \"conference-event-obfuscated\": [\"conference-event-obfuscated-to-storage-monitoring-oci\"]}"
    if consumerGroupListJsonStr:
        consumerGroupsToCheck = json.loads(consumerGroupListJsonStr)
        # go through the list of (consumergroup: topic)
        cg = createCGAsync(consumerGroupsToCheck)
        #for topicName, cgNames in consumerGroupsToCheck.items():
        #    for cgName in cgNames:
        #        cgOffsetsInfo = getConsumerGroupOffsets(topicName, cgName)
        #        for partition_id, offset in cgOffsetsInfo.items():
        #            consumerGroupOffsetTracker.labels('broker', topicName, partition_id, cgName).set(offset)


    return generate_latest(registry)

def createCGAsync(consumerGroupsToCheck):
    tasks_cg = []
    for topicName, cgNames in consumerGroupsToCheck.items():
            for cgName in cgNames:
                tasks_cg.append(getConsumerGroupOffsets(topicName, cgName))
                #cgOffsetsInfo = getConsumerGroupOffsets(topicName, cgName)
    print("here1")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.gather(*tasks_cg))
    loop.close()

    for cgOffsetsInfo in cgOffsetsInfo_list:
        for partition_id, offset in cgOffsetsInfo.items():
            consumerGroupOffsetTracker.labels('broker', topicName, partition_id, cgName).set(offset)

    return "ok"

async def createAsyncTasks(topic_names):
    tasks = []

    for topic_name in topic_names:
        tasks.append(getOffset(topic_name))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()

    #loop.run_until_complete(createAsyncTasks(topic_names))
    #loop.close()
    # return results in the form of a dictionary where keys are topic_name and values are the partition_num, offset info
    #offset_info = {}

    # for topic_name in topic_names:
    #     earliest_offsets = self.client.topics[topic_name].earliest_available_offsets()
    #     latest_offsets = self.client.topics[topic_name].latest_available_offsets()
    #     offset_info[topic_name] = {'earliest': earliest_offsets, 'latest': latest_offsets}
    # for task in tasks:
    #     await task
    #loop.close()
    return offset_info

async def getOffset(topic_name):
    try:
        earliest_offsets = client.topics[topic_name].earliest_available_offsets()
        latest_offsets = client.topics[topic_name].latest_available_offsets()
        offset_info[topic_name] = {'earliest': earliest_offsets, 'latest': latest_offsets}
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
        for partition in  latestOffsetInfo:
            offsetPartitionResponse = latestOffsetInfo[partition]
            # LOGGER.info(offsetPartitionResponse)
            latestOffsets.append({'topic': topicName, 'partition': partition, 'offset': offsetPartitionResponse.offset[0]})

    return latestOffsets


async def getConsumerGroupOffsets(topicName, consumerGroupName):
    '''
    returns the consumer group offset info in the form of a dictionary: {partition_id: offset)
    '''
    topic = client.topics[topicName]
    print(topic)
    consumer = topic.get_simple_consumer(consumer_group=consumerGroupName,
                                         auto_start=False,
                                         reset_offset_on_fetch=False)
    current_offsets = consumer.fetch_offsets()
    #filter out consumer group offsets of '-1'
    consumerGroupOffsetInfo={}

    for p_id, res in current_offsets:
        if res.offset >=0:
            consumerGroupOffsetInfo[p_id] = res.offset

    cgOffsetsInfo_list.append(consumerGroupOffsetInfo)

def main():
    # start the http server
    listeningPort = 8080
    print("listening on port {}".format(listeningPort))

    app.run(host='0.0.0.0', port=int(listeningPort))

if __name__ == '__main__':
    main()

                                                                                                                                                                                                                                            225,0-1       Bot


