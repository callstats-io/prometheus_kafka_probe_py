'''
Created on 6 Nov 2017

@author: Shaohong
'''

import os
from flask import Flask
from prometheus_client import Gauge, CollectorRegistry, generate_latest
import app_config
import helper
import logging
LOGGER = logging.getLogger(__name__)

# the flask app
app = Flask(__name__)

# create the Gauge object to track the offset change
registry = CollectorRegistry()
offsetTracker = Gauge('kafka_offset', 'the latest available offsets', ['service', 'topic', 'partition'], registry=registry)


def readEnvConfig(env_var_name, default=""):
    env_var_val = os.environ.get(env_var_name, default)
    assert env_var_val.strip(), "{} env variable not set!".format(env_var_name)
    return env_var_val.strip()

@app.route('/ping')
def ping():
    return 'pong'


@app.route('/internal/metrics')
def getKafkaOffset():
    # fetch the latest available offsets from kafka and return the metrics
    zookeeper_host = readEnvConfig("ZOOKEEPERS")
    kafka_helper = helper.KafkaHelper(zookeeper_host)
    topic_offset_info = kafka_helper.getLatestOffset()
    #LOGGER.info(topic_offset_info)

    for offsetInfo in topic_offset_info:
        offsetTracker.labels('broker', offsetInfo['topic'], offsetInfo['partition']).set(offsetInfo['offset'])
    return generate_latest(registry)


def main():
    # start the http server
    listeningPort = readEnvConfig("PORT", "8080")
    app.run(host='0.0.0.0', port=listeningPort)

if __name__ == '__main__':
    main()
