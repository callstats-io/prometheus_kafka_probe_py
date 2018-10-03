# Kafka offset probe with Prometheus scraper

This application exposes the (topic, partition, offset) triple via HTTP endpoint `/internal/metrics`.

## Exposed metrics


| Metric        | Type  | Description     |
| -----------   | ----- | --------------- |
| kafka_offset  | Gauge | latest available Offset |
| cg_kafka_offset| Gauge | consumer group offsets |

Following labels are added

### Labels

| Label       | Description        |
| ----------- | ---------------    |
| service     | Always `broker`    |
| topic       | Name of the topic  |
| partition   | Partition number   |
| consumergroup | name of the consumer group |

Other labels (e.g. environment, cluster_name, etc) should be added at higher level

## Configuration

Application reads configuration from following environment variables

| Name       | Description                         |
| ---------- | ----------------------------------- |
| PORT       | HTTP port to listen to              |
| KAFKAHOSTS | Comma separated list of kafka broker hosts  |
| TOPICS     | Comma separated list of topics to scrape |
| CONSUMERGROUPS | a json representation the topic and consumer group names. e.g. '{"topic_1": ["cg1"], "topic_2", ["cg1", "cg2"]}' |