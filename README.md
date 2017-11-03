# Kafka offset probe with Prometheus scraper

This application exposes the (topic, partition, offset) triple via HTTP endpoint `/internal/metrics`.

## Exposed metrics


| Metric        | Type  | Description     |
| -----------   | ----- | --------------- |
| offset        | Gauge | latest available Offset |

Following labels are automatically added

### Labels

| Label       | Description        |
| ----------- | ---------------    |
| topic       | Name of the topic  |
| service     | Always `broker`    |
| partition   | Partition number   |

Other labels (e.g. environment, cluster_name, etc) should be added at higher level

## Configuration

Application reads configuration from following environment variables

| Name       | Description                         |
| ---------- | ----------------------------------- |
| PORT       | HTTP port to listen to              |
| ZOOKEEPERS | Comma separated list of Zookeepers  |
| KAFKAPATH  | Path to find kafka brokers in the Zookeeper      |
