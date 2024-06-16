#!/bin/bash
bin/kafka-topics.sh --create --topic aml_alerts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
