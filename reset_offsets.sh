#!/bin/bash
CONSUMER_GROUP_ID="aml_alerts_group"
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group $CONSUMER_GROUP_ID --reset-offsets --all-topics --to-earliest --execute
