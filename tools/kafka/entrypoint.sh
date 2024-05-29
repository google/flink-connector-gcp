#!/bin/sh
export KAFKA_PASSWORD=$(echo -n $GMK_PASSWORD | base64 -w 0)
export KAFKA_USERNAME=$(echo "$GMK_PASSWORD" | jq -r ".client_email")
envsubst < "/opt/kafka-config-template.properties" > "/opt/kafka-config.properties"
exec "$@"