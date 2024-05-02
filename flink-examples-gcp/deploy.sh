#!/bin/bash

set -e

mvn clean package -U -DskipTests

docker build ./ -t us-central1-docker.pkg.dev/managed-flink-shared-dev/flink-connector-repo/flink-examples-gcp:$1
docker push us-central1-docker.pkg.dev/managed-flink-shared-dev/flink-connector-repo/flink-examples-gcp:$1
echo "Image pushed with tag number: " $1