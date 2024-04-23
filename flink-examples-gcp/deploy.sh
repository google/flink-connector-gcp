#!/bin/bash

set -e

mvn clean package -DskipTests

docker build ./ -t us-central1-docker.pkg.dev/managed-flink-shared-dev/flink-connector-repo/flink-examples-gcp:without-creds
docker push us-central1-docker.pkg.dev/managed-flink-shared-dev/flink-connector-repo/flink-examples-gcp:without-creds
echo "Image pushed with tag number: "