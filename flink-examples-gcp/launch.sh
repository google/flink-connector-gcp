#!/bin/bash

set -e

mvn clean install -U -DskipTests -Dcheckstyle.skip

export IMAGE_WITH_TAG=us-central1-docker.pkg.dev/managed-flink-shared-dev/flink-connector-repo/bq-table-api:$1
echo $IMAGE_WITH_TAG
docker build ./ -t $IMAGE_WITH_TAG

docker push $IMAGE_WITH_TAG

# kubectl delete -f bq-table-api.yaml
# envsubst < bq-table-api.yaml | kubectl apply -f -