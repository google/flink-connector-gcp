#!/bin/bash

# Compiles `flink-examples-gcp` and uploads it to GCS
# Command example:
#   bash scripts/util-scripts/create_jar.sh -j <JAR_URI>

set -e

jar_uri=

# Flag parsing with getopts
while getopts ":j:" opt; do 
  case $opt in
    # Strings
    j) jar_uri=$OPTARG ;;

    \?)
       echo "Invalid option: -$OPTARG" >&2
       exit 1
       ;;
  esac
done

SCRIPT_PATH=$(dirname "$0")

cd $SCRIPT_PATH

echo "Building JAR"
mvn -f ../../flink-examples-gcp/ clean package

echo "Uploading JAR to $jar_uri"
gcloud storage cp ../../flink-examples-gcp/target/flink-examples-gcp-0.0.0-shaded.jar "$jar_uri"
