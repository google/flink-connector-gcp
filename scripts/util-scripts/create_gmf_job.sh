#!/bin/bash

# Creates a GMF job on a deployment or on demand
# Command example:
#   bash scripts/util-scripts/create_gmf_job.sh -d <DEPLOYMENT> -p <PROJECT> \
#       -t <SERVICE_ACCOUNT>  -a <ARGS> -r <REGION> \
#       -m <MIN_PARALLELISM> -M <MAX_PARALLELISM> -e <ENTRY_CLASS>  \
#       -k <MANAGED_KAFKA_CLUSTERS>


set -e

# Required
jar_uri=

# Optional parameters
region="us-central1"
project_id=
args=
deployment=
service_account=
entry_class="flink.connector.gcp.GCStoGCSWordCount"
min_parallelism=1
max_parallelism=10

# Flag parsing with getopts
while getopts ":a:d:e:f:j:m:p:r:k:t:M:" opt; do 
  case $opt in
    # Strings
    a) args=$OPTARG ;;
    d) deployment=$OPTARG ;;
    e) entry_class=$OPTARG ;; 
    j) jar_uri=$OPTARG ;;
    p) project_id=$OPTARG ;;
    r) region=$OPTARG ;;
    t) service_account=$OPTARG ;;
    k) managed_kakfa_clusters=$OPTARG ;;

    # Integers
    m) min_parallelism=$OPTARG ;;
    M) max_parallelism=$OPTARG ;;

    \?)
       echo "Invalid option: -$OPTARG" >&2
       exit 1
       ;;
  esac
done


if [[ ${#jar_uri} -eq 0 ]]; then
    echo "Error: Missing required parameter -j for jar_uri." >&2
    exit 1  
fi

if (( $min_parallelism > $max_parallelism )); then
    echo "Error: Max parallelism cannot be lower than min parallelism." >&2
    exit 1  
fi 

PROJECT=""
if [[ ${#project_id} -ge 1 ]]; then
  PROJECT="--project=$project_id"
fi

DEPLOYMENT=""
if [[ ${#deployment} -ge 1 ]]; then
    echo "Launching job in deployment $deployment"
    DEPLOYMENT="--deployment=$deployment"
else
    echo "Launching On-Demand job"
fi

SERVICE_ACCOUNT=
if [[ ${#service_account} -ge 1 ]]; then
    SERVICE_ACCOUNT="--impersonate-service-account=$service_account"
fi

ARGS=
if [[ ${#args} -ge 1 ]]; then
  ARGS="--args=$args"
fi

MANAGED_KAFKA_CLUSTER=
if [[ ${#managed_kakfa_clusters} -ge 1 ]]; then
  MANAGED_KAFKA_CLUSTER="--managed-kafka-clusters=$managed_kakfa_clusters"
fi

gcloud alpha managed-flink jobs create \
    --location=$region \
    --job-jar=$jar_uri \
    --entry-class=$entry_class \
    --throughput-based-min-parallelism=$min_parallelism \
    --throughput-based-max-parallelism=$max_parallelism \
    $ARGS \
    $PROJECT \
    $DEPLOYMENT \
    $SERVICE_ACCOUNT \
    $MANAGED_KAFKA_CLUSTER
