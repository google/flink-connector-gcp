#!/bin/bash

# Creates a GMF deployment 
# Command example:
#   bash scripts/util-scripts/create_gmf_deployment.sh -d <DEPLOYMENT> -p <PROJECT>  -r <REGION> \
#       -v <VPC> -n <SUBNET> -t <SERVICE_ACCOUNT> -s <MAX_SLOTS>

set -e

# Required parameters
deployment=

# Optional parameters
region="us-central1"
project_id=
vpc="default"
subnet="default"
max_slots=10
service_account=

# Flag parsing with getopts
while getopts ":d:p:r:v:s:t:n:" opt; do 
  case $opt in
    # String
    d) deployment=$OPTARG ;; 
    p) project_id=$OPTARG ;;
    r) region=$OPTARG ;;
    v) vpc=$OPTARG ;;
    n) subnet=$OPTARG ;;
    t) service_account=$OPTARG ;;
    s) max_slots=$OPTARG ;;

    \?)
       echo "Invalid option: -$OPTARG" >&2
       exit 1
       ;;
  esac
done

if [[ ${#deployment} -eq 0 ]]; then
    echo "Error: Missing required parameter -d for deployment name." >&2
    exit 1  
fi

PROJECT=""
if [[ ${#project_id} -ge 1 ]]; then
   PROJECT="--project=$project_id"
else
  echo "Using default project to create cluster"
fi

SERVICE_ACCOUNT=
if [[ ${#service_account} -ge 1 ]]; then
    SERVICE_ACCOUNT="--impersonate-service-account=$service_account"
fi

# Create deployment
echo "Creating Flink deployment...this may take a few minutes"

gcloud alpha managed-flink deployments create \
  $deployment \
  --location=$region \
  --network-config-vpc=$vpc \
  --network-config-subnetwork=$subnet \
  --max-slots=$max_slots \
  $PROJECT \
  $SERVICE_ACCOUNT