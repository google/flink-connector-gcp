#!/bin/bash

# Creates GMF cluster and installs Certs and Flink-kubernetes-operator.
# Command example:
#   bash scripts/util-scripts/create_gmf_flink.sh -c <CLUSTER_NAME> -p <PROJECT>  -r <REGION> -y

# Required parameters
cluster_name=

# Optional parameters
region="us-central1"
project_id=

# Flag parsing with getopts
while getopts ":c:r:p:" opt; do 
  case $opt in
    c) cluster_name=$OPTARG ;; 
    r) region=$OPTARG ;;
    p) project_id=$OPTARG ;;

    \?)
       echo "Invalid option: -$OPTARG" >&2
       exit 1
       ;;
  esac
done

if [[ ${#cluster_name} -eq 0 ]]; then
    echo "Error: Missing required parameter -c for cluster name." >&2
    exit 1  
fi

PROJECT=""
if [[ ${#project_id} -ge 1 ]]; then
   PROJECT="--project=$project_id"
else
  echo "Using default project to create cluster"
fi


# Create cluster
create_cluster=$(gcloud container clusters create-auto $cluster_name --region=$region $PROJECT 2>&1)
exit_code=$?
if [ $exit_code -ne 0 ] && [[ $create_cluster == *"Already exists"* ]]; then
    echo "Cluster $cluster_name already exists, skipping creation"
else
    echo "Unexpected error occurred: $create_cluster"
    exit 1
fi

# Install Certs and Flink K8s Operator
bash $(dirname "$0")/install_certs_and_operator.sh