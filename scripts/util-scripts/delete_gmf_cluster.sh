#!/bin/bash

# Deletes GMF cluster
# Command example:
#   bash scripts/util-scripts/delete_gmf_flink.sh -c <CLUSTER_NAME> -p <PROJECT>  -r <REGION> -y -a

# Required parameters
cluster_name=

# Optional parameters
region="us-central1"
project_id=
auto_confirm_deletion="false"
async="false"

# Flag parsing with getopts
while getopts "c:p:r:ya" opt; do 
  case $opt in
    c) cluster_name=$OPTARG ;; 
    r) region=$OPTARG ;;
    p) project_id=$OPTARG ;;
    y) auto_confirm_deletion="true" ;;
    a) async="true" ;;

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
  echo "Using default project to delete cluster"
fi

AUTO_CONFIRM=""
if [[ $auto_confirm_deletion == "true" ]]; then
  AUTO_CONFIRM="echo Y |"
fi

ASYNC=""
if [[ $async == "true" ]]; then
   ASYNC="--async"
fi

# Delete cluster
eval "$AUTO_CONFIRM gcloud container clusters delete $cluster_name --region=$region $PROJECT $ASYNC"
