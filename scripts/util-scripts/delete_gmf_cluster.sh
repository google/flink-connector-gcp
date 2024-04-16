#!/bin/bash

# Deletes GMF cluster
# Command example:
#   bash scripts/util-scripts/delete_gmf_cluster.sh -c <CLUSTER_NAME> -p <PROJECT>  -r <REGION> -Y -A

# Required parameters
cluster_name=

# Optional parameters
region="us-central1"
project_id=
auto_confirm_deletion=false
async=false

# Flag parsing with getopts
while getopts "c:p:r:AY" opt; do 
  case $opt in
    # Strings
    c) cluster_name=$OPTARG ;; 
    r) region=$OPTARG ;;
    p) project_id=$OPTARG ;;

    # Booleans
    A) async=true ;;
    Y) auto_confirm_deletion=true ;;

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
if [[ $auto_confirm_deletion == true ]]; then
  AUTO_CONFIRM="echo Y |"
fi

ASYNC=""
if [[ $async == true ]]; then
   ASYNC="--async"
fi

# Delete cluster
eval "$AUTO_CONFIRM gcloud container clusters delete $cluster_name --region=$region $PROJECT $ASYNC"
