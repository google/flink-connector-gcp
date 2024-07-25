#!/bin/bash

# Deletes GMF cluster
# Command example:
#   bash scripts/util-scripts/delete_gmf_deployment.sh -d <DEPLOYMENT> -p <PROJECT>  -r <REGION> -Y

# Required parameters
deployment=
project_id=

# Optional parameters
region="us-central1"

# Flag parsing with getopts
while getopts ":d:p:r:Y" opt; do 
  case $opt in
    # Strings
    d) deployment=$OPTARG ;; 
    p) project_id=$OPTARG ;;
    r) region=$OPTARG ;;

    # Booleans
    Y) auto_confirm_deletion=true ;;

    \?)
       echo "Invalid option: -$OPTARG" >&2
       exit 1
       ;;
  esac
done

if [[ ${#deployment} -eq 0 ]]; then
    echo "Error: Missing required parameter -d for deployment." >&2
    exit 1  
fi

if [[ ${#project_id} -eq 0 ]]; then
    echo "Error: Missing required parameter -p for project." >&2
    exit 1  
fi

AUTO_CONFIRM=""
if [[ $auto_confirm_deletion == true ]]; then
  AUTO_CONFIRM="echo Y |"
fi

# Delete deployment
FULL_DEPLOYMENT="projects/$project_id/locations/$region/deployments/$deployment"
eval "$AUTO_CONFIRM gcloud alpha managed-flink deployments delete $FULL_DEPLOYMENT"
