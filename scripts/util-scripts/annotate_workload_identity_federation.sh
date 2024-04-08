#!/bin/bash

# Annotates Workload Identity Federation for a Service Account to the GMF cluster
# Command example:
#   bash scripts/util-scripts/annotate_workload_identity_federation.sh -p <PROJECT>  -s <SERVICE_ACCOUNT>

set -e

# Required parameters
service_account=

# Optional parameters
project_id=

while getopts "p:s:" opt; do 
  case $opt in
    p) project_id=$OPTARG ;;
    s) service_account=$OPTARG ;;

    \?)
       echo "Invalid option: -$OPTARG" >&2
       exit 1
       ;;
  esac
done

if [[ ${#service_account} -eq 0 ]]; then
    echo "Error: Missing required parameter -s for service account." >&2
    exit 1  
fi

if [[ ${#project_id} -ge 1 ]]; then
  PROJECT=$project_id
else
  echo "Using default project to build image"
  PROJECT=$(gcloud config get-value project)
fi

FULL_SERVICE_ACCOUNT=$service_account@$PROJECT.iam.gserviceaccount.com

gcloud iam service-accounts add-iam-policy-binding "$FULL_SERVICE_ACCOUNT" \
      --role roles/iam.workloadIdentityUser \
      --member "serviceAccount:$PROJECT.svc.id.goog[default/flink]"

kubectl annotate serviceaccount flink \
    --namespace default \
    iam.gke.io/gcp-service-account="$FULL_SERVICE_ACCOUNT"