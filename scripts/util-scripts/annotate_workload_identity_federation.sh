#!/bin/bash

# Annotates Workload Identity Federation for a Service Account to the GMF cluster
# Command example:
#   bash scripts/util-scripts/annotate_workload_identity_federation.sh -p <PROJECT>  -s <SERVICE_ACCOUNT>

set -e

# Optional parameters
service_account=
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

if [[ ${#project_id} -ge 1 ]]; then
  PROJECT=$project_id
else
  echo "Using default project to build image"
  PROJECT=$(gcloud config get-value project)
fi

if [[ ${#service_account} -ge 1 ]]; then
  FULL_SERVICE_ACCOUNT=$service_account
else
  PROJECT_NUMBER=$(gcloud projects describe $PROJECT --format="value(projectNumber)")
  FULL_SERVICE_ACCOUNT=$PROJECT_NUMBER-compute@developer.gserviceaccount.com
  echo "Service account parameter (-s) missing, using Compute Engine default service account $FULL_SERVICE_ACCOUNT"
fi

gcloud iam service-accounts add-iam-policy-binding "$FULL_SERVICE_ACCOUNT" \
      --role roles/iam.workloadIdentityUser \
      --member "serviceAccount:$PROJECT.svc.id.goog[default/flink]"

kubectl annotate serviceaccount flink \
    --namespace default \
    iam.gke.io/gcp-service-account="$FULL_SERVICE_ACCOUNT"