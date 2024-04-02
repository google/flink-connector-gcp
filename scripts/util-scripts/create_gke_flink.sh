#!/bin/bash
 
cluster_name=
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

# Create cluster
gcloud container clusters create-auto "$cluster_name" --region="$region" --project="$project_id"

# Install certs
echo "Installing CERTs"
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.3/cert-manager.crds.yaml

helm install \
  cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --version v1.13.3 --set global.leaderElection.namespace=cert-manager

# Install K8s Operator
echo "Installing flink k8s operator"
helm install flink-kubernetes-operator \
    flink-operator-repo/flink-kubernetes-operator
