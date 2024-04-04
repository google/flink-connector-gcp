#!/bin/bash

# Installs Certs and Flink K8s Operator

function helm_install_handling_errors() {
    local command="$1"
    shift

    output=$($command "$@" 2>&1)
    exit_code=$?

    if [ $exit_code -ne 0 ]; then
        if [[ $output == *"cannot re-use a name that is still in use"* ]]; then
            echo "Already installed. Continuing..."
        else
            echo "Unexpected error : $output"
            exit 1
        fi
    fi
}

# Install certs
echo "Installing CERTs"
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.3/cert-manager.crds.yaml

cert_manager="helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --version v1.13.3 --set global.leaderElection.namespace=cert-manager"

helm_install_handling_errors $cert_manager

echo "Installing Flink k8s operator"
flink_kuberneted_operator="helm install flink-kubernetes-operator \
    flink-operator-repo/flink-kubernetes-operator"

helm_install_handling_errors $flink_kuberneted_operator
