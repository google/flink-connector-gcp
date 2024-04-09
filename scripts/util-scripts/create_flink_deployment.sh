#!/bin/bash

# Creates a Flink deployment
# Command example:
#   bash scripts/util-scripts/create_flink_deployment.sh -i <IMAGE> -j <JAR> -d <DEPLOYMENT_NAME> \
#       -f <FLINK_VERSION> -e <ENTRY_CLASS> -a "--arg1 value1 --arg2=value2 --arg3 value3" -q  <PARALLELISM>

# Required parameters
image=

# Optional
jar_uri="local:///opt/flink/usrlib/gmf-examples.jar"
deployment_name="wordcount"
flink_version="v1_18"
parallelism=1
args=
entry_class=

while getopts "a:d:e:f:i:j:q:" opt; do 
  case $opt in
    # Strings
    a) args=$OPTARG ;;
    d) deployment_name=$OPTARG ;;
    e) entry_class=$OPTARG ;; 
    f) flink_version=$OPTARG ;;
    i) image=$OPTARG ;;
    j) jar_uri=$OPTARG ;;

    # Integers
    q) parallelism=$OPTARG ;;

    \?)
       echo "Invalid option: -$OPTARG" >&2
       exit 1
       ;;
  esac
done

if [[ ${#image} -eq 0 ]]; then
    echo "Error: Missing required parameter -i for image name." >&2
    exit 1  
fi

export FLINK_DEPLOYMENT_NAME="$deployment_name"
export FLINK_IMAGE="$image"
export FLINK_VERSION="$flink_version"
export JAR_URI="$jar_uri"
export PARALLELISM="$parallelism"

SCRIPT_PATH=$(dirname "$0")

export APP_ARGS=""
if [[ ${#args} -ge 1 ]]; then
    PARSED_ARGS=$(bash "$SCRIPT_PATH"/split_args.sh $args)
    export APP_ARGS="args: [$PARSED_ARGS]"
fi

if [[ ${#entry_class} -ge 1 ]]; then
    export ENTRY_CLASS="entryClass: $entry_class" 
fi 

envsubst < "$SCRIPT_PATH"/job_template.yaml > "$SCRIPT_PATH"/deployment.yaml

desired_status="1/1"
max_retries=100 

for (( i=1; i<=$max_retries; i++ ))
do
    STATUS=$(kubectl get deployment | grep flink-kubernetes-operator | awk '{print $2}')
    if [[ "$STATUS" == "$desired_status" ]]; then
        echo "flink-kubernetes-operator is READY. Proceeding..."
        break
    fi

    if [[ $i == $max_retries ]]; then
        echo "flink-kubernetes-operator failed to be ready after $max_retries retries. Exiting."
        exit 1 
    fi

    echo "flink-kubernetes-operator is not READY yet $STATUS. Retry $i of $max_retries..."
    sleep 10
done

kubectl apply -f "$SCRIPT_PATH"/deployment.yaml