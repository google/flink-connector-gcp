#!/bin/bash

# Builds a JAR and creates a GMF job on a (newly created) deployment or on demand
# Command example:
#   bash scripts/run_flink_job.sh -j <JAR_URI> -d <DEPLOYMENT> -p <PROJECT> \
#       -v <VPC> -n <SUBNET> -t <SERVICE_ACCOUNT> -s <MAX_SLOTS> -a <ARGS> \
#       -m <MIN_PARALLELISM> -M <MAX_PARALLELISM> -e <ENTRY_CLASS> -r <REGION> \
#       -k <MANAGED_KAFKA_CLUSTERS> -C -J

set -e

# Required
jar_uri= 

# Optional parameters
deployment=
region="us-central1"
project_id=
vpc="default"
subnet="default"
max_slots=10
service_account=
entry_class="flink.connector.gcp.GCStoGCSWordCount"
min_parallelism=1
max_parallelism=10
create_jar=
args=

# Flag parsing with getopts
while getopts ":a:d:p:e:r:v:s:m:n:t:j:k:M:JD" opt; do 
  case $opt in
    # String
    a) args=$OPTARG ;;
    d) deployment=$OPTARG ;; 
    p) project_id=$OPTARG ;;
    r) region=$OPTARG ;;
    v) vpc=$OPTARG ;;
    n) subnet=$OPTARG ;;
    t) service_account=$OPTARG ;;
    j) jar_uri=$OPTARG ;;
    e) entry_class=$OPTARG ;; 
    k) managed_kakfa_clusters=$OPTARG ;;
    
    # Integer
    s) max_slots=$OPTARG ;;
    m) min_parallelism=$OPTARG ;;
    M) max_parallelism=$OPTARG ;;

    # Booleans
    J) create_jar=true ;;
    D) create_deployment=true ;;

    \?)
       echo "Invalid option: -$OPTARG" >&2
       exit 1
       ;;
  esac
done

SCRIPT_PATH=$(pwd)/$(dirname "$0")

if [[ ${#jar_uri} -eq 0 ]]; then
      echo "Error: Missing required parameter -j for GCS path to the JAR." >&2
      exit 1  
fi

if (( $min_parallelism > $max_parallelism )); then
    echo "Error: Max parallelism cannot be lower than min parallelism." >&2
    exit 1  
fi 

PROJECT=""
if [[ ${#project_id} -ge 1 ]]; then
   PROJECT="-p $project_id"
else
  echo "Using default project to start job"
fi

SERVICE_ACCOUNT=
if [[ ${#service_account} -ge 1 ]]; then
    SERVICE_ACCOUNT="-t $service_account"
fi

if [[ $create_jar == true ]]; then
  bash "$SCRIPT_PATH"/util-scripts/create_jar.sh -j "$jar_uri"
fi

ARGS=
if [[ ${#args} -ge 1 ]]; then
  ARGS="-a $args"
fi

MANAGED_KAFKA_CLUSTER=
if [[ ${#managed_kakfa_clusters} -ge 1 ]]; then
  MANAGED_KAFKA_CLUSTER="-k $managed_kakfa_clusters"
fi

DEPLOYMENT=""
if [[ ${#deployment} -ge 1 ]]; then
  if [[ $create_deployment == true ]]; then
    bash "$SCRIPT_PATH"/util-scripts/create_gmf_deployment.sh -d $deployment $PROJECT $SERVICE_ACCOUNT -v $vpc -n $subnet -s $max_slots -r $region
  fi
  DEPLOYMENT="-d $deployment"
fi

bash "$SCRIPT_PATH"/util-scripts/create_gmf_job.sh -j $jar_uri $PROJECT $SERVICE_ACCOUNT $DEPLOYMENT -m $min_parallelism -M $max_parallelism -e $entry_class -r $region $ARGS $MANAGED_KAFKA_CLUSTER 
