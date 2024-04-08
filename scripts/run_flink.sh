#!/bin/bash

# Creates GMF cluster, builds Dockerimage and starts a Flink deployment
# Command example:
#   bash scripts/run_flink.sh -c <CLUSTER_NAME> -p <PROJECT>  -r <REGION> -m <IMAGE_NAME> -t <TAG>\
#       -n <REPO_NAME> -f <FLINK_VERSION> -i <IMAGE> -j <JAR> -d <DEPLOYMENT_NAME> \
#       -f <FLINK_VERSION> -e <ENTRY_CLASS> -a "--arg1 value1 --arg2=value2 --arg3 value3"  \
#       -B -C -D -F -M -P -W



set -e

# Required parameters
cluster_name=
service_account=

# Optional parameters
region="us-central1"
flink_version="1.18.1"
jar_uri="local:///opt/flink/usrlib/gmf-examples.jar"
deployment_name="wordcount"
repo_name="gmf-repo"
image_name="flink-image"
image_tag="latest"
entry_class=WordCount
project_id=
full_image= 
args=
build_maven=true
create_cluster=true
create_docker=true
create_flink_deployment=true
workload_identity_federation=true
install_plugins=true
use_cloud_build=false

# Flag parsing with getopts
while getopts ":a:c:d:e:f:i:j:m:n:p:r:s:t:BCDFMPW" opt; do 
  case $opt in
    # Strings
    a) args=$OPTARG ;;
    c) cluster_name=$OPTARG ;; 
    d) deployment_name=$OPTARG ;;
    e) entry_class=$OPTARG ;; 
    f) flink_version=$OPTARG ;;
    i) full_image=$OPTARG ;;
    j) jar_uri=$OPTARG ;;
    m) image_name=$OPTARG ;;
    n) repo_name=$OPTARG ;;
    p) project_id=$OPTARG ;;
    r) region=$OPTARG ;;
    s) service_account=$OPTARG ;;
    t) image_tag=$OPTARG ;;

    # Booleans
    B) use_cloud_build=true ;;
    C) create_cluster=false ; echo "Skipping Cluster creation" ;;
    D) create_docker=false ; echo "Skipping creating Docker Image" ;;
    F) create_flink_deployment=false; echo "Skipping Flink deployment" ;;
    M) build_maven=false ;;
    P) install_plugins=false ;;
    W) workload_identity_federation=false; echo "Skipping Workload Identity Federation Annotation" ;;

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

if [[ ${#service_account} -eq 0 ]] && [[ $workload_identity_federation == true ]]; then
    echo "Error: Missing required parameter -s for Service Account if annotating with Workload Identity Federation." >&2
    exit 1  
fi

if [[ ${#project_id} -ge 1 ]]; then
  PROJECT=$project_id
else
  echo "Using default project to build image"
  PROJECT=$(gcloud config get-value project)
fi

SCRIPT_PATH=$(pwd)/$(dirname "$0")

CREATE_CLUSTER=""
if [[ $create_cluster == true ]]; then
  bash "$SCRIPT_PATH"/util-scripts/create_gmf_cluster.sh -c "$cluster_name" \
  -p "$PROJECT" -r "$region"
fi

WORKLOAD_IDENTITY=""
if [[ $workload_identity_federation == true ]]; then
  bash "$SCRIPT_PATH"/util-scripts/annotate_workload_identity_federation.sh \
  -p "$PROJECT" -s "$service_account"
fi

CREATE_DOCKER=""
if [[ $create_docker == true ]] && [[ !${#full_image} -ge 1 ]]; then
  USE_CLOUD_BUILD=""
  if [[ $use_cloud_build == true ]]; then
    USE_CLOUD_BUILD="-B"
  fi

  INSTALL_PLUGINS=""
  if [[ $install_plugins == false ]]; then
    INSTALL_PLUGINS="-P"
  fi

  BUILD_MAVEN=""
  if [[ $build_maven == false ]]; then
    BUILD_MAVEN="-M"
  fi

  bash "$SCRIPT_PATH"/util-scripts/create_docker_image.sh \
  -r "$region"  -p "$PROJECT"  -f "$flink_version" \
  -n "$repo_name" -m "$image_name" -t "$image_tag" \
   $USE_CLOUD_BUILD $BUILD_MAVEN $INSTALL_PLUGINS
fi


if [[ $create_flink_deployment == true ]]; then
  if [[ ${#full_image} -ge 1 ]]; then
    IMAGE_FULL_NAME=$full_image
  else
    IMAGE_FULL_NAME=$region-docker.pkg.dev/$PROJECT/$repo_name/$image_name:$image_tag
  fi


  PARSED_FLINK_VERSION=${flink_version/./_}
  PARSED_FLINK_VERSION=v${PARSED_FLINK_VERSION%.*}
  bash "$SCRIPT_PATH"/util-scripts/create_flink_deployment.sh -i "$IMAGE_FULL_NAME" \
   -j "$jar_uri" -f "$PARSED_FLINK_VERSION" \
   -d "$deployment_name" -a "$args" \
   -e "$entry_class"
fi