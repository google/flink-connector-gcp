#!/bin/bash

# Creates GMF cluster, builds Dockerimage and starts a Flink deployment
# Command example:
#   bash scripts/run_flink.sh -c <CLUSTER_NAME> -p <PROJECT>  -r <REGION> -m <IMAGE_NAME> -t <TAG>\
#       -n <REPO_NAME> -f <FLINK_VERSION> -i <IMAGE> -j <JAR> -d <DEPLOYMENT_NAME> -q <PARALLELISM> \
#       -f <FLINK_VERSION> -e <ENTRY_CLASS> -a "--arg1 value1 --arg2=value2 --arg3 value3"  \
#       -B -C -D -F -M -P -W

set -e

# Required parameters
cluster_name=

# Optional parameters
region="us-central1"
flink_version="1.18.1"
jar_uri="local:///opt/flink/usrlib/gmf-examples.jar"
deployment_name="wordcount"
repo_name="flink-connector-repo"
image_name="flink-image"
image_tag="latest"
entry_class="flink.connector.gcp.GCStoGCSWordCount"
parallelism=1
project_id=
service_account=
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
while getopts ":a:c:d:e:f:i:j:m:n:p:q:r:s:t:BCDFMPW" opt; do 
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

    # Integers
    q) parallelism=$OPTARG ;;

    # Booleans
    B) use_cloud_build=true ;;
    C) create_cluster=false ; echo "Skipping Cluster creation" ;;
    D) create_docker=false ; echo "Skipping creating Docker Image" ;;
    F) create_flink_deployment=false ; echo "Skipping Flink deployment" ;;
    M) build_maven=false ;;
    P) install_plugins=false ;;
    W) workload_identity_federation=false ; echo "Skipping Workload Identity Federation Annotation" ;;

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

if [[ ${#project_id} -ge 1 ]]; then
  PROJECT=$project_id
else
  echo "Using default project to build image"
  PROJECT=$(gcloud config get-value project)
fi

SCRIPT_PATH=$(pwd)/$(dirname "$0")

if [[ $create_cluster == true ]]; then
  bash "$SCRIPT_PATH"/util-scripts/create_gmf_cluster.sh -c "$cluster_name" \
  -p "$PROJECT" -r "$region"
else
  echo "Getting context for cluster $cluster_name"
  gcloud container clusters get-credentials "$cluster_name" --region "$region" --project "$PROJECT"
fi

if [[ $workload_identity_federation == true ]]; then
  if [[ ${#service_account} -ge 1 ]]; then
    FULL_SERVICE_ACCOUNT=$service_account
  else
    PROJECT_NUMBER=$(gcloud projects describe $PROJECT --format="value(projectNumber)")
    FULL_SERVICE_ACCOUNT=$PROJECT_NUMBER-compute@developer.gserviceaccount.com
    echo "Service account parameter (-s) missing, using Compute Engine default service account $FULL_SERVICE_ACCOUNT"
  fi
  bash "$SCRIPT_PATH"/util-scripts/annotate_workload_identity_federation.sh -p "$PROJECT" -s "$FULL_SERVICE_ACCOUNT"
fi

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
    echo "Using custom image $full_image"
    IMAGE_FULL_NAME=$full_image
  else
    IMAGE_FULL_NAME=$region-docker.pkg.dev/$PROJECT/$repo_name/$image_name:$image_tag
  fi


  PARSED_FLINK_VERSION=${flink_version/./_}
  PARSED_FLINK_VERSION=v${PARSED_FLINK_VERSION%.*}
  bash "$SCRIPT_PATH"/util-scripts/create_flink_deployment.sh -i "$IMAGE_FULL_NAME" \
   -j "$jar_uri" -f "$PARSED_FLINK_VERSION" \
   -d "$deployment_name" -a "$args" \
   -e "$entry_class" -q $parallelism
fi